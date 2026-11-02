// Package markwalk turns BeeGFS Watch events into a GUFI suspect file: one "<inode> d" line per
// directory whose own index database must be rebuilt.
//
// It works in two steps:
//   - Add folds each event into the changed-directory list, see ChangeList. The collector keeps
//     the list between two updates.
//   - Dump runs at the hand-over, when an update starts. It looks the whole list up in the index
//     in one pass and writes the suspect file. The update runs after it, so no lookup reads the
//     index while gufi_incremental_update rewrites it.
//
// The list is not on disk until Dump, so the events behind it stay unacknowledged until then.
// After a crash Watch resends them, and the collector lists them again.
//
// gufi_incremental_update finds new, moved and deleted directories by itself, so a line is
// needed only for a directory that is in the index at its current path and whose own rows
// changed:
//   - the parent of a changed entry, because the parent's database holds the entry's row;
//   - the entry itself on an attribute or stripe change, in case it is a directory, because a
//     directory's attributes live in its own database;
//   - every directory that lists another name of a changed hard-linked file, because each name's
//     row carries the file's size, mode and link count.
//
// Nothing here reads the BeeGFS mount: every inode comes from the index through gufi_query, and
// only from GUFI's own tables, so an index built without the BeeGFS plugin works the same.
//
// No event data ever goes into gufi_query's SQL. gufi_query replaces "{key}" anywhere in its SQL
// text, inside string literals too, and has no escape for it. So directories go in through path
// lists, and inodes through a database that gufi_query attaches.
package markwalk

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/renameio/v2"
	"github.com/thinkparq/protobuf/go/beeindex"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// MaxWorkSizeLimit is the largest Config.MaxWorkSize, in MiB, whose size in bytes fits an int64.
const MaxWorkSizeLimit = math.MaxInt64 >> 20

// ownInode selects a directory's own inode. A rolled-up index adds summary rows for
// subdirectories, with isroot clear.
const ownInode = "inode FROM summary WHERE isroot = 1"

type Config struct {
	Work       string `mapstructure:"work"`
	Index      string `mapstructure:"index"`
	Query      string `mapstructure:"query"`
	Sqlite     string `mapstructure:"sqlite"`
	TrackOpens bool   `mapstructure:"track-opens"`
	// MaxWorkSize is in MiB of allocated blocks, not bytes written, because it protects the disk.
	MaxWorkSize int64 `mapstructure:"max-work-size"`
}

// ErrWorkFull means the work directory holds max-work-size, so a hand-over writes no file. The
// list stays with the collector and is not acknowledged. The updater deletes suspect files only
// after a successful run, so the run applies the files already there, which frees the space.
var ErrWorkFull = errors.New("work directory full")

// errIndex marks a failure to read the index tree itself, as opposed to a query over it.
var errIndex = errors.New("index not readable")

// errQuery marks a failed gufi_query run.
var errQuery = errors.New("index query failed")

// ChangeList is the changed-directory list: what the collector keeps of the events since the last
// hand-over. It holds the directories they changed, the names that may have other names
// elsewhere, and the sequence IDs delivered per metadata server. Add fills it, so the collector
// never holds whole events. A path is held once, however many events name it.
type ChangeList struct {
	Events    int
	Dirs      map[string]struct{}
	Links     map[string]struct{}
	Delivered map[uint32][]SeqRange
}

// SeqRange is a run of consecutive sequence IDs delivered by one metadata server, First and Last
// included. The subscriber SDK reports progress to Watch only once every delivered event is
// acknowledged on its own, so the collector must acknowledge each sequence ID, not just the
// highest. Watch numbers each server's events one by one, so a list usually holds one range per
// server. A new range starts after a gap, where Watch lost events, and where Watch redelivers
// events after a reconnect.
type SeqRange struct{ First, Last uint64 }

// NewChangeList returns an empty list.
func NewChangeList() *ChangeList {
	return &ChangeList{Dirs: map[string]struct{}{}, Links: map[string]struct{}{}, Delivered: map[uint32][]SeqRange{}}
}

// Paths is the number of distinct paths l holds. The collector bounds the list by it, because
// the list's memory follows its paths, not its events.
func (l *ChangeList) Paths() int {
	return len(l.Dirs) + len(l.Links)
}

// Append adds the paths and sequence IDs of later to l. A failed hand-over puts its list back in
// front of the events that arrived since, so the ranges of later go after those of l, and each
// metadata server's IDs stay in delivery order.
func (l *ChangeList) Append(later *ChangeList) {
	l.Events += later.Events
	maps.Copy(l.Dirs, later.Dirs)
	maps.Copy(l.Links, later.Links)
	for meta, ranges := range later.Delivered {
		for _, r := range ranges {
			l.Delivered[meta] = extend(l.Delivered[meta], r)
		}
	}
}

type Marker struct {
	cfg     Config
	threads int
	log     *zap.Logger

	// What Dump is doing, for Status. Dump writes these fields, Status reads them.
	mu      sync.Mutex
	state   beeindex.MarkwalkStatus_State
	events  int
	since   time.Time // when the lookup started, or, while failing, when the first failure was
	step    string
	failure string
	// failingSince is when hand-overs started failing. A successful Dump clears it.
	failingSince time.Time
	handovers    uint64
	suspectDirs  uint64
}

// New returns a Marker whose gufi_query runs use threads threads. GUFI's own default is one.
func New(cfg Config, threads int, log *zap.Logger) *Marker {
	return &Marker{cfg: cfg, threads: threads, log: log}
}

// Check refuses a wrong setting at startup instead of failing every hand-over.
func (m *Marker) Check(ctx context.Context) error {
	for _, bin := range []string{m.cfg.Query, m.cfg.Sqlite} {
		if _, err := exec.LookPath(bin); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(m.cfg.Work, 0o755); err != nil {
		return err
	}
	out, err := m.query(ctx, "--max-level", "0", "-S", "SELECT "+ownInode)
	if err != nil {
		return err
	}
	inos, err := inodes(out)
	if err != nil {
		return err
	}
	if len(inos) != 1 {
		return fmt.Errorf("%s is not an index root: no summary row", m.cfg.Index)
	}
	return nil
}

// Add records what ev changed in l: the directories, and the name when the entry may have
// another name elsewhere. It records ev's sequence ID too, because Dump's caller acknowledges
// every delivered event.
func (m *Marker) Add(l *ChangeList, ev *bw.Event) {
	paths, link := pathsFor(ev.GetV2(), m.cfg.TrackOpens)
	for _, p := range paths {
		l.Dirs[p] = struct{}{}
	}
	if link != "" {
		l.Links[link] = struct{}{}
	}
	l.Delivered[ev.GetMetaId()] = extend(l.Delivered[ev.GetMetaId()], SeqRange{ev.GetSeqId(), ev.GetSeqId()})
	l.Events++
}

// extend appends r to ranges, merging it into the last range when it follows on directly.
func extend(ranges []SeqRange, r SeqRange) []SeqRange {
	if n := len(ranges); n > 0 && ranges[n-1].Last+1 == r.First {
		ranges[n-1].Last = r.Last
		return ranges
	}
	return append(ranges, r)
}

// Dumped is what one Dump wrote.
type Dumped struct {
	// Delivered is every sequence ID per metadata server the file covers, in delivery order.
	Delivered map[uint32][]SeqRange
	// Seqs is the highest of them per metadata server.
	Seqs   map[uint32]uint64
	Inodes int // lines written; zero means no file
	Events int
}

// Dump runs at the hand-over, when an update starts. It looks list l up in the index and writes
// the inodes to file. Status reports each step. The rules:
//   - The work directory's size is checked first. At max-work-size Dump writes nothing and
//     returns ErrWorkFull, because the updater deletes suspect files only after a successful run.
//   - A list that names no directory of the index writes no file. Its sequence IDs are still
//     returned, because those events changed nothing in the index.
//   - The file is synced before Dump returns, so the caller may then acknowledge every sequence
//     ID it returns.
//   - On an error Dump writes nothing and returns nothing. The caller keeps the list for the
//     next hand-over, so its events stay unacknowledged.
func (m *Marker) Dump(ctx context.Context, l *ChangeList, file string) (d Dumped, err error) {
	m.begin(l.Events)
	defer func() { m.end(d.Inodes, err) }()
	m.setStep("checking the work directory's size")
	used, err := allocated(m.cfg.Work)
	if err != nil {
		return Dumped{}, err
	}
	if limit := m.cfg.MaxWorkSize << 20; used >= limit {
		return Dumped{}, fmt.Errorf("%w: %s holds %d of %d bytes", ErrWorkFull, m.cfg.Work, used, limit)
	}
	m.setStep("finding the changed directories in the index")
	listed, dropped, err := m.indexDirs(l.Dirs)
	if err != nil {
		return Dumped{}, err
	}
	m.log.Debug("changed directories", zap.Strings("in_index", listed), zap.Int("not_in_index", dropped))
	fields := []zap.Field{zap.Int("events", l.Events), zap.Int("directories", len(listed)),
		zap.Int("not_index_directories", dropped)}
	d = Dumped{Delivered: l.Delivered, Seqs: map[uint32]uint64{}, Events: l.Events}
	for meta, ranges := range l.Delivered {
		for _, r := range ranges {
			d.Seqs[meta] = max(d.Seqs[meta], r.Last)
		}
	}
	if len(listed) == 0 {
		m.log.Info("lookup done: no directory of the index changed", fields...)
		return d, nil
	}
	// The path lists and the hard-link database go to a scratch directory. One hand-over runs at
	// a time, so one directory serves them all.
	scratch := filepath.Join(m.cfg.Work, "lookup")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		return Dumped{}, err
	}
	defer os.RemoveAll(scratch)
	m.setStep("looking up inodes in the index")
	found, files, err := m.indexInodes(ctx, scratch, listed, linkedDirs(listed, l.Links))
	if err != nil {
		return Dumped{}, err
	}
	m.setStep("looking up the other names of hard links")
	others, err := m.otherNames(ctx, scratch, files)
	if err != nil {
		return Dumped{}, err
	}
	// A directory can be found both ways, so the set holds each inode once.
	set := map[uint64]struct{}{}
	for _, ino := range slices.Concat(found, others) {
		set[ino] = struct{}{}
	}
	m.setStep("writing the suspect file")
	if err := writeSuspects(set, file); err != nil {
		return Dumped{}, fmt.Errorf("write %s: %w", file, err)
	}
	d.Inodes = len(set)
	m.log.Info("lookup done", append(fields, zap.Int("inodes", d.Inodes), zap.Int("multi_name_files", len(files)),
		zap.Int("other_name_dirs", len(others)))...)
	return d, nil
}

// begin records that a lookup started.
func (m *Marker) begin(events int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state, m.events, m.since = beeindex.MarkwalkStatus_PROCESSING, events, time.Now()
	m.step, m.failure = "", ""
}

func (m *Marker) setStep(step string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.step = step
}

// end records how Dump ended. A failure stays in the status until a later Dump succeeds, and the
// status keeps the time of the first failure across the hand-overs that fail after it.
func (m *Marker) end(marks int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err != nil {
		if m.failingSince.IsZero() {
			m.failingSince = time.Now()
		}
		m.state, m.since, m.step, m.failure = beeindex.MarkwalkStatus_FAILING, m.failingSince, "", reason(err)
		return
	}
	m.state, m.events, m.since = beeindex.MarkwalkStatus_WAITING, 0, time.Time{}
	m.step, m.failure, m.failingSince = "", "", time.Time{}
	m.handovers++
	m.suspectDirs += uint64(marks)
}

// Status reports what Dump is doing, for the IndexUpdater API. BatchesDone counts the hand-overs
// whose lookup succeeded, and SuspectDirs the lines their suspect files hold.
func (m *Marker) Status() *beeindex.MarkwalkStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := &beeindex.MarkwalkStatus{State: m.state, BatchesDone: m.handovers, SuspectDirs: m.suspectDirs}
	if m.state != beeindex.MarkwalkStatus_WAITING {
		s.Events, s.Since = uint64(m.events), timestamppb.New(m.since)
		s.Step, s.Error = m.step, m.failure
	}
	return s
}

// reason is what Status reports for a failed lookup, in fixed words. The full error goes to the
// log only, because it can quote file names and tool output, and any client that reaches the
// service may ask. The first rule that matches wins:
//   - the work directory holds its limit, so an update must apply and delete suspect files first;
//   - the work directory's file system is out of space;
//   - the index tree cannot be read, reported with the system error;
//   - an index query failed.
func reason(err error) string {
	var errno syscall.Errno
	switch {
	case errors.Is(err, ErrWorkFull):
		return "the work directory is full until an update applies and deletes suspect files"
	case errors.Is(err, syscall.ENOSPC):
		return "no space is left on the work directory's file system"
	case errors.Is(err, errIndex) && errors.As(err, &errno):
		return "the index cannot be read: " + errno.Error()
	case errors.Is(err, errIndex):
		return "the index cannot be read; the service log has the error"
	case errors.Is(err, errQuery):
		return "an index query failed; the service log has its output"
	}
	return "the lookup failed; the service log has the error"
}

// allocated skips a path that vanishes during the walk, so a file deleted meanwhile does not fail
// the hand-over.
func allocated(dir string) (int64, error) {
	var total int64
	err := filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err == nil {
			var info fs.FileInfo
			if info, err = d.Info(); err == nil {
				total += info.Sys().(*syscall.Stat_t).Blocks * 512
				return nil
			}
		}
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	})
	return total, err
}

// pathsFor returns the index-relative directories ev dirties and, when the entry may have another
// name elsewhere, the path of the name the event saw. See linkedDirs.
func pathsFor(ev *bw.V2Event, trackOpens bool) (dirs []string, link string) {
	parent := dirOf(ev.GetPath())
	entry := rel(ev.GetPath())
	names := ev.GetNumLinks()
	switch ev.GetType() {
	case bw.V2Event_INVALID, bw.V2Event_OPEN_BLOCKED, bw.V2Event_INODE_LOCKED:
		return nil, ""

	case bw.V2Event_RENAME:
		// Fired once, on the source. A renamed directory is moved and rebuilt by the update.
		return []string{parent, dirOf(ev.GetTargetPath())}, ""

	case bw.V2Event_HARDLINK:
		// num_links counts the new name, and both directories are marked already, so the other
		// names exist only when more than two remain.
		dirs = []string{parent, dirOf(ev.GetTargetPath())}
		if names > 2 {
			return dirs, rel(ev.GetTargetPath())
		}
		return dirs, ""

	case bw.V2Event_UNLINK:
		// num_links counts the names left after the unlink.
		if names >= 1 {
			return []string{parent}, entry
		}
		return []string{parent}, ""

	case bw.V2Event_OPEN_READ, bw.V2Event_OPEN_WRITE, bw.V2Event_OPEN_READ_WRITE:
		if !trackOpens {
			return nil, ""
		}
		return []string{parent}, multiName(entry, names)

	case bw.V2Event_SETATTR, bw.V2Event_STRIPE_PATTERN_CHANGED:
		// The event does not say whether the entry is a directory, whose attributes live in its own
		// database. indexDirs keeps the path only if the index holds it as a directory.
		return []string{parent, entry}, multiName(entry, names)

	case bw.V2Event_FLUSH, bw.V2Event_TRUNCATE, bw.V2Event_CLOSE_WRITE, bw.V2Event_LAST_WRITER_CLOSED:
		return []string{parent}, multiName(entry, names)

	default:
		// MKDIR, RMDIR, CREATE, MKNOD, SYMLINK, and any type added later.
		return []string{parent}, ""
	}
}

func multiName(entry string, names uint64) string {
	if names > 1 {
		return entry
	}
	return ""
}

// linkedDirs returns the listed directories that hold a name from links. Their multi-name files
// are looked up by inode across the whole index, see otherNames. A link that is itself a listed
// directory is skipped, because a directory's link count counts its subdirectories, not names.
func linkedDirs(listed []string, links map[string]struct{}) []string {
	isListed := map[string]bool{}
	for _, d := range listed {
		isListed[d] = true
	}
	linked := map[string]struct{}{}
	for l := range links {
		if d := path.Dir(l); isListed[d] && !isListed[l] {
			linked[d] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(linked))
}

// rel maps an event path, absolute within the mount, to gufi_query's path-list form: relative to
// the index root, "." for the root.
func rel(p string) string {
	if r := strings.TrimPrefix(path.Clean("/"+p), "/"); r != "" {
		return r
	}
	return "."
}

func dirOf(p string) string {
	return path.Dir(rel(p))
}

// indexDirs keeps the paths that are directories in the index tree and drops the rest. gufi_query
// opens every path-list entry as a directory, and any failure but ENOENT prints an "Error:" line
// that fails every hand-over until the path changes. The cases dropped:
//   - a file, or a path through a file, such as a user file named db.db where the parent's
//     database sits;
//   - a missing path or one too long to be in the index;
//   - a path with "\n" or "\r", because gufi_query reads its path list line by line and strips
//     "\r", or with a NUL byte, which no path can hold. Such a directory is rebuilt only when its
//     place in the tree changes or a repair runs.
func (m *Marker) indexDirs(dirs map[string]struct{}) (listed []string, dropped int, err error) {
	for _, d := range slices.Sorted(maps.Keys(dirs)) {
		if strings.ContainsAny(d, "\n\r\x00") {
			m.log.Debug("not an index directory: control character in the path", zap.String("path", strconv.Quote(d)))
			dropped++
			continue
		}
		info, err := os.Lstat(filepath.Join(m.cfg.Index, d))
		switch {
		case err == nil && info.IsDir():
			listed = append(listed, d)
		case err == nil, errors.Is(err, fs.ErrNotExist), errors.Is(err, syscall.ENOTDIR), errors.Is(err, syscall.ENAMETOOLONG):
			m.log.Debug("not an index directory", zap.String("path", d), zap.NamedError("lstat", err))
			dropped++
		default:
			return nil, 0, fmt.Errorf("%w: checking %s: %w", errIndex, d, err)
		}
	}
	return listed, dropped, nil
}

// indexInodes returns the own inode of every listed directory, and the inode of every
// multi-name file in the linked directories. A file the event did not name is matched too when
// it shares a linked directory, which costs extra rebuilds but keeps file names out of the SQL.
func (m *Marker) indexInodes(ctx context.Context, scratch string, listed, linked []string) (dirInodes, fileInodes []uint64, err error) {
	dirInodes, err = m.queryList(ctx, filepath.Join(scratch, "dirs"), listed, "-S", "SELECT "+ownInode)
	if err != nil || len(linked) == 0 {
		return dirInodes, nil, err
	}
	fileInodes, err = m.queryList(ctx, filepath.Join(scratch, "linkdirs"), linked, "-E", "SELECT inode FROM entries WHERE nlink > 1")
	return dirInodes, fileInodes, err
}

// queryList writes dirs to the path-list file list and runs one query over those directories.
// A failed run fails the hand-over, so the list is kept rather than acknowledged incomplete.
func (m *Marker) queryList(ctx context.Context, list string, dirs []string, sql ...string) ([]uint64, error) {
	if err := os.WriteFile(list, []byte(strings.Join(dirs, "\n")+"\n"), 0o644); err != nil {
		return nil, err
	}
	out, err := m.query(ctx, slices.Concat([]string{"--min-level", "1", "--max-level", "1", "--path-list", list}, sql)...)
	if err != nil {
		return nil, fmt.Errorf("index lookup: %w", err)
	}
	return inodes(out)
}

// otherNames returns the own inode of every directory in the index that lists one of
// fileInodes, in one walk of the whole index. The inodes go into <scratch>/links.db, which every
// gufi_query thread attaches through -I. In the SQL text they would cost one walk per few
// thousand inodes, because Linux caps one argument at 128 KiB. The walk has no time limit, so a
// large index makes the hand-over wait for it. A failed lookup loses only the list's hard-link
// marks: the other names keep a stale row until their directory changes again.
func (m *Marker) otherNames(ctx context.Context, scratch string, fileInodes []uint64) ([]uint64, error) {
	if len(fileInodes) == 0 {
		return nil, nil
	}
	slices.Sort(fileInodes)
	fileInodes = slices.Compact(fileInodes)
	db := filepath.Join(scratch, "links.db")
	defer os.Remove(db)
	err := m.writeLinks(ctx, db, fileInodes)
	var out []byte
	if err == nil {
		out, err = m.query(ctx, "-I", "ATTACH "+sqlString(db)+" AS links",
			"-E", "SELECT (SELECT "+ownInode+") FROM entries WHERE nlink > 1 AND inode IN (SELECT inode FROM links.inodes)")
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		m.log.Warn("hard-link lookup failed; the other names of these files keep a stale row",
			zap.Int("files", len(fileInodes)), zap.Error(err))
		return nil, nil
	}
	return inodes(out)
}

// writeLinks writes one statement per line, because gufi_sqlite3 runs its standard input line by
// line. Inodes are text, as GUFI stores them in entries.
func (m *Marker) writeLinks(ctx context.Context, db string, inos []uint64) error {
	if err := os.Remove(db); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	var sql strings.Builder
	sql.WriteString("CREATE TABLE inodes(inode TEXT PRIMARY KEY);\nBEGIN;\n")
	for _, ino := range inos {
		fmt.Fprintf(&sql, "INSERT INTO inodes VALUES ('%d');\n", ino)
	}
	sql.WriteString("COMMIT;\n")
	cmd := exec.CommandContext(ctx, m.cfg.Sqlite, db)
	cmd.Stdin = strings.NewReader(sql.String())
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("gufi_sqlite3: %w: %s", err, bytes.TrimSpace(out))
	}
	return nil
}

func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// query runs gufi_query over the index. gufi_query exits zero after most failures and reports
// them on stderr only:
//   - "Error:" means rows are missing, so the query fails.
//   - "Cannot attach database" means one directory's database is unreadable, for example
//     corrupt. A retry would fail the same way and hold the list forever, so the line is logged
//     and that directory's rows are left out.
func (m *Marker) query(ctx context.Context, args ...string) ([]byte, error) {
	args = append(append([]string{"-n", strconv.Itoa(m.threads), "-d", " "}, args...), m.cfg.Index)
	cmd := exec.CommandContext(ctx, m.cfg.Query, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%w: gufi_query: %w: %s", errQuery, err, bytes.TrimSpace(stderr.Bytes()))
	}
	for line := range strings.Lines(stderr.String()) {
		switch {
		case strings.HasPrefix(line, "Error:"):
			return nil, fmt.Errorf("%w: gufi_query: %s", errQuery, strings.TrimSpace(line))
		case strings.HasPrefix(line, "Cannot attach database"):
			m.log.Warn("gufi_query could not read a directory's index database; its rows are missing from this hand-over",
				zap.String("detail", strings.TrimSpace(line)))
		}
	}
	return out, nil
}

func inodes(out []byte) ([]uint64, error) {
	fields := strings.Fields(string(out))
	inos := make([]uint64, 0, len(fields))
	for _, f := range fields {
		ino, err := strconv.ParseUint(f, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("gufi_query output: %q is not an inode", f)
		}
		inos = append(inos, ino)
	}
	return inos, nil
}

// writeSuspects writes the file gufi_incremental_update reads with --suspect-method 1. It renames
// the file into place and syncs the directory, because the events behind it are acknowledged
// once Dump returns.
func writeSuspects(dirs map[uint64]struct{}, out string) error {
	var b bytes.Buffer
	for _, ino := range slices.Sorted(maps.Keys(dirs)) {
		fmt.Fprintf(&b, "%d d\n", ino)
	}
	if err := renameio.WriteFile(out, b.Bytes(), 0o644); err != nil {
		return err
	}
	return syncDir(filepath.Dir(out))
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
