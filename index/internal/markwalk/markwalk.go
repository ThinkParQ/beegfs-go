// Package markwalk turns BeeGFS Watch events into a GUFI suspect file: one "<inode> d" line per
// directory whose own index database must be rebuilt.
//
// It works in two steps, at two paces:
//   - Process looks up one batch in the index as the batch arrives, and adds the inodes to the run
//     set: every inode found since the last Dump, each held once, in memory only.
//   - Dump writes the run set to the file the updater names when a run starts, and starts an empty
//     one. It reads no index and processes no batch.
//
// The run set is not on disk until Dump, so the events behind it stay unacknowledged until then.
// After a crash Watch resends them, and Process looks them up again.
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

// ErrWorkFull holds a batch back until the updater deletes applied suspect files. The batch is
// neither added to the run set nor acknowledged.
var ErrWorkFull = errors.New("work directory full")

// errIndex marks a failure to read the index tree itself, as opposed to a query over it.
var errIndex = errors.New("index not readable")

// errQuery marks a failed gufi_query run.
var errQuery = errors.New("index query failed")

// Batch is what the collector keeps of a batch's events: the directories they changed, the names
// that may have other names elsewhere, and the sequence IDs delivered per metadata server. Add
// fills it, so the collector never holds whole events.
type Batch struct {
	N         uint64 // for the log only
	Events    int
	Dirs      map[string]struct{}
	Links     map[string]struct{}
	Delivered map[uint32][]SeqRange
}

// SeqRange is a run of consecutive sequence IDs delivered by one metadata server, First and Last
// included. The subscriber SDK reports progress to Watch only once every delivered event is
// acknowledged on its own, so the collector must acknowledge each sequence ID, not just the
// highest. Watch numbers each server's events one by one, so a run set usually holds one range
// per server. A new range starts after a gap, where Watch lost events, and where Watch
// redelivers events after a reconnect.
type SeqRange struct{ First, Last uint64 }

// NewBatch returns an empty batch numbered n.
func NewBatch(n uint64) *Batch {
	return &Batch{N: n, Dirs: map[string]struct{}{}, Links: map[string]struct{}{}, Delivered: map[uint32][]SeqRange{}}
}

type Marker struct {
	cfg     Config
	threads int
	log     *zap.Logger

	mu sync.Mutex
	// The run set and the sequence IDs of the batches in it, in delivery order per metadata
	// server. Process adds to them, Dump takes them.
	set       map[uint64]struct{}
	delivered map[uint32][]SeqRange
	batches   int
	// What Process is doing, for Status. Process writes these fields, Status reads them.
	state   beeindex.MarkwalkStatus_State
	batch   uint64 // the batch in markwalk, while processing or failing
	events  int
	since   time.Time // when the batch entered markwalk, or, while failing, when it first failed
	step    string
	failure string
	// failingSince is when the batch in markwalk first failed. The caller retries a batch until
	// it succeeds, so a success clears it.
	failingSince time.Time
	batchesDone  uint64
	suspectDirs  uint64
}

// New returns a Marker whose gufi_query runs use threads threads. GUFI's own default is one.
func New(cfg Config, threads int, log *zap.Logger) *Marker {
	return &Marker{cfg: cfg, threads: threads, log: log, set: map[uint64]struct{}{}, delivered: map[uint32][]SeqRange{}}
}

// Check refuses a wrong setting at startup instead of failing every batch.
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

// Add records what ev changed in b: the directories, and the name when the entry may have
// another name elsewhere. It records ev's sequence ID too, because Dump's caller acknowledges
// every delivered event.
func (m *Marker) Add(b *Batch, ev *bw.Event) {
	paths, link := pathsFor(ev.GetV2(), m.cfg.TrackOpens)
	for _, p := range paths {
		b.Dirs[p] = struct{}{}
	}
	if link != "" {
		b.Links[link] = struct{}{}
	}
	b.Delivered[ev.GetMetaId()] = extend(b.Delivered[ev.GetMetaId()], SeqRange{ev.GetSeqId(), ev.GetSeqId()})
	b.Events++
}

// Process looks up batch b in the index and adds the inodes to the run set, together with the
// batch's sequence IDs. A batch that fails adds nothing, so its events stay unacknowledged until
// the caller's retry succeeds. The space limit is checked before the batch, because the updater
// deletes suspect files only after a successful run. Status reports each step.
func (m *Marker) Process(ctx context.Context, b *Batch) (err error) {
	m.begin(b.N, b.Events)
	marks := 0
	defer func() { m.end(marks, err) }()
	m.setStep("checking the work directory's size")
	used, err := allocated(m.cfg.Work)
	if err != nil {
		return err
	}
	if limit := m.cfg.MaxWorkSize << 20; used >= limit {
		return fmt.Errorf("%w: %s holds %d of %d bytes", ErrWorkFull, m.cfg.Work, used, limit)
	}
	m.setStep("finding the batch's directories in the index")
	listed, dropped, err := m.indexDirs(b.Dirs)
	if err != nil {
		return err
	}
	m.log.Debug("directories of the batch", zap.Uint64("batch", b.N), zap.Strings("in_index", listed), zap.Int("not_in_index", dropped))
	fields := []zap.Field{zap.Uint64("batch", b.N), zap.Int("events", b.Events),
		zap.Int("directories", len(listed)), zap.Int("not_index_directories", dropped)}
	if len(listed) == 0 {
		m.add(nil, b.Delivered)
		m.log.Info("batch done: no directory of the index changed", fields...)
		return nil
	}
	// The path lists and the hard-link database go to a scratch directory. One batch is processed
	// at a time, so one directory serves them all.
	scratch := filepath.Join(m.cfg.Work, "lookup")
	if err := os.MkdirAll(scratch, 0o755); err != nil {
		return err
	}
	defer os.RemoveAll(scratch)
	m.setStep("looking up inodes in the index")
	found, files, err := m.indexInodes(ctx, scratch, listed, linkedDirs(listed, b.Links))
	if err != nil {
		return err
	}
	m.setStep("looking up the other names of hard links")
	others, err := m.otherNames(ctx, scratch, files)
	if err != nil {
		return err
	}
	inos := slices.Concat(found, others)
	marks = len(inos)
	held := m.add(inos, b.Delivered)
	m.log.Info("batch done", append(fields, zap.Int("marks", marks), zap.Int("multi_name_files", len(files)),
		zap.Int("other_name_dirs", len(others)), zap.Int("run_set", held))...)
	return nil
}

// add puts one batch's inodes and sequence IDs into the run set and returns its size. Batches
// arrive in delivery order, so the ranges keep that order.
func (m *Marker) add(inodes []uint64, delivered map[uint32][]SeqRange) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches++
	for _, ino := range inodes {
		m.set[ino] = struct{}{}
	}
	for meta, ranges := range delivered {
		for _, r := range ranges {
			m.delivered[meta] = extend(m.delivered[meta], r)
		}
	}
	return len(m.set)
}

// extend appends r to ranges, merging it into the last range when it follows on directly.
func extend(ranges []SeqRange, r SeqRange) []SeqRange {
	if n := len(ranges); n > 0 && ranges[n-1].Last+1 == r.First {
		ranges[n-1].Last = r.Last
		return ranges
	}
	return append(ranges, r)
}

// Dumped is what one Dump took from the run set.
type Dumped struct {
	// Delivered is every sequence ID per metadata server the file covers, in delivery order.
	Delivered map[uint32][]SeqRange
	// Seqs is the highest of them per metadata server.
	Seqs    map[uint32]uint64
	Inodes  int // lines written; zero means no file
	Batches int
}

// Dump runs when the updater starts a run. It writes the run set to file and starts an empty one.
// The rules:
//   - It takes the run set as it is. A batch still in Process goes into the new run set, and so
//     does every batch after it.
//   - The file is synced before Dump returns, so the caller may then acknowledge every
//     delivered sequence ID it returns.
//   - An empty run set writes no file. Its sequence IDs are still returned, because those
//     events changed nothing in the index.
//   - A failed write puts the taken run set back, so the next Dump writes it.
func (m *Marker) Dump(file string) (Dumped, error) {
	m.mu.Lock()
	d := Dumped{Delivered: m.delivered, Seqs: map[uint32]uint64{}, Inodes: len(m.set), Batches: m.batches}
	set := m.set
	m.set, m.delivered, m.batches = map[uint64]struct{}{}, map[uint32][]SeqRange{}, 0
	m.mu.Unlock()
	for meta, ranges := range d.Delivered {
		for _, r := range ranges {
			d.Seqs[meta] = max(d.Seqs[meta], r.Last)
		}
	}
	if len(set) == 0 {
		return d, nil
	}
	if err := writeSuspects(set, file); err != nil {
		// The taken ranges are older than any added since, so they go in front.
		m.mu.Lock()
		maps.Copy(m.set, set)
		for meta, ranges := range d.Delivered {
			m.delivered[meta] = append(ranges, m.delivered[meta]...)
		}
		m.batches += d.Batches
		m.mu.Unlock()
		return Dumped{}, fmt.Errorf("write %s: %w", file, err)
	}
	return d, nil
}

// begin records that batch n entered markwalk.
func (m *Marker) begin(n uint64, events int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state, m.batch, m.events, m.since = beeindex.MarkwalkStatus_PROCESSING, n, events, time.Now()
	m.step, m.failure = "", ""
}

func (m *Marker) setStep(step string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.step = step
}

// end records how Process ended. A failed batch stays in markwalk, because the caller retries it
// until it succeeds, and the status keeps the time of its first failure across the retries.
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
	m.state, m.batch, m.events, m.since = beeindex.MarkwalkStatus_WAITING, 0, 0, time.Time{}
	m.step, m.failure, m.failingSince = "", "", time.Time{}
	m.batchesDone++
	m.suspectDirs += uint64(marks)
}

// Status reports what Process is doing, for the IndexUpdater API.
func (m *Marker) Status() *beeindex.MarkwalkStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := &beeindex.MarkwalkStatus{State: m.state, BatchesDone: m.batchesDone, SuspectDirs: m.suspectDirs}
	if m.state != beeindex.MarkwalkStatus_WAITING {
		s.Batch, s.Events, s.Since = m.batch, uint64(m.events), timestamppb.New(m.since)
		s.Step, s.Error = m.step, m.failure
	}
	return s
}

// reason is what Status reports for a failed batch, in fixed words. The full error goes to the
// log only, because it can quote file names and tool output, and any client with the secret may
// ask. The first rule that matches wins:
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
	return "the batch failed; the service log has the error"
}

// allocated skips a path that vanishes during the walk, because the updater deletes applied
// suspect files concurrently.
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
// that fails the batch on every retry. The cases dropped:
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
// A failed run fails the batch, so it is retried rather than acknowledged incomplete.
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
// large index makes the batch wait for it. A failed lookup loses only the batch's hard-link marks:
// the other names keep a stale row until their directory changes again.
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
//     corrupt. A retry would fail the same way and hold the batch forever, so the line is logged
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
			m.log.Warn("gufi_query could not read a directory's index database; its rows are missing from this batch",
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
