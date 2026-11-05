package collector

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
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

	"github.com/thinkparq/protobuf/go/beeindex"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ownInode selects a directory's own inode. A rolled-up index adds summary rows for
// subdirectories, with isroot clear.
const ownInode = "inode FROM summary WHERE isroot = 1"

// errIndex marks a failure to read the index tree itself, as opposed to a query over it.
var errIndex = errors.New("index not readable")

// errQuery marks a failed gufi_query run.
var errQuery = errors.New("index query failed")

// lookup turns a closed batch into the own inodes of the index directories the batch changed.
// The hand-over writes those inodes to a suspect file, one "<inode> d" line per directory whose
// own index database must be rebuilt.
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
type lookup struct {
	index      string // the index root, with symlinks resolved, because indexDirs uses lstat
	gufiQuery  string
	gufiSqlite string
	scratch    string // holds the path lists and the hard-link database of one lookup
	threads    int
	log        *zap.Logger

	// What convert is doing, for status. convert writes these fields, status reads them.
	mu      sync.Mutex
	state   beeindex.MarkwalkStatus_State
	batch   uint64
	events  int
	since   time.Time // when the lookup started, or, while failing, when the first failure was
	step    string
	failure string
	// failingSince is when lookups started failing. A successful lookup clears it.
	failingSince time.Time
	done         uint64 // batches looked up
	found        uint64 // inodes those lookups found, counted per batch
}

// check refuses a wrong setting at startup instead of failing every lookup.
func (l *lookup) check(ctx context.Context) error {
	for _, bin := range []string{l.gufiQuery, l.gufiSqlite} {
		if _, err := exec.LookPath(bin); err != nil {
			return err
		}
	}
	out, err := l.query(ctx, "--max-level", "0", "-S", "SELECT "+ownInode)
	if err != nil {
		return err
	}
	inos, err := inodes(out)
	if err != nil {
		return err
	}
	if len(inos) != 1 {
		return fmt.Errorf("%s is not an index root: no summary row", l.index)
	}
	return nil
}

// convert looks batch b up in the index and returns the inodes to rebuild, with what it found for
// the caller's log line. A directory can be found both ways, through its path and through a hard
// link's other name, so an inode can come back twice. The status reports each step. The rules:
//   - A batch that names no directory of the index runs no query and returns no inode. Its events
//     changed nothing in the index, so the hand-over may acknowledge them.
//   - On an error convert returns nothing. The caller keeps the batch and retries it, so its
//     events stay unacknowledged.
func (l *lookup) convert(ctx context.Context, b *batch) (inos []uint64, fields []zap.Field, err error) {
	l.begin(b)
	defer func() { l.end(len(inos), err) }()
	l.setStep("finding the changed directories in the index")
	listed, dropped, err := l.indexDirs(b.dirs)
	if err != nil {
		return nil, nil, err
	}
	l.log.Debug("changed directories", zap.Uint64("batch", b.n), zap.Strings("in_index", listed), zap.Int("not_in_index", dropped))
	fields = []zap.Field{zap.Uint64("batch", b.n), zap.Int("events", b.events), zap.Int("directories", len(listed)),
		zap.Int("not_index_directories", dropped)}
	if len(listed) == 0 {
		return nil, append(fields, zap.Int("inodes", 0)), nil
	}
	// One lookup runs at a time, so one scratch directory serves them all.
	if err := os.MkdirAll(l.scratch, 0o755); err != nil {
		return nil, nil, err
	}
	defer os.RemoveAll(l.scratch)
	l.setStep("looking up inodes in the index")
	found, files, err := l.indexInodes(ctx, l.scratch, listed, linkedDirs(listed, b.links))
	if err != nil {
		return nil, nil, err
	}
	l.setStep("looking up the other names of hard links")
	others, err := l.otherNames(ctx, l.scratch, files)
	if err != nil {
		return nil, nil, err
	}
	inos = slices.Concat(found, others)
	return inos, append(fields, zap.Int("inodes", len(inos)), zap.Int("multi_name_files", len(files)),
		zap.Int("other_name_dirs", len(others))), nil
}

// begin records that the lookup of b started.
func (l *lookup) begin(b *batch) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.state, l.batch, l.events, l.since = beeindex.MarkwalkStatus_PROCESSING, b.n, b.events, time.Now()
	l.step, l.failure = "", ""
}

func (l *lookup) setStep(step string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.step = step
}

// end records how a lookup ended. A failure stays in the status until a later lookup succeeds,
// and the status keeps the time of the first failure across the retries that fail after it.
func (l *lookup) end(n int, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err != nil {
		if l.failingSince.IsZero() {
			l.failingSince = time.Now()
		}
		l.state, l.since, l.step, l.failure = beeindex.MarkwalkStatus_FAILING, l.failingSince, "", reason(err)
		return
	}
	l.state, l.batch, l.events, l.since = beeindex.MarkwalkStatus_WAITING, 0, 0, time.Time{}
	l.step, l.failure, l.failingSince = "", "", time.Time{}
	l.done++
	l.found += uint64(n)
}

// status reports what convert is doing, for the IndexUpdater API. BatchesDone counts the batches
// looked up, and SuspectDirs the inodes their lookups found.
func (l *lookup) status() *beeindex.MarkwalkStatus {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := &beeindex.MarkwalkStatus{State: l.state, BatchesDone: l.done, SuspectDirs: l.found}
	if l.state != beeindex.MarkwalkStatus_WAITING {
		s.Batch, s.Events, s.Since = l.batch, uint64(l.events), timestamppb.New(l.since)
		s.Step, s.Error = l.step, l.failure
	}
	return s
}

// reason is what status reports for a failed lookup, in fixed words. The full error goes to the
// log only, because it can quote file names and tool output, and any client that reaches the
// service may ask. The first rule that matches wins:
//   - the work directory's file system is out of space;
//   - the index tree cannot be read, reported with the system error;
//   - an index query failed.
func reason(err error) string {
	var errno syscall.Errno
	switch {
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

// indexDirs keeps the paths that are directories in the index tree and drops the rest. gufi_query
// opens every path-list entry as a directory, and any failure but ENOENT prints an "Error:" line
// that fails the batch's lookup on every retry. The cases dropped:
//   - a file, or a path through a file, such as a user file named db.db where the parent's
//     database sits;
//   - a missing path or one too long to be in the index;
//   - a path with "\n" or "\r", because gufi_query reads its path list line by line and strips
//     "\r", or with a NUL byte, which no path can hold. Such a directory is rebuilt only when its
//     place in the tree changes or a repair runs.
func (l *lookup) indexDirs(dirs map[string]struct{}) (listed []string, dropped int, err error) {
	for _, d := range slices.Sorted(maps.Keys(dirs)) {
		if strings.ContainsAny(d, "\n\r\x00") {
			l.log.Debug("not an index directory: control character in the path", zap.String("path", strconv.Quote(d)))
			dropped++
			continue
		}
		info, err := os.Lstat(filepath.Join(l.index, d))
		switch {
		case err == nil && info.IsDir():
			listed = append(listed, d)
		case err == nil, errors.Is(err, fs.ErrNotExist), errors.Is(err, syscall.ENOTDIR), errors.Is(err, syscall.ENAMETOOLONG):
			l.log.Debug("not an index directory", zap.String("path", d), zap.NamedError("lstat", err))
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
func (l *lookup) indexInodes(ctx context.Context, scratch string, listed, linked []string) (dirInodes, fileInodes []uint64, err error) {
	dirInodes, err = l.queryList(ctx, filepath.Join(scratch, "dirs"), listed, "-S", "SELECT "+ownInode)
	if err != nil || len(linked) == 0 {
		return dirInodes, nil, err
	}
	fileInodes, err = l.queryList(ctx, filepath.Join(scratch, "linkdirs"), linked, "-E", "SELECT inode FROM entries WHERE nlink > 1")
	return dirInodes, fileInodes, err
}

// queryList writes dirs to the path-list file list and runs one query over those directories.
// A failed run fails the lookup, so the batch is kept rather than acknowledged incomplete.
func (l *lookup) queryList(ctx context.Context, list string, dirs []string, sql ...string) ([]uint64, error) {
	if err := os.WriteFile(list, []byte(strings.Join(dirs, "\n")+"\n"), 0o644); err != nil {
		return nil, err
	}
	out, err := l.query(ctx, slices.Concat([]string{"--min-level", "1", "--max-level", "1", "--path-list", list}, sql)...)
	if err != nil {
		return nil, fmt.Errorf("index lookup: %w", err)
	}
	return inodes(out)
}

// otherNames returns the own inode of every directory in the index that lists one of
// fileInodes, in one walk of the whole index. The inodes go into <scratch>/links.db, which every
// gufi_query thread attaches through -I. In the SQL text they would cost one walk per few
// thousand inodes, because Linux caps one argument at 128 KiB. The walk has no time limit, so on a
// large index the collector reads no event until it ends. A failed walk loses only the batch's
// hard-link marks: the other names keep a stale row until their directory changes again.
func (l *lookup) otherNames(ctx context.Context, scratch string, fileInodes []uint64) ([]uint64, error) {
	if len(fileInodes) == 0 {
		return nil, nil
	}
	slices.Sort(fileInodes)
	fileInodes = slices.Compact(fileInodes)
	db := filepath.Join(scratch, "links.db")
	defer os.Remove(db)
	err := l.writeLinks(ctx, db, fileInodes)
	var out []byte
	if err == nil {
		out, err = l.query(ctx, "-I", "ATTACH "+sqlString(db)+" AS links",
			"-E", "SELECT (SELECT "+ownInode+") FROM entries WHERE nlink > 1 AND inode IN (SELECT inode FROM links.inodes)")
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		l.log.Warn("hard-link lookup failed; the other names of these files keep a stale row",
			zap.Int("files", len(fileInodes)), zap.Error(err))
		return nil, nil
	}
	return inodes(out)
}

// writeLinks writes one statement per line, because gufi_sqlite3 runs its standard input line by
// line. Inodes are text, as GUFI stores them in entries.
func (l *lookup) writeLinks(ctx context.Context, db string, inos []uint64) error {
	if err := os.Remove(db); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	var sql strings.Builder
	sql.WriteString("CREATE TABLE inodes(inode TEXT PRIMARY KEY);\nBEGIN;\n")
	for _, ino := range inos {
		fmt.Fprintf(&sql, "INSERT INTO inodes VALUES ('%d');\n", ino)
	}
	sql.WriteString("COMMIT;\n")
	cmd := exec.CommandContext(ctx, l.gufiSqlite, db)
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
func (l *lookup) query(ctx context.Context, args ...string) ([]byte, error) {
	args = append(append([]string{"-n", strconv.Itoa(l.threads), "-d", " "}, args...), l.index)
	cmd := exec.CommandContext(ctx, l.gufiQuery, args...)
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
			l.log.Warn("gufi_query could not read a directory's index database; its rows are missing from this batch's lookup",
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
