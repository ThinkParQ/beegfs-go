// Package markwalk turns a batch of BeeGFS Watch events into a GUFI suspect file: one
// "<inode> d" line per directory whose own index database must be rebuilt, written to
// <work>/batch-<N>/suspects. Running gufi_incremental_update on the files is up to beegfs
// index update.
//
// The update finds new, moved and deleted directories by itself: it walks the tree, diffs it
// against the index by inode, and rebuilds every directory whose place in the tree changed. A
// suspect line is needed only for a directory that exists in the index at its current path and
// whose own rows changed:
//   - the parent of a created, deleted, renamed, linked, opened or written entry, because the
//     parent's database holds the entry's row and the parent's own times;
//   - the entry itself on an attribute or stripe change, in case it is a directory, because a
//     directory's attributes live in its own database;
//   - every directory that lists another name of a hard-linked file, because each name's row
//     carries the file's size, mode and link count, and only the index knows those directories.
//
// Nothing here reads the file system. The inodes come from the index: gufi_query reads each
// directory's own summary row. A path the index does not hold as a directory yields no row and
// no line, and that is exactly a directory the update rebuilds on its own.
package markwalk

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
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/renameio/v2"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
)

type Config struct {
	// Work is the directory each batch's files are written to, as batch-<N>/.
	Work string `mapstructure:"work"`
	// Index is the GUFI index root: the index directory that corresponds to the BeeGFS mount.
	Index string `mapstructure:"index"`
	// Query is the gufi_query binary.
	Query string `mapstructure:"query"`
	// QueryTimeout bounds one gufi_query run.
	QueryTimeout time.Duration `mapstructure:"query-timeout"`
	// TrackOpens includes open events so atime changes are indexed.
	TrackOpens bool `mapstructure:"track-opens"`
}

// entryIDPattern matches the entry ID of a file: three 32-bit hex fields
// (StorageTk::generateFileID). The reserved names root, disposal and mdisposal are directories,
// which never have other names. IDs go into SQL text on a command line, so they are checked,
// not escaped.
var entryIDPattern = regexp.MustCompile(`^[0-9A-Fa-f]{1,8}-[0-9A-Fa-f]{1,8}-[0-9A-Fa-f]{1,8}$`)

// idsPerQuery keeps each query's SQL under 100 KiB: Linux refuses a single argument of
// 128 KiB (MAX_ARG_STRLEN) with E2BIG, and a quoted ID takes at most 29 bytes.
const idsPerQuery = (100 << 10) / 29

// summaryInode selects a directory's own inode. Every db.db has one summary row with isroot set,
// the directory's own; a rolled-up index adds rows for subdirectories with isroot clear.
const summaryInode = "SELECT inode FROM summary WHERE isroot = 1;"

// Marker turns batches into suspect files.
type Marker struct {
	cfg Config
	log *zap.Logger
}

func New(cfg Config, log *zap.Logger) *Marker {
	return &Marker{cfg: cfg, log: log}
}

// Check reads the index root's inode once, so a wrong binary or index path is refused at
// startup instead of failing every batch.
func (m *Marker) Check(ctx context.Context) error {
	out, err := m.query(ctx, "--max-level", "0", "-S", summaryInode)
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
	m.log.Info("index root checked", zap.String("index", m.cfg.Index), zap.Uint64("inode", inos[0]))
	return nil
}

// Process writes the suspect file for one batch, <work>/batch-<n>/suspects, and returns once it
// is on disk, so the caller can acknowledge the events. Next to it, <work>/batch-<n>/dirs lists
// the directories the events named, which is what the index was asked about. A batch with
// nothing to mark leaves no suspect file.
func (m *Marker) Process(ctx context.Context, n uint64, events []*bw.Event) error {
	dirs := map[string]struct{}{} // index-relative directory paths, "." for the root
	var linked []string           // entry IDs of files with names in other directories, see siblingInodes
	var v1 int
	for _, ev := range events {
		v2 := ev.GetV2()
		if v2 == nil {
			v1++ // BeeGFS 7 format, not used
			continue
		}
		paths, id := pathsFor(v2, m.cfg.TrackOpens)
		for _, p := range paths {
			dirs[p] = struct{}{}
		}
		if id != "" {
			linked = append(linked, id)
		}
	}
	batchDir := BatchDir(m.cfg.Work, n)
	marks, err := m.dirInodes(ctx, batchDir, dirs)
	if err != nil {
		return err
	}
	siblings, err := m.siblingInodes(ctx, linked)
	if err != nil {
		return err
	}
	suspects := map[uint64]struct{}{}
	for _, ino := range slices.Concat(marks, siblings) {
		suspects[ino] = struct{}{}
	}
	out := filepath.Join(batchDir, "suspects")
	if len(suspects) == 0 {
		out = "none: nothing to mark"
	} else if err := writeSuspects(suspects, out); err != nil {
		return fmt.Errorf("write %s: %w", out, err)
	}
	m.log.Info("batch done", zap.Uint64("batch", n), zap.Int("events", len(events)), zap.Int("directories", len(dirs)),
		zap.Int("marks", len(suspects)), zap.Int("multi_name_files", len(linked)), zap.Int("v1_events_skipped", v1),
		zap.String("suspects", out))
	return nil
}

// BatchDir is the directory of batch n under work: batch-<n>, zero-padded to ten digits so
// names list in number order.
func BatchDir(work string, n uint64) string {
	return filepath.Join(work, fmt.Sprintf("batch-%010d", n))
}

// LastBatch returns the highest batch number present under work, so numbering continues after
// a restart. A missing work directory is batch 0.
func LastBatch(work string) (uint64, error) {
	entries, err := os.ReadDir(work)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var last uint64
	for _, e := range entries {
		digits, ok := strings.CutPrefix(e.Name(), "batch-")
		if !ok {
			continue
		}
		n, err := strconv.ParseUint(digits, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("%s: not a batch directory", filepath.Join(work, e.Name()))
		}
		last = max(last, n)
	}
	return last, nil
}

// pathsFor returns the index-relative paths of the directories ev dirties and, when the entry
// is a file that may have a name in a directory the event does not name, the entry's ID so
// siblingInodes can find that directory.
func pathsFor(ev *bw.V2Event, trackOpens bool) (dirs []string, linked string) {
	parent := dirOf(ev.GetPath())
	switch ev.GetType() {
	case bw.V2Event_INVALID, bw.V2Event_OPEN_BLOCKED, bw.V2Event_INODE_LOCKED:
		// No entry, a refused open, and a lock: nothing the index holds changed.
		return nil, ""

	case bw.V2Event_RENAME:
		// Fired once, on the source. A renamed directory is moved and rebuilt by the update.
		return []string{parent, dirOf(ev.GetTargetPath())}, ""

	case bw.V2Event_HARDLINK:
		return []string{parent, dirOf(ev.GetTargetPath())}, linkedEntryID(ev)

	case bw.V2Event_OPEN_READ, bw.V2Event_OPEN_WRITE, bw.V2Event_OPEN_READ_WRITE:
		if !trackOpens { // only atime, and very many events
			return nil, ""
		}
		return []string{parent}, linkedEntryID(ev)

	case bw.V2Event_SETATTR, bw.V2Event_STRIPE_PATTERN_CHANGED:
		// The entry may be a directory, whose attributes live in its own database. The event
		// does not say; the index answers only for a directory.
		return []string{parent, rel(ev.GetPath())}, linkedEntryID(ev)

	default:
		// MKDIR, RMDIR, CREATE, MKNOD, SYMLINK, UNLINK, the write events, and any type added
		// later: the parent's rows changed.
		return []string{parent}, linkedEntryID(ev)
	}
}

// rel turns an event path, which is absolute within the mount, into the form gufi_query's path
// list takes: relative to the index root, with "." for the root itself.
func rel(p string) string {
	if r := strings.TrimPrefix(path.Clean("/"+p), "/"); r != "" {
		return r
	}
	return "."
}

// dirOf is the index-relative path of the directory that lists p. The root lists itself.
func dirOf(p string) string {
	return path.Dir(rel(p))
}

// linkedEntryID returns the event's entry ID when the entry is a file that may have a name in a
// directory the event does not name, so that directory's row for it is stale. UNLINK reports the
// names left after it. HARDLINK reports the names including the new one, and both of its
// directories are marked already. Every other type reports the names including its own.
func linkedEntryID(ev *bw.V2Event) string {
	id := ev.GetEntryId()
	if !entryIDPattern.MatchString(id) {
		return ""
	}
	names := ev.GetNumLinks()
	switch ev.GetType() {
	case bw.V2Event_UNLINK:
		if names >= 1 {
			return id
		}
	case bw.V2Event_HARDLINK:
		if names > 2 {
			return id
		}
	case bw.V2Event_FLUSH, bw.V2Event_TRUNCATE, bw.V2Event_SETATTR,
		bw.V2Event_CLOSE_WRITE, bw.V2Event_LAST_WRITER_CLOSED,
		bw.V2Event_STRIPE_PATTERN_CHANGED,
		bw.V2Event_OPEN_READ, bw.V2Event_OPEN_WRITE, bw.V2Event_OPEN_READ_WRITE:
		if names > 1 {
			return id
		}
	}
	return ""
}

// dirInodes asks the index for the inode of every directory in dirs, in one gufi_query run over
// a path list written to <batchDir>/dirs. A path the index does not hold as a directory yields
// no row: it is new, moved, or a file, and the update handles all three without a line. A
// failed run is an error, so the batch is retried rather than acknowledged incomplete.
func (m *Marker) dirInodes(ctx context.Context, batchDir string, dirs map[string]struct{}) ([]uint64, error) {
	if len(dirs) == 0 {
		return nil, nil
	}
	if err := os.MkdirAll(batchDir, 0o755); err != nil {
		return nil, err
	}
	list := filepath.Join(batchDir, "dirs")
	var b strings.Builder
	for _, d := range slices.Sorted(maps.Keys(dirs)) {
		b.WriteString(d)
		b.WriteByte('\n')
	}
	if err := os.WriteFile(list, []byte(b.String()), 0o644); err != nil {
		return nil, err
	}
	out, err := m.query(ctx, "--min-level", "1", "--max-level", "1", "--path-list", list, "-S", summaryInode)
	if err != nil {
		return nil, fmt.Errorf("directory inode lookup: %w", err)
	}
	return inodes(out)
}

// siblingInodes asks the index which directories list these entry IDs, through the
// beegfs_entries table the BeeGFS plugin fills in, and returns each directory's inode from its
// own summary row. Every query walks the whole index, so it asks for as many IDs at once as
// fit. A failed or timed-out query loses only its own IDs: the other names of those files keep
// a stale row until the next change in their directory.
func (m *Marker) siblingInodes(ctx context.Context, entryIDs []string) ([]uint64, error) {
	if len(entryIDs) == 0 {
		return nil, nil
	}
	slices.Sort(entryIDs)
	var found []uint64
	for chunk := range slices.Chunk(slices.Compact(entryIDs), idsPerQuery) {
		sql := "SELECT (" + strings.TrimSuffix(summaryInode, ";") + ") FROM beegfs_entries WHERE entry_id IN ('" +
			strings.Join(chunk, "','") + "');"
		out, err := m.query(ctx, "-E", sql)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			m.log.Warn("hard-link lookup failed; the other names of these files keep a stale row",
				zap.Int("entry_ids", len(chunk)), zap.Error(err))
			continue
		}
		inos, err := inodes(out)
		if err != nil {
			return nil, err
		}
		found = append(found, inos...)
	}
	return found, nil
}

// query runs gufi_query over the index with one run's timeout and a space between columns, and
// returns its standard output: one line per row, from every directory the SQL matched.
func (m *Marker) query(ctx context.Context, args ...string) ([]byte, error) {
	qctx, cancel := context.WithTimeout(ctx, m.cfg.QueryTimeout)
	defer cancel()
	args = append(append([]string{"-d", " "}, args...), m.cfg.Index)
	out, err := exec.CommandContext(qctx, m.cfg.Query, args...).Output()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return nil, fmt.Errorf("%w: %s", err, bytes.TrimSpace(exit.Stderr))
	}
	return out, err
}

// inodes parses gufi_query output that holds one inode per row.
func inodes(out []byte) ([]uint64, error) {
	var inos []uint64
	for _, field := range strings.Fields(string(out)) {
		ino, err := strconv.ParseUint(field, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("gufi_query returned %q for an inode", field)
		}
		inos = append(inos, ino)
	}
	return inos, nil
}

// writeSuspects writes one "<inode> d" line per directory, in inode order so the file is
// reproducible.
func writeSuspects(dirs map[uint64]struct{}, out string) error {
	var b strings.Builder
	for _, ino := range slices.Sorted(maps.Keys(dirs)) {
		fmt.Fprintf(&b, "%d d\n", ino) // unsigned: GUFI matches inodes as text
	}
	return writeFileDurable(out, []byte(b.String()))
}

// writeFileDurable replaces path with data so that after a crash it holds either the old or
// the new content. renameio writes, fsyncs and renames a temp file; the directory is then
// fsynced too, so the rename itself survives a crash before the events are acknowledged.
func writeFileDurable(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	if err := renameio.WriteFile(path, data, 0o644, renameio.WithTempDir(dir)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
