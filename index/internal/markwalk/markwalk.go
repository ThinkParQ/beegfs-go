// Package markwalk turns a batch of BeeGFS Watch events into a GUFI suspect file: one
// "<inode> d" line per directory to rescan, written to <work>/batch-<N>/suspects. Running
// gufi_incremental_update on the files is up to beegfs index update.
package markwalk

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/renameio/v2"
	"github.com/hashicorp/golang-lru/v2/simplelru"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
)

type Config struct {
	// Work is the directory each batch's suspect file is written to, as batch-<N>/suspects.
	Work string `mapstructure:"work"`
	// Mount is the path where BeeGFS is mounted, so event paths can be made absolute.
	Mount string `mapstructure:"mount"`
	// Index is the GUFI index root.
	Index string `mapstructure:"index"`
	// Query is the gufi_query binary used to find the other names of a multi-link inode. Empty
	// disables the lookup.
	Query string `mapstructure:"query"`
	// QueryTimeout bounds one gufi_query run; on timeout the batch goes on without those names.
	QueryTimeout time.Duration `mapstructure:"query-timeout"`
	// StatCache is the most stat results a batch keeps.
	StatCache int `mapstructure:"stat-cache"`
	// TrackOpens includes open events so atime changes are indexed.
	TrackOpens bool `mapstructure:"track-opens"`
}

// Process writes the suspect file for one batch, <work>/batch-<n>/suspects, and returns once it
// is on disk, so the caller can acknowledge the events. With nothing to mark it writes nothing.
// A cancelled ctx returns ctx.Err() before the file is written, because the sibling lookup it
// cut short would leave directories out.
func Process(ctx context.Context, cfg Config, log *zap.Logger, n uint64, events []*bw.Event) error {
	mount, index := filepath.Clean(cfg.Mount), filepath.Clean(cfg.Index)
	dirs := map[string]struct{}{}
	links := map[string]struct{}{} // entry IDs of multi-name files, see siblingDirs
	var v1 int
	for _, ev := range events {
		v2 := ev.GetV2()
		if v2 == nil {
			v1++ // BeeGFS 7 format, not used
			continue
		}
		ds, id := dirsFor(v2, mount, cfg.TrackOpens)
		for _, d := range ds {
			dirs[d] = struct{}{}
		}
		if id != "" {
			links[id] = struct{}{}
		}
	}
	marked := append(slices.Collect(maps.Keys(dirs)),
		siblingDirs(ctx, log, slices.Collect(maps.Keys(links)), mount, index, cfg.Query, cfg.QueryTimeout)...)
	if err := ctx.Err(); err != nil {
		return err
	}
	out := filepath.Join(BatchDir(cfg.Work, n), "suspects")
	marks, stats, err := writeSuspects(marked, mount, index, out, cfg.StatCache)
	if err != nil {
		return fmt.Errorf("write %s: %w", out, err)
	}
	if marks == 0 {
		out = "none: nothing left to mark"
	}
	log.Info("batch done", zap.Uint64("batch", n), zap.Int("events", len(events)),
		zap.Int("directories", len(dirs)), zap.Int("multi_name_inodes", len(links)), zap.Int("marks", marks),
		zap.Int("stats", stats), zap.Int("v1_events_skipped", v1), zap.String("suspects", out))
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

// dirsFor returns the directories an event dirties and, when the inode has other names, its
// entry ID so siblingDirs can find them. A listing change dirties the parent. An inode change
// names the path itself, which markChain keeps if it is a directory (its attributes live in
// its own db.db) and otherwise replaces with the parent.
func dirsFor(ev *bw.V2Event, mount string, trackOpens bool) (dirs []string, entryID string) {
	path := filepath.Join(mount, ev.GetPath())
	parent := dirOf(path, mount)

	switch ev.GetType() {
	case bw.V2Event_INVALID, bw.V2Event_OPEN_BLOCKED, bw.V2Event_INODE_LOCKED:
		// No path, a refused open, and a bare filename: nothing to mark.
		return nil, ""

	case bw.V2Event_MKDIR:
		// The new directory needs its own line, or GUFI creates it without a db.db.
		return []string{parent, path}, ""

	case bw.V2Event_RENAME:
		// Fired once, on the source. Without the common ancestor GUFI sees a delete plus a
		// create and throws the moved databases away instead of moving them.
		target := filepath.Join(mount, ev.GetTargetPath())
		dst := dirOf(target, mount)
		return []string{commonAncestor(parent, dst), parent, dst, target}, ""

	case bw.V2Event_HARDLINK:
		// nlink is stored per name, so the other name's directory is stale too.
		return []string{parent, dirOf(filepath.Join(mount, ev.GetTargetPath()), mount)}, linkedEntryID(ev)

	case bw.V2Event_OPEN_READ, bw.V2Event_OPEN_WRITE, bw.V2Event_OPEN_READ_WRITE:
		if !trackOpens { // only atime, and very many events
			return nil, ""
		}
		return []string{parent}, linkedEntryID(ev)

	case bw.V2Event_SETATTR, bw.V2Event_STRIPE_PATTERN_CHANGED:
		return []string{path}, linkedEntryID(ev) // can be a directory's own inode

	case bw.V2Event_FLUSH, bw.V2Event_TRUNCATE, bw.V2Event_CLOSE_WRITE, bw.V2Event_LAST_WRITER_CLOSED:
		// Only files get these, so name the parent directly: naming the file costs a stat of
		// every written file, millions of gone names for a tree written and removed.
		return []string{parent}, linkedEntryID(ev)

	default:
		// CREATE, MKNOD, SYMLINK, RMDIR, UNLINK, and any type added later: the listing changed.
		return []string{parent}, linkedEntryID(ev)
	}
}

// dirOf returns the directory that lists path. The root lists itself: its parent is outside
// the mount.
func dirOf(path, mount string) string {
	if path == mount {
		return mount
	}
	return filepath.Dir(path)
}

// linkedEntryID returns the event's entry ID when it changed an inode that has other names,
// whose directories then report a stale size, times or link count. UNLINK counts the names
// left after it (3 names, rm -> 2); every other type counts its own name too.
func linkedEntryID(ev *bw.V2Event) string {
	id := ev.GetEntryId()
	if !entryIDPattern.MatchString(id) {
		return ""
	}
	switch ev.GetType() {
	case bw.V2Event_UNLINK:
		if ev.GetNumLinks() >= 1 {
			return id
		}
	case bw.V2Event_FLUSH, bw.V2Event_TRUNCATE, bw.V2Event_SETATTR,
		bw.V2Event_CLOSE_WRITE, bw.V2Event_LAST_WRITER_CLOSED,
		bw.V2Event_STRIPE_PATTERN_CHANGED, bw.V2Event_HARDLINK,
		bw.V2Event_OPEN_READ, bw.V2Event_OPEN_WRITE, bw.V2Event_OPEN_READ_WRITE:
		if ev.GetNumLinks() > 1 {
			return id
		}
	}
	return ""
}

// entryIDPattern matches a BeeGFS entry ID: three 32-bit hex fields (StorageTk::generateFileID)
// or a reserved name. IDs go into SQL text on a command line, so they are checked, not escaped.
var entryIDPattern = regexp.MustCompile(`^(?:[0-9A-Fa-f]{1,8}-[0-9A-Fa-f]{1,8}-[0-9A-Fa-f]{1,8}|root|disposal|mdisposal)$`)

// idsPerQuery keeps each query's SQL under 100 KiB: Linux refuses a single argument of
// 128 KiB (MAX_ARG_STRLEN) with E2BIG, and a quoted ID takes at most 29 bytes.
const idsPerQuery = (100 << 10) / 29

// siblingDirs asks the index which directories list these entry IDs, through the
// beegfs_entries table the BeeGFS plugin fills in. Every query walks the whole index, so
// it asks for as many IDs at once as fit; a failed or timed-out query loses only its own IDs.
// gufi_query prints each directory as the index root it was given plus the relative path,
// so swapping the root for the mount names the source directory.
func siblingDirs(ctx context.Context, log *zap.Logger, entryIDs []string, mount, indexRoot, queryBin string, timeout time.Duration) []string {
	if queryBin == "" {
		return nil
	}
	var dirs []string
	seen := map[string]bool{}
	for chunk := range slices.Chunk(entryIDs, idsPerQuery) {
		sql := "SELECT path() FROM beegfs_entries WHERE entry_id IN ('" + strings.Join(chunk, "','") + "');"
		qctx, cancel := context.WithTimeout(ctx, timeout)
		out, err := exec.CommandContext(qctx, queryBin, "-d", "\n", "-E", sql, indexRoot).Output()
		cancel()
		if err != nil {
			log.Warn("sibling lookup failed; other names of these multi-name files keep a stale size",
				zap.Int("entry_ids", len(chunk)), zap.Error(err))
			continue
		}
		for _, line := range strings.Split(string(out), "\n") {
			if line = strings.TrimSpace(line); line == "" {
				continue
			}
			if src := filepath.Join(mount, strings.TrimPrefix(line, indexRoot)); !seen[src] {
				seen[src] = true
				dirs = append(dirs, src)
			}
		}
	}
	return dirs
}

// statCache remembers, for one batch, whether a path is a directory and its inode, so paths
// many marks share cost one stat. Beyond its limit it drops the least recently used answer;
// forgetting only means asking again.
type statCache struct {
	lru   *simplelru.LRU[string, dirStat]
	stats int // os.Stat calls made
}

type dirStat struct {
	inode uint64
	isDir bool
}

func newStatCache(limit int) *statCache {
	lru, err := simplelru.NewLRU[string, dirStat](limit, nil)
	if err != nil {
		panic(err) // a size below 1, which ValidateConfig refuses
	}
	return &statCache{lru: lru}
}

// stat reports whether path is a directory now, and its inode. The type check matters: a
// directory replaced by a file must not reach the suspect file as a directory.
func (c *statCache) stat(path string) (inode uint64, isDir bool) {
	if s, ok := c.lru.Get(path); ok {
		return s.inode, s.isDir
	}
	c.stats++
	var s dirStat
	if info, err := os.Stat(path); err == nil && info.IsDir() {
		s = dirStat{info.Sys().(*syscall.Stat_t).Ino, true}
	}
	c.lru.Add(path, s)
	return s.inode, s.isDir
}

// markChain returns the directories to mark for dir, shallowest first. It rises to the
// nearest path that is still a directory, then keeps rising while the parent has no db.db:
// GUFI creates index directories with a plain mkdir, so a parent missing from the index must
// be marked too. It checks for db.db rather than the directory because a GUFI run that died
// half way leaves directories without one.
func markChain(dir, mount, indexRoot string, stat func(string) (uint64, bool)) []string {
	cur := dir
	for {
		if _, ok := stat(cur); ok {
			break
		}
		if cur = filepath.Dir(cur); !within(cur, mount) {
			return nil
		}
	}
	chain := []string{cur}
	for cur != mount {
		parent := filepath.Dir(cur)
		rel, _ := filepath.Rel(mount, parent)
		if info, err := os.Stat(filepath.Join(indexRoot, rel, "db.db")); err == nil && info.Mode().IsRegular() {
			break
		}
		chain = append([]string{parent}, chain...)
		cur = parent
	}
	return chain
}

// writeSuspects writes one "<inode> d" line per directory to mark for dirs, deduplicated by
// inode, and returns how many lines and how many stats that took. With no lines it writes
// nothing.
func writeSuspects(dirs []string, mount, indexRoot, out string, cacheSize int) (marks, stats int, err error) {
	cache := newStatCache(cacheSize)
	inodes := map[uint64]struct{}{}
	for _, d := range dirs {
		for _, c := range markChain(d, mount, indexRoot, cache.stat) {
			if ino, ok := cache.stat(c); ok {
				inodes[ino] = struct{}{}
			}
		}
	}
	if len(inodes) == 0 {
		return 0, cache.stats, nil
	}
	var b strings.Builder
	for ino := range inodes {
		fmt.Fprintf(&b, "%d d\n", ino) // unsigned: GUFI matches inodes as text
	}
	return len(inodes), cache.stats, writeFileDurable(out, []byte(b.String()))
}

// commonAncestor returns the deepest directory that holds both a and b. Both lie within the
// mount, so the climb ends there at the latest.
func commonAncestor(a, b string) string {
	for !within(b, a) {
		a = filepath.Dir(a)
	}
	return a
}

// within reports whether path is root or below it.
func within(path, root string) bool {
	return path == root || strings.HasPrefix(path, root+string(filepath.Separator))
}
