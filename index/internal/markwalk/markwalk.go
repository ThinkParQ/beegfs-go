// Package markwalk turns the collector's segment files into GUFI suspect files.
//
// It reads sealed segments in number order and turns each batch into one "<inode> d" line per
// directory to rescan, written to <work>/batch-<last segment>/suspects. Only then does the
// state file move past the batch, so a crash repeats a batch and never skips one. Running
// gufi_incremental_update on the file is up to beegfs index update.
package markwalk

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	bw "github.com/thinkparq/protobuf/go/beewatch"
	"go.uber.org/zap"
)

// Config holds markwalk's configuration.
type Config struct {
	// Spool is the directory the collector writes segment files to, set from the shared spool option.
	Spool string `mapstructure:"-"`
	// State is the file recording the last segment whose suspect file was written.
	State string `mapstructure:"state"`
	// Work is the directory each batch's suspect file is written to, as batch-<N>/suspects.
	Work string `mapstructure:"work"`
	// Mount is the path where BeeGFS is mounted, so event paths can be made absolute.
	Mount string `mapstructure:"mount"`
	// Index is the GUFI index root.
	Index string `mapstructure:"index"`
	// Query is the gufi_query binary used to find the other names of a multi-link inode. Empty
	// disables the lookup.
	Query string `mapstructure:"query"`
	// Every is the least time between batches.
	Every time.Duration `mapstructure:"every"`
	// MaxSegments is the most segments in one batch.
	MaxSegments int `mapstructure:"max-segments"`
	// StatCache is the most stat results a batch keeps.
	StatCache int `mapstructure:"stat-cache"`
	// TrackOpens includes open events so atime changes are indexed.
	TrackOpens bool `mapstructure:"track-opens"`
	// Retain removes segments once their suspect file is written, keeping the newest.
	Retain bool `mapstructure:"retain"`
}

// Run makes batches until ctx is done. A nudge on sealed means the collector sealed a
// segment. A failed batch is retried after a growing delay, so that nothing is skipped and
// the collector sharing the process is never stopped.
func Run(ctx context.Context, cfg Config, log *zap.Logger, sealed <-chan struct{}) error {
	cfg.Mount, cfg.Index = filepath.Clean(cfg.Mount), filepath.Clean(cfg.Index)
	return (&reader{Config: cfg, log: log}).run(ctx, sealed)
}

// dirsFor returns the directories an event dirties and, when the inode has other names, its
// entry ID so siblingDirs can find them. A listing change dirties the parent. An inode change
// names the path itself, which markChain keeps if it is a directory (its attributes live in
// its own db.db) and otherwise replaces with the parent.
func dirsFor(ev *bw.V2Event, mount string, trackOpens bool) (dirs []string, entryID string) {
	path := filepath.Join(mount, ev.GetPath()) // "/" for the root makes the mount itself
	parent := filepath.Dir(path)

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
		dst := filepath.Dir(target)
		return []string{commonAncestor(parent, dst), parent, dst, target}, ""

	case bw.V2Event_HARDLINK:
		// nlink is stored per name, so the other name's directory is stale too.
		return []string{parent, filepath.Dir(filepath.Join(mount, ev.GetTargetPath()))}, linkedEntryID(ev)

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
// it asks for as many IDs at once as fit; a failed query loses only its own IDs.
func siblingDirs(ctx context.Context, log *zap.Logger, entryIDs []string, mount, indexRoot, queryBin string) []string {
	if queryBin == "" {
		return nil
	}
	var dirs []string
	seen := map[string]bool{}
	for chunk := range slices.Chunk(entryIDs, idsPerQuery) {
		sql := "SELECT path() FROM beegfs_entries WHERE entry_id IN ('" + strings.Join(chunk, "','") + "');"
		out, err := exec.CommandContext(ctx, queryBin, "-d", "\n", "-E", sql, indexRoot).Output()
		if err != nil {
			log.Warn("sibling lookup failed; other names of these multi-name files keep a stale size",
				zap.Int("entry_ids", len(chunk)), zap.Error(err))
			continue
		}
		for _, line := range strings.Split(string(out), "\n") {
			if line = strings.TrimSpace(line); line == "" {
				continue
			}
			rel, err := filepath.Rel(indexRoot, line)
			if err != nil || strings.HasPrefix(rel, "..") {
				continue
			}
			if src := filepath.Join(mount, rel); !seen[src] {
				seen[src] = true
				dirs = append(dirs, src)
			}
		}
	}
	return dirs
}

// statCache remembers, for one batch, whether a path is a directory and its inode, so paths
// many marks share cost one metadata round trip. It drops the least recently used answer
// beyond its limit; forgetting only means asking again.
type statCache struct {
	lru   *simplelru.LRU[string, dirStat]
	stats int // os.Stat calls made
}

type dirStat struct {
	inode uint64
	isDir bool
}

func newStatCache(limit int) *statCache {
	lru, err := simplelru.NewLRU[string, dirStat](max(limit, 1), nil)
	if err != nil {
		panic(err) // only for a size below 1
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

// commonAncestor returns the deepest directory that holds both a and b.
func commonAncestor(a, b string) string {
	for !within(b, a) && filepath.Dir(a) != a {
		a = filepath.Dir(a)
	}
	return a
}

// within reports whether path is root or below it.
func within(path, root string) bool {
	return path == root || strings.HasPrefix(path, root+string(filepath.Separator))
}
