package collector

import (
	"path"
	"strings"

	"github.com/thinkparq/beegfs-go/index/internal/updater"
	bw "github.com/thinkparq/protobuf/go/beewatch"
)

// batch is what the collector keeps of the events it read since the last lookup: the directories
// they changed, the names that may have other names elsewhere, and their sequence IDs per
// metadata server. A path is held once, however many events name it, so the batch never holds
// whole events. The lookup turns a closed batch into inodes, see lookup.convert.
type batch struct {
	n         uint64 // the batch's number since the service started, for the log and the status
	events    int
	dirs      map[string]struct{}
	links     map[string]struct{}
	delivered map[uint32][]updater.SeqRange
}

func newBatch(n uint64) *batch {
	return &batch{n: n, dirs: map[string]struct{}{}, links: map[string]struct{}{}, delivered: map[uint32][]updater.SeqRange{}}
}

// add records what ev changed in b: the directories, and the name when the entry may have
// another name elsewhere. It records ev's sequence ID too, because the hand-over acknowledges
// every delivered event.
func (b *batch) add(ev *bw.Event, trackOpens bool) {
	paths, link := pathsFor(ev.GetV2(), trackOpens)
	for _, p := range paths {
		b.dirs[p] = struct{}{}
	}
	if link != "" {
		b.links[link] = struct{}{}
	}
	b.delivered[ev.GetMetaId()] = extend(b.delivered[ev.GetMetaId()], updater.SeqRange{First: ev.GetSeqId(), Last: ev.GetSeqId()})
	b.events++
}

// extend appends r to ranges, merging it into the last range when it follows on directly.
func extend(ranges []updater.SeqRange, r updater.SeqRange) []updater.SeqRange {
	if n := len(ranges); n > 0 && ranges[n-1].Last+1 == r.First {
		ranges[n-1].Last = r.Last
		return ranges
	}
	return append(ranges, r)
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
