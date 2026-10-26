//go:build linux

package posixbench

import (
	"os"

	"golang.org/x/sys/unix"
)

// dropCache advises the kernel to evict f's cached pages, so a read does not
// report the page cache's bandwidth instead of the device's.
//
// It only works where the filesystem actually uses the Linux page cache, and
// on BeeGFS that depends on the client's cache mode. The default is
// tuneFileCacheType=buffered, which uses the client's OWN buffers and registers
// no page-cache address_space operations -- so this call succeeds and evicts
// nothing. Under tuneFileCacheType=native it does work: that mode registers
// release_folio/invalidate_folio, which is what invalidate_mapping_pages needs.
//
// Do not read a dropCache call as a guarantee that the read phase measured
// storage. O_DIRECT is the mode-independent answer; this is best effort.
//
// Best-effort and ignored on failure -- FADV_DONTNEED is an advisory hint some
// filesystems do not implement, and failing hard over a hint would make the
// benchmark unusable on any filesystem lacking it, for no correctness gain.
// Only measurement accuracy is at stake.
func dropCache(f *os.File) {
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}
