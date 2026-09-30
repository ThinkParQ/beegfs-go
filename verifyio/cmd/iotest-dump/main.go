// iotest-dump -- print every block in a file written with an iotest tool.
//
// For each record found in the xattr store, iotest-dump reads the
// corresponding bytes from the data file, verifies them, and prints a
// human-readable summary. Entries are printed in offset order.
//
//	iotest-dump -path /tmp/iotest.dat
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/internal/climain"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

type entry struct {
	offset, length int64
	header         []byte
}

func main() {
	path := flag.String("path", "", "data file to inspect (required)")
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintln(w, "Usage: iotest-dump -path <file>")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Print every block in a file written with an iotest tool.")
		fmt.Fprintln(w, "Reads xattrs to discover records, verifies each block, and prints a summary in offset order.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Flags:")
		flag.PrintDefaults()
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Examples:")
		fmt.Fprintln(w, "  iotest-dump -path /tmp/iotest.dat")
	}
	climain.ExitIfNoArgs()
	flag.Parse()

	if *path == "" {
		flag.Usage()
		os.Exit(2)
	}

	f, err := fileops.Open(*path, os.O_RDONLY, 0)
	if err != nil {
		climain.Fail(climain.FailEnvironment, "open: %v", err)
	}
	defer f.Close()

	// Stat the open fd, not the path, so the size bounding every read below
	// describes the same file those reads come from.
	fileSize, err := f.Size()
	if err != nil {
		climain.Fail(climain.FailEnvironment, "stat: %v", err)
	}

	store, err := xattrstore.OpenStore(*path, xattrstore.DefaultLockTimeout)
	if err != nil {
		climain.Fail(climain.FailEnvironment, "OpenStore: %v", err)
	}
	defer store.Close()

	var entries []entry
	// Strict: a record whose xattr *name* doesn't parse has no extent to print
	// a block for, but staying quiet about it contradicts this tool's stated
	// job of printing every block stored -- ForEachEntry's plain form silently
	// drops exactly these.
	malformed, err := store.ForEachEntryStrict(func(offset, length int64, header []byte) error {
		h := make([]byte, len(header))
		copy(h, header)
		entries = append(entries, entry{offset, length, h})
		return nil
	})
	if err != nil {
		climain.Fail(climain.FailEnvironment, "ForEachEntryStrict: %v", err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].offset < entries[j].offset
	})

	fmt.Printf("%s: %d block(s)\n\n", *path, len(entries))
	for _, m := range malformed {
		fmt.Printf("MALFORMED: %s\n\n", m)
	}

	scratch := make([]byte, 4096)
	for _, e := range entries {
		h, err := block.UnmarshalHeader(e.header)
		if err != nil {
			fmt.Printf("offset=%-8d length=%-6d  header parse error: %v\n\n", e.offset, e.length, err)
			continue
		}

		// The record's placement (e.offset, e.length, from the xattr name) must
		// agree with what its own header claims. Nothing else checks this:
		// VerifyBlock sees only the body bytes, so a record whose size or
		// offset contradicts its header still verifies OK on content. Through
		// the SAME predicate the verifier uses, so a post-mortem here can never
		// call a record fine that iotest-verify calls broken.
		if v := block.RecordSelfCheck(&h, e.offset, e.length); v != block.VerdictOK {
			// bodyLen as well as the implied size: the implied size is -1
			// for any BodyLen past block.MaxBodyLen, so alone it says the header
			// was garbage without saying how -- and this `continue` skips the
			// detail block below, the only other place bodyLen is printed.
			fmt.Printf("offset=%-8d length=%-6d  verdict=%s (header says offset=%d, bodyLen=%d, size=%d)\n\n",
				e.offset, e.length, v, h.Offset, h.BodyLen, block.HeaderImpliedSize(&h))
			continue
		}

		// Both lengths here come off disk and neither has been checked against
		// the file: e.length from the xattr name, h.BodyLen from the header
		// (whose CRC32C is a checksum, not a MAC, so a valid HeadCRC can be
		// computed for any BodyLen). Size the buffers from what the file can
		// actually supply instead.
		var readErr error
		buf := make([]byte, xattrstore.ReadableLen(e.offset, e.length, fileSize))
		if len(buf) > 0 {
			_, readErr = f.ReadAt(buf, e.offset)
		}

		var verdictStr string
		if readErr != nil {
			verdictStr = fmt.Sprintf("READ_ERROR(%v)", readErr)
		} else {
			// VerifyBlock short-circuits to VerdictTruncated when BodyLen
			// exceeds the buffer, so scratch is never needed beyond that.
			if h.BodyLen <= uint64(len(buf)) && int(h.BodyLen) > len(scratch) {
				scratch = make([]byte, h.BodyLen)
			}
			// VerifyBlock's only error is an unrecognized header Kind, which it
			// reports as VerdictHeadBadFormat -- printed below. The error adds
			// only the offending kind value, so dropping it loses no verdict.
			v, _ := block.VerifyBlock(buf, &h, scratch)
			verdictStr = v.String()
		}

		ts := time.Unix(0, h.TimeNs).UTC().Format(time.RFC3339Nano)
		fmt.Printf("offset=%-8d length=%-6d  verdict=%s\n", e.offset, e.length, verdictStr)
		fmt.Printf("  kind=%-10s worker=%-4d cycle=%-6d node=%q tid=%d\n",
			h.Kind, h.WorkerID, h.Cycle, block.NodeNameString(h.NodeName), h.TID)
		fmt.Printf("  time=%s\n", ts)
		fmt.Printf("  bodyLen=%-6d seed=0x%016x bodyCRC=0x%08x\n\n",
			h.BodyLen, h.Seed, h.BodyCRC)
	}
}
