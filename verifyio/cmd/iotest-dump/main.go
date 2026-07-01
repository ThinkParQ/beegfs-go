// iotest-dump -- print every block stored in a verifyio data file.
//
// For each record found in the xattr store, iotest-dump reads the
// corresponding bytes from the data file, verifies them, and prints a
// human-readable summary. Entries are printed in offset order.
//
//	iotest-dump -path /tmp/foo.dat
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/thinkparq/beegfs-go/verifyio/block"
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
		fmt.Fprintln(w, "Print every block stored in a verifyio data file.")
		fmt.Fprintln(w, "Reads xattrs to discover records, verifies each block, and prints a summary in offset order.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Flags:")
		flag.PrintDefaults()
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Examples:")
		fmt.Fprintln(w, "  iotest-dump -path /tmp/smoke.dat")
		fmt.Fprintln(w, "  iotest-dump -path /tmp/soak/iotest-soak-0.shared")
	}
	if len(os.Args) == 1 {
		flag.Usage()
		os.Exit(2)
	}
	flag.Parse()

	if *path == "" {
		flag.Usage()
		os.Exit(2)
	}

	f, err := os.Open(*path)
	if err != nil {
		die("open: %v", err)
	}
	defer f.Close()

	store, err := xattrstore.OpenStore(*path)
	if err != nil {
		die("OpenStore: %v", err)
	}

	var entries []entry
	if err := store.ForEachEntry(func(offset, length int64, header []byte) error {
		h := make([]byte, len(header))
		copy(h, header)
		entries = append(entries, entry{offset, length, h})
		return nil
	}); err != nil {
		die("ForEachEntry: %v", err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].offset < entries[j].offset
	})

	fmt.Printf("%s: %d block(s)\n\n", *path, len(entries))

	scratch := make([]byte, 4096)
	for _, e := range entries {
		h, err := block.UnmarshalHeader(e.header)
		if err != nil {
			fmt.Printf("offset=%-8d length=%-6d  header parse error: %v\n\n", e.offset, e.length, err)
			continue
		}

		buf := make([]byte, e.length)
		_, readErr := f.ReadAt(buf, e.offset)

		var verdictStr string
		if readErr != nil {
			verdictStr = fmt.Sprintf("READ_ERROR(%v)", readErr)
		} else {
			if int(h.BodyLen) > len(scratch) {
				scratch = make([]byte, h.BodyLen)
			}
			v, _ := block.VerifyBlock(buf, &h, scratch)
			verdictStr = v.String()
		}

		ts := time.Unix(0, h.TimeNs).UTC().Format(time.RFC3339Nano)
		fmt.Printf("offset=%-8d length=%-6d  verdict=%s\n", e.offset, e.length, verdictStr)
		fmt.Printf("  kind=%-10s worker=%-4d cycle=%-6d node=%s tid=%d\n",
			h.Kind, h.WorkerID, h.Cycle, block.NodeNameString(h.NodeName), h.TID)
		fmt.Printf("  time=%s\n", ts)
		fmt.Printf("  bodyLen=%-6d seed=0x%016x bodyCRC=0x%08x\n\n",
			h.BodyLen, h.Seed, h.BodyCRC)
	}
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
