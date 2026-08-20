package iotest

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

func newDumpCmd() *cobra.Command {
	var path string
	cmd := &cobra.Command{
		Use:   "dump",
		Short: "Print every block stored in a verifyio data file",
		Long: `Print every block stored in a verifyio data file.
Reads xattrs to discover records, verifies each block, and prints a summary in offset order.`,
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if helpIfNoFlags(cmd) {
				return nil
			}
			if path == "" {
				return fmt.Errorf("required flag \"path\" not set")
			}
			f, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("open: %w", err)
			}
			defer f.Close()

			store, err := xattrstore.OpenStore(path)
			if err != nil {
				return fmt.Errorf("OpenStore: %w", err)
			}

			type entry struct {
				offset, length int64
				header         []byte
			}
			var entries []entry
			if err := store.ForEachEntry(func(offset, length int64, header []byte) error {
				h := make([]byte, len(header))
				copy(h, header)
				entries = append(entries, entry{offset, length, h})
				return nil
			}); err != nil {
				return fmt.Errorf("ForEachEntry: %w", err)
			}

			sort.Slice(entries, func(i, j int) bool {
				return entries[i].offset < entries[j].offset
			})

			fmt.Printf("%s: %d block(s)\n\n", path, len(entries))

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
			return nil
		},
	}
	cmd.Flags().StringVar(&path, "path", "", "data file to inspect")
	return cmd
}
