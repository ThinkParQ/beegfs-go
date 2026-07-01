package iotest

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

var allVerdicts = []block.Verdict{
	block.VerdictOK,
	block.VerdictBodyCorrupt,
	block.VerdictBodyCRCMismatch,
	block.VerdictHeadBadCRC,
	block.VerdictHeadBadMagic,
	block.VerdictHeadBadFormat,
	block.VerdictTruncated,
}

func newSmokeCmd() *cobra.Command {
	var (
		path      string
		records   int
		blocksize int
		kind      string
		iotrace   int
		iologfile string
	)
	cmd := &cobra.Command{
		Use:   "smoke",
		Short: "Write N blocks to a file, storing each block's header in an xattr, then verify",
		Long: `Write N blocks to a file, storing each block's header in an xattr, then read back and verify.
The file is truncated on every run. Single-threaded and deterministic.`,
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			if helpIfNoFlags(cmd) {
				return nil
			}
			if records <= 0 {
				return fmt.Errorf("blocks must be > 0")
			}
			// blocksize is the exact number of bytes written per block; the body
			// fills it minus the in-data stripe CRCs. block.BodyLen rejects sizes
			// that can't be partitioned cleanly (rather than silently rounding).
			if _, err := block.BodyLen(blocksize); err != nil {
				return fmt.Errorf("invalid --blocksize: %w", err)
			}
			bodyKind, err := block.KindFromString(kind)
			if err != nil {
				return err
			}

			ioLog, cleanup, err := newIOTraceLogger(iotrace, iologfile)
			if err != nil {
				return err
			}
			defer cleanup()

			f, err := fileops.Open(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
			if err != nil {
				return fmt.Errorf("open %s: %w", path, err)
			}
			// A silent Close failure here is exactly the class of bug this tool
			// exists to catch -- don't let it slip past unchecked in smoke itself.
			defer func() {
				if closeErr := f.Close(); closeErr != nil && err == nil {
					err = closeErr
				}
			}()

			store, err := xattrstore.OpenStore(path)
			if err != nil {
				return fmt.Errorf("OpenStore: %w", err)
			}

			w, err := xattrstore.NewWriter(f, store, 0, bodyKind, blocksize, ioLog)
			if err != nil {
				return fmt.Errorf("NewWriter: %w", err)
			}

			// Write phase
			fmt.Printf("Writing %d blocks (%d bytes each, kind=%s) to %s\n", records, blocksize, bodyKind, path)
			writeStart := time.Now()
			for i := 0; i < records; i++ {
				if err := w.WriteBlock(int64(i)*int64(blocksize), fileops.IOTypeBuffered, false); err != nil {
					return fmt.Errorf("WriteBlock at i=%d: %w", i, err)
				}
			}
			if err := f.Sync(); err != nil {
				return fmt.Errorf("fsync: %w", err)
			}
			fmt.Printf("Wrote %d blocks (%d bytes) in %s\n",
				records, int64(records)*int64(blocksize), time.Since(writeStart).Round(time.Millisecond))

			// Verify phase
			fmt.Println("Verifying ...")
			buf := make([]byte, blocksize)
			counts := make(map[block.Verdict]int, len(allVerdicts))
			scratch := make([]byte, blocksize)
			verifyStart := time.Now()
			for i := 0; i < records; i++ {
				offset := int64(i) * int64(blocksize)
				if _, err := f.ReadAt(buf, offset); err != nil {
					return fmt.Errorf("ReadAt at i=%d: %w", i, err)
				}
				hdrBytes, getErr := store.Get(offset, int64(blocksize))
				if getErr != nil {
					counts[block.VerdictHeadBadMagic]++
					continue
				}
				h, hdrErr := block.UnmarshalHeader(hdrBytes)
				if hdrErr != nil {
					counts[block.VerdictHeadBadCRC]++
					continue
				}
				v, _ := block.VerifyBlock(buf, &h, scratch)
				counts[v]++
			}
			verifyElapsed := time.Since(verifyStart).Round(time.Millisecond)

			fmt.Println("Results:")
			for _, v := range allVerdicts {
				if c := counts[v]; c > 0 {
					fmt.Printf("  %-18s %d\n", v.String()+":", c)
				}
			}
			fmt.Printf("Verify took %s\n", verifyElapsed)

			if counts[block.VerdictOK] == records {
				fmt.Println("PASS")
				return nil
			}
			return fmt.Errorf("FAIL")
		},
	}
	cmd.Flags().StringVar(&path, "path", "/tmp/iotest-smoke.dat", "data file path")
	cmd.Flags().IntVar(&records, "blocks", 1000, "number of blocks to write")
	cmd.Flags().IntVar(&blocksize, "blocksize", 4096, "total bytes written per block on disk (the exact write size; use a power of two)")
	cmd.Flags().StringVar(&kind, "kind", "decimal", "body pattern: decimal | prng | repeat | countup | zeros | ones")
	cmd.Flags().IntVar(&iotrace, "iotrace", 0, "IO trace level (0=off, 1=error, 2=warn, 3=info, 4/5=debug)")
	cmd.Flags().StringVar(&iologfile, "iologfile", "", "IO trace destination file (default stderr)")
	return cmd
}
