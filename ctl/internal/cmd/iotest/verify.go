package iotest

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/verifier"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

func newVerifyCmd() *cobra.Command {
	var (
		path    string
		verbose bool
	)
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify a data file written by smoke or soak",
		Long: `Verify a data file written by smoke or soak.
Every byte range is classified by coverage and checked against its stored xattr header.
Anomalies are always printed; --verbose prints all spans including clean ones.

If --path is a directory, every iotest-soak-* data file in it (as produced
by 'beegfs iotest start soak --path <dir>') is verified in turn.`,
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if helpIfNoFlags(cmd) {
				return nil
			}
			if path == "" {
				return fmt.Errorf("required flag \"path\" not set")
			}

			info, err := os.Stat(path)
			if err != nil {
				return fmt.Errorf("stat: %w", err)
			}

			files := []string{path}
			if info.IsDir() {
				files, err = filepath.Glob(filepath.Join(path, "iotest-soak-*"))
				if err != nil {
					return fmt.Errorf("glob: %w", err)
				}
				if len(files) == 0 {
					return fmt.Errorf("%s: no iotest-soak-* data files found", path)
				}
				sort.Strings(files)
			}

			totalAnomalies := 0
			for _, f := range files {
				anomalies, _, err := verifyOneFile(f, verbose)
				if err != nil {
					return err
				}
				totalAnomalies += anomalies
			}
			if totalAnomalies > 0 {
				return fmt.Errorf("FAIL: %d anomaly(s)", totalAnomalies)
			}
			fmt.Println("PASS")
			return nil
		},
	}
	cmd.Flags().StringVar(&path, "path", "", "data file or soak directory to verify")
	cmd.Flags().BoolVar(&verbose, "verbose", false, "print all spans, not just anomalies")
	return cmd
}

// verifyOneFile verifies a single data file, printing per-span output and a
// summary line. It returns the number of anomalous spans found.
func verifyOneFile(path string, verbose bool) (anomalies, total int, err error) {
	f, err := fileops.Open(path, os.O_RDONLY, 0)
	if err != nil {
		return 0, 0, fmt.Errorf("open: %w", err)
	}
	defer f.Close()

	store, err := xattrstore.OpenStore(path)
	if err != nil {
		return 0, 0, fmt.Errorf("OpenStore: %w", err)
	}

	if err := verifyCheckXattrPresence(path, store); err != nil {
		return 0, 0, err
	}

	err = verifier.VerifyFile(store, f, verifier.Options{}, func(span verifier.Span) error {
		total++
		ok, line, detail := verifySummariseSpan(span)
		if !ok {
			anomalies++
		}
		if verbose || !ok {
			fmt.Print(line)
			if detail != "" {
				fmt.Print(detail)
			}
		}
		return nil
	})
	if err != nil {
		return anomalies, total, fmt.Errorf("verify %s: %w", path, err)
	}

	fmt.Printf("\n%s: %d span(s) checked", path, total)
	if anomalies > 0 {
		fmt.Printf(", %d anomaly(s)\n", anomalies)
	} else {
		fmt.Println(", 0 anomalies")
	}
	return anomalies, total, nil
}

func verifyCheckXattrPresence(path string, store *xattrstore.Store) error {
	info, err := os.Stat(path)
	if err != nil || info.Size() == 0 {
		return nil
	}
	n := 0
	if err := store.ForEachEntry(func(_, _ int64, _ []byte) error {
		n++
		return nil
	}); err != nil {
		// A listxattr failure (permissions, the file vanishing mid-check, ...)
		// is a different problem than "genuinely zero xattr records" -- don't
		// let it get misdiagnosed as missing xattr support below.
		return fmt.Errorf("%s: listing xattrs: %w", path, err)
	}
	if n == 0 {
		return fmt.Errorf("%s: no iotest xattr records found on a non-empty file\n"+
			"Possible causes:\n"+
			"  - Filesystem does not support user xattrs\n"+
			"    BeeGFS: add 'client_extra_mount_options = user_xattr' to beegfs-client.conf\n"+
			"  - File was copied without preserving xattrs (use 'cp --preserve=xattr')\n"+
			"  - File was not written by verifyio", path)
	}
	return nil
}

func verifySummariseSpan(s verifier.Span) (ok bool, line, detail string) {
	switch s.Coverage {
	case verifier.CoverageOne:
		clean := s.Verdict == block.VerdictOK && s.XattrMatch
		xattrStr := "match"
		if !s.XattrMatch {
			xattrStr = "MISMATCH"
		}
		status := "ok"
		if !clean {
			status = "FAIL"
		}
		line = fmt.Sprintf("offset=%-10d length=%-8d coverage=one      verdict=%-18s xattr=%-8s %s\n",
			s.Offset, s.Length, s.Verdict, xattrStr, status)
		if s.Header != nil {
			detail = fmt.Sprintf("  kind=%-10s worker=%-4d cycle=%-6d node=%s\n",
				s.Header.Kind, s.Header.WorkerID, s.Header.Cycle,
				block.NodeNameString(s.Header.NodeName))
		}
		return clean, line, detail

	case verifier.CoverageNone:
		status := "ok"
		if !s.AllZero {
			status = "FAIL (non-zero bytes in uncovered region)"
		}
		line = fmt.Sprintf("offset=%-10d length=%-8d coverage=none     allzero=%-5v %s\n",
			s.Offset, s.Length, s.AllZero, status)
		return s.AllZero, line, ""

	case verifier.CoverageMany:
		line = fmt.Sprintf("offset=%-10d length=%-8d coverage=many     FAIL (%d overlapping records)\n",
			s.Offset, s.Length, len(s.Entries))
		return false, line, ""

	case verifier.CoverageContended:
		line = fmt.Sprintf("offset=%-10d length=%-8d coverage=contended (lock busy; skipped)\n",
			s.Offset, s.Length)
		return true, line, ""

	default:
		line = fmt.Sprintf("offset=%-10d length=%-8d coverage=unknown(%d)\n",
			s.Offset, s.Length, int(s.Coverage))
		return false, line, ""
	}
}
