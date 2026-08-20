// iotest-verify -- verify one or more data files written by verifyio.
//
// Each span of the file is classified by coverage and verified against its
// stored xattr header. Anomalies are always printed; use -verbose to print
// all spans including clean ones.
//
//	iotest-verify -path /tmp/iotest-smoke.dat
//	iotest-verify -path /tmp/soak/iotest-soak-0.noOverlap -verbose
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/verifier"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

func main() {
	var (
		path    = flag.String("path", "", "data file to verify")
		verbose = flag.Bool("verbose", false, "print all spans, not just anomalies")
	)
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintln(w, "Usage: iotest-verify -path <file> [flags]")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Verify a data file written by iotest-smoke or iotest-soak.")
		fmt.Fprintln(w, "Every byte range is classified by coverage and checked against its stored xattr header.")
		fmt.Fprintln(w, "Anomalies are always printed; -verbose prints all spans including clean ones.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Flags:")
		flag.PrintDefaults()
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Examples:")
		fmt.Fprintln(w, "  iotest-verify -path /tmp/smoke.dat")
		fmt.Fprintln(w, "  iotest-verify -path /tmp/soak/iotest-soak-0.noOverlap -verbose")
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

	f, err := fileops.Open(*path, os.O_RDONLY, 0)
	if err != nil {
		die("open: %v", err)
	}
	defer f.Close()

	store, err := xattrstore.OpenStore(*path)
	if err != nil {
		die("OpenStore: %v", err)
	}

	checkXattrPresence(*path, store)

	anomalies := 0
	total := 0

	err = verifier.VerifyFile(store, f, verifier.Options{}, func(span verifier.Span) error {
		total++
		ok, line, detail := summarise(span)
		if !ok {
			anomalies++
		}
		if *verbose || !ok {
			fmt.Print(line)
			if detail != "" {
				fmt.Print(detail)
			}
		}
		return nil
	})
	if err != nil {
		die("verify: %v", err)
	}

	fmt.Printf("\n%s: %d span(s) checked", *path, total)
	if anomalies > 0 {
		fmt.Printf(", %d anomaly(s)\n", anomalies)
		fmt.Println("FAIL")
		os.Exit(1)
	}
	fmt.Println(", 0 anomalies")
	fmt.Println("PASS")
}

// checkXattrPresence exits with a clear message if the file is non-empty but
// has no iotest xattr records. This catches the common case of running on a
// filesystem where user xattrs are not enabled (e.g. BeeGFS without
// client_extra_mount_options = user_xattr), or a file copied without
// preserving xattrs.
func checkXattrPresence(path string, store *xattrstore.Store) {
	info, err := os.Stat(path)
	if err != nil || info.Size() == 0 {
		return
	}
	n := 0
	if err := store.ForEachEntry(func(_, _ int64, _ []byte) error {
		n++
		return nil
	}); err != nil {
		// A listxattr failure (permissions, the file vanishing mid-check, ...)
		// is a different problem than "genuinely zero xattr records" -- don't
		// let it get misdiagnosed as missing xattr support below.
		fmt.Fprintf(os.Stderr, "%s: listing xattrs: %v\n", path, err)
		os.Exit(1)
	}
	if n == 0 {
		fmt.Fprintf(os.Stderr, "%s: no iotest xattr records found on a non-empty file\n", path)
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Possible causes:")
		fmt.Fprintln(os.Stderr, "  - Filesystem does not support user xattrs")
		fmt.Fprintln(os.Stderr, "    BeeGFS: add 'client_extra_mount_options = user_xattr' to beegfs-client.conf")
		fmt.Fprintln(os.Stderr, "  - File was copied without preserving xattrs (use 'cp --preserve=xattr')")
		fmt.Fprintln(os.Stderr, "  - File was not written by verifyio")
		os.Exit(1)
	}
}

// summarise returns whether the span is clean, a one-line summary, and an
// optional second line with header details (for CoverageOne spans).
func summarise(s verifier.Span) (ok bool, line, detail string) {
	switch s.Coverage {
	case verifier.CoverageOne:
		clean := s.Verdict == block.VerdictOK
		status := "ok"
		if !clean {
			status = "FAIL"
		}
		line = fmt.Sprintf("offset=%-10d length=%-8d coverage=one      verdict=%-18s %s\n",
			s.Offset, s.Length, s.Verdict, status)
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
		return true, line, "" // contended is not an anomaly; writer is active

	default:
		line = fmt.Sprintf("offset=%-10d length=%-8d coverage=unknown(%d)\n",
			s.Offset, s.Length, int(s.Coverage))
		return false, line, ""
	}
}

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
