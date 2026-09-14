// iotest-verify -- verify a data file written with an iotest tool.
//
// Each span of the file is classified by coverage and verified against its
// stored xattr header. Anomalies are always printed; use -verbose to print
// all spans including clean ones.
//
// This is the POST-RUN forensic tool: the workload has stopped and nothing else
// is writing, so contended is normally 0. Verification during a run is a
// different mode -- a verify thread inside the running tool, alongside its
// writers -- and it consumes verifier.Span directly rather than running this
// binary. Contention is expected there and misuse here.
//
//	iotest-verify -path /tmp/iotest.dat
//	iotest-verify -path /tmp/iotest.dat -verbose
//
// The last line of stdout is always a machine-readable summary with a fixed
// field set, so a wrapping script parses one line instead of grepping prose:
//
//	verdict=PASS records=12 gaps=3 contended=0 anomalies=0 path=/tmp/iotest.dat
//
// path is last because it may contain spaces; everything after "path=" is the
// path. The line is printed on every exit path below, including the ones that
// end the run before a sweep completes, so a caller never has to special-case
// its absence.
//
// Exit status is one code per verdict, so a wrapper can branch on the code
// alone and use the fields only for detail:
//
//	0  PASS        at least one record verified, no anomalies
//	1  FAIL        an anomaly, including a malformed xattr record name
//	2  usage
//	3  INCOMPLETE  no record verified because every record's span was contended
//	4  NO_DATA     no record verified and nothing was skipped -- nothing to verify
//	5  ERROR       the sweep did not complete
//
// The numbers are the shared iotest vocabulary; internal/climain's package doc
// owns that table. Worth knowing about 3: classify tests anomalies first, so
// INCOMPLETE here always means zero of them -- which is NOT true of
// iotest-posixbench's 3. climain's doc records why, and why neither should
// change to match the other.
//
// Adding an error path here? It has to end through dieError, never log.Fatal
// or a bare os.Exit(1) -- both are FAIL. dieError's doc carries the contract
// and names what still escapes the invariant tests.
//
// PASS means every record the sweep reached verified clean. Check contended=0
// to know it reached all of them: a sweep that verified some records and was
// locked out of others still passes, because for the intended post-run use
// there is no writer to lock it out. INCOMPLETE is the one verdict that says
// "ask again" -- a lock held the tool off the whole file, which here means it
// ran too early, not that anything is wrong.
//
// Coverage is counted in RECORDS, never in spans -- a gap span claims no
// record, so reading it proves nothing about written data and must not make a
// sweep that verified nothing look like a pass.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/internal/climain"
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
		fmt.Fprintln(w, "Verify a data file written with an iotest tool.")
		fmt.Fprintln(w, "Every byte range is classified by coverage and checked against its stored xattr header.")
		fmt.Fprintln(w, "Anomalies are always printed; -verbose prints all spans including clean ones.")
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Flags:")
		flag.PrintDefaults()
		fmt.Fprintln(w)
		fmt.Fprintln(w, "Examples:")
		fmt.Fprintln(w, "  iotest-verify -path /tmp/iotest.dat")
		fmt.Fprintln(w, "  iotest-verify -path /tmp/iotest.dat -verbose")
	}
	climain.ExitIfNoArgs()
	flag.Parse()

	if *path == "" {
		flag.Usage()
		os.Exit(exitUsage)
	}

	f, err := fileops.Open(*path, os.O_RDONLY, 0)
	if err != nil {
		dieError(*path, "open: %v", err)
	}
	defer f.Close()

	store, err := xattrstore.OpenStore(*path, xattrstore.DefaultLockTimeout)
	if err != nil {
		dieError(*path, "OpenStore: %v", err)
	}
	defer store.Close()

	checkXattrPresence(*path, f, store)

	var c counts

	err = verifier.VerifyFile(store, f, verifier.Options{}, func(span verifier.Span) error {
		ok, line, detail := countSpan(&c, span)
		if span.Coverage == verifier.CoverageContended {
			// Always printed, unverified or not: hiding a skipped span is how a
			// partial sweep comes to read as a whole one.
			fmt.Print(line)
			return nil
		}
		if *verbose || !ok {
			fmt.Print(line)
			if detail != "" {
				fmt.Print(detail)
			}
		}
		return nil
	})
	finding, fatal := classifySweepErr(err)
	if fatal {
		dieError(*path, "verify: %v", err)
	}
	if finding != nil {
		fmt.Fprintln(os.Stderr, finding.Error())
		c.anomalies += len(finding.Entries)
	}

	out, code := sweepVerdict(*path, c)
	fmt.Print(out)
	os.Exit(code)
}

// Verdict tokens, as they appear in the machine-readable summary line. NO_DATA
// carries an underscore because a space would break the line's key=value
// contract; the human-readable token above it still reads "NO DATA READ".
const (
	verdictPass       = "PASS"
	verdictFail       = "FAIL"
	verdictIncomplete = "INCOMPLETE"
	verdictNoData     = "NO_DATA"
	verdictError      = "ERROR"
)

// One exit code per verdict, so a wrapper can branch on the code alone. See the
// package doc for the contract these are half of.
const (
	exitPass       = 0
	exitFail       = 1
	exitUsage      = 2
	exitIncomplete = 3
	exitNoData     = 4
	exitError      = 5
)

// counts is what a sweep observed. records is COVERAGE -- spans a record
// claimed and the tool therefore verified. gaps is spans no record claimed:
// their bytes were read, but reading unclaimed bytes says nothing about written
// data, so a gap is never coverage.
type counts struct {
	records   int
	gaps      int
	contended int
	anomalies int
}

// countSpan folds one span into c and returns summarise's verdict on it, so the
// count and the printed line can never disagree about the same span.
//
// Which spans count as coverage is the whole point of this function. Only a
// span a record claims is coverage: a gap span's bytes were read, but they are
// bytes no record ever wrote, so reading them proves nothing about written
// data. Counting gaps as coverage made classify's INCOMPLETE arm unreachable --
// a single all-zero gap span was enough for a sweep that verified no record to
// print PASS at exit 0.
//
// Split out of main's callback so it is testable without a filesystem.
func countSpan(c *counts, span verifier.Span) (ok bool, line, detail string) {
	ok, line, detail = summarise(span)
	switch span.Coverage {
	case verifier.CoverageContended:
		// Never verified: a held lock skipped the body read entirely (see
		// verifier.CoverageContended). Not coverage, and not an anomaly either
		// -- being locked out is not a finding about the data.
		c.contended++
		return ok, line, detail
	case verifier.CoverageNone:
		c.gaps++
	case verifier.CoverageOne, verifier.CoverageMany:
		c.records++
	default:
		// Unknown coverage: not counted as coverage, and summarise reports it
		// not-ok, so it lands as an anomaly below.
	}
	if !ok {
		c.anomalies++
	}
	return ok, line, detail
}

// classify maps a completed sweep's counts to its verdict and exit code.
//
// The order is the priority order and each arm excludes the ones below it: an
// anomaly outranks everything (something is definitely wrong, a stronger
// statement than "some spans were skipped"), then any coverage at all is a
// pass, then a skipped span makes it retryable, and only a sweep that both
// verified nothing and skipped nothing is NO_DATA.
//
// "Any coverage at all is a pass" is deliberate, not a gap. Both contended arms
// describe a forensic sweep run while something was still writing, which is
// operator error the printed counts already report -- so PASS alongside a
// non-zero contended is not worth tightening the predicate for. The in-run
// verify mode, where contention IS the normal case, never reaches this
// function.
//
// Split out of the rendering so the arithmetic is testable without a
// filesystem, and so the code and the printed token cannot disagree.
func classify(c counts) (string, int) {
	switch {
	case c.anomalies > 0:
		return verdictFail, exitFail
	case c.records > 0:
		return verdictPass, exitPass
	case c.contended > 0:
		return verdictIncomplete, exitIncomplete
	default:
		return verdictNoData, exitNoData
	}
}

// classifySweepErr splits VerifyFile's error return into a finding about the
// data and a failure that stopped the sweep.
//
// A malformed record NAME is the only error of the first kind. VerifyFile emits
// every span it can and reports those afterwards, so by the time it returns the
// counts and the verdict are complete -- dying instead threw away a finished
// sweep's results over metadata the sweep had already worked around, on exactly
// the corrupt input this tool exists to diagnose.
//
// Every other error aborted the sweep, so no verdict about the data is
// possible and the run gets ERROR rather than sharing FAIL's exit code. Getting
// that split backwards is invisible in the output of a healthy run, which is
// why it is a function with a table behind it rather than an inline branch: a
// read error mid-sweep would otherwise report as corrupt data.
func classifySweepErr(err error) (finding *verifier.MalformedEntriesError, fatal bool) {
	if err == nil {
		return nil, false
	}
	var me *verifier.MalformedEntriesError
	if errors.As(err, &me) {
		return me, false
	}
	return nil, true
}

// summaryLine renders the machine-readable last line of stdout. The field set
// is fixed -- every field is printed on every exit path, present or zero -- so
// a parser needs no conditionals. path is last because it may contain spaces.
func summaryLine(path, verdict string, c counts) string {
	return fmt.Sprintf("verdict=%s records=%d gaps=%d contended=%d anomalies=%d path=%s\n",
		verdict, c.records, c.gaps, c.contended, c.anomalies, path)
}

// sweepVerdict renders the closing summary for a completed sweep and returns
// the process exit code.
func sweepVerdict(path string, c counts) (string, int) {
	verdict, code := classify(c)

	var b strings.Builder
	fmt.Fprintf(&b, "\n%s: %d record(s) verified", path, c.records)
	if c.gaps > 0 {
		fmt.Fprintf(&b, ", %d gap(s)", c.gaps)
	}
	if c.contended > 0 {
		fmt.Fprintf(&b, ", %d contended (unverified)", c.contended)
	}
	fmt.Fprintf(&b, ", %d anomaly(s)\n", c.anomalies)

	switch verdict {
	case verdictFail:
		b.WriteString("FAIL\n")
	case verdictIncomplete:
		b.WriteString("INCOMPLETE (no record verified; every record's span was contended)\n")
	case verdictNoData:
		b.WriteString("NO DATA READ (nothing to verify)\n")
	default:
		b.WriteString("PASS\n")
	}
	b.WriteString(summaryLine(path, verdict, c))
	return b.String(), code
}

// exitWithLine prints the machine-readable summary line and exits. Callers that
// have human-readable detail to give print it to stderr first.
//
// Every early exit goes through here rather than a bare os.Exit so that the
// summary line is genuinely the last line of stdout on every path. That output
// contract is why this tool keeps its own exit helper rather than calling
// climain.Fail: Fail writes a message to stderr and exits, and cannot emit the
// summary line a wrapper parses. Whether the code mapping moves onto climain is
// still open; the output half does not.
func exitWithLine(path, verdict string, code int, c counts) {
	fmt.Print(summaryLine(path, verdict, c))
	os.Exit(code)
}

// dieError reports a failure that stopped the sweep from completing: no verdict
// about the data is possible, which is a different thing from a verdict of bad
// data and now gets its own exit code instead of sharing FAIL's.
//
// Every failure exit in this file routes through here -- open, OpenStore, Size,
// listxattr, an aborted sweep -- so a target the tool could not read never
// reaches a wrapper as FAIL, which means corrupt data. checkXattrPresence is
// the sole exception and picks its own verdict deliberately.
//
// # The rule for anything added to this file
//
// End a failure through dieError. Never through log.Fatal* or syscall.Exit:
// both hardcode status 1, which this tool reserves for FAIL, "checked it, the
// data is corrupt". An error path written with one of them reports a file the
// tool could not READ as corrupt data, and skips the summary line a wrapping
// script parses.
//
// climain.Die was the third and likeliest spelling -- the idiom in the other
// iotest binaries, and already imported here for ExitIfNoArgs, so it was the
// easy wrong answer rather than a far-fetched one. It was deleted rather than
// guarded, and nothing in climain exits 1 now. Its replacement climain.Fail
// then inherited the same role: in-repo, already imported here, and producing 2
// or 4 -- which for a five-code tool is as wrong as 1. So Fail is banned in this
// package by name, not left to a reader. A cross-package exit is visible to a
// guard as an ordinary call expression, but only when it is spelled with the
// package's own name; see exitguard's escape list.
//
// # What the invariant tests catch
//
// Two layers, and neither subsumes the other.
// TestOnlyDieErrorAndCheckXattrPresenceExit and TestNoErrorPathPicksItsOwnVerdict
// are in this package because they know its vocabulary: they match a call to
// exitWithLine, and they are what says dieError is the one wrapper and
// checkXattrPresence the one exception. verifyio/internal/exitguard parses every
// tool directory under cmd/ and knows only about exiting: it keys on the SPELLING
// of a callee (os.Exit, log.Fatal*, syscall.Exit, climain.Fail) and on the
// spelling of an os.Exit argument against this tool's policy, which here allows
// exitUsage and the identifier code.
//
// What escapes both layers is listed once, in exitguard's package doc. Do not
// restate it here, and do not summarise it as a count: this comment claimed
// completeness twice as "covered by construction" and once as "two escape", and
// a round of review falsified all three.
//
// One escape belongs here because it is specific to this file: inside
// checkXattrPresence only, an error-conditioned exitWithLine whose condition the
// rule cannot read. Everywhere else a direct exitWithLine trips the first rule
// whatever the condition -- but checkXattrPresence is allowlisted there, so the
// second rule is all that covers its two ERROR arms.
func dieError(path, format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	exitWithLine(path, verdictError, exitError, counts{})
}

// checkXattrPresence exits with a clear message if the file is non-empty but
// has no iotest xattr records -- a file not written by an iotest tool, or one
// copied without preserving xattrs.
//
// It can also mean the filesystem has no user xattrs enabled. On BeeGFS the
// controls are sysXAttrsEnabled (beegfs-client.conf) and storeClientXAttrs
// (beegfs-meta.conf), NOT client_extra_mount_options, which this comment named
// until 2026-09-14. The causes below are deliberately unranked: which one is
// likely depends on the deployment, so the block names a value the operator can
// read instead of guessing for them.
//
// Strict, so a record whose NAME does not parse is reported as the corruption
// it is. ForEachEntry's plain form silently skips those, so a file carrying
// nothing but malformed verifyio records counted zero and was diagnosed as
// "filesystem does not support user xattrs" -- sending the operator to a mount
// option when the actual finding was corrupt metadata in verifyio's own
// namespace, on a filesystem that supports xattrs perfectly well.
//
// Sizes the file through the held fd rather than by path: the rest of this tool
// works from f, and a path re-stat answers about whatever the name resolves to
// now, which on the concurrent workloads these tools exist for need not be the
// file being verified.
func checkXattrPresence(path string, f *fileops.File, store *xattrstore.Store) {
	size, err := f.Size()
	if err != nil {
		// Returning silently here skipped the entire diagnosis this function
		// exists to produce, and the sweep then ran against a file nothing had
		// checked for xattr support at all.
		dieError(path, "%s: %v", path, err)
	}
	if size == 0 {
		return
	}
	n := 0
	malformed, err := store.ForEachEntryStrict(func(_, _ int64, _ []byte) error {
		n++
		return nil
	})
	// err first: on a listxattr failure the malformed slice is partial at best,
	// and reporting a short list of bad names would bury the real cause.
	if err != nil {
		// A listxattr failure (permissions, the file vanishing mid-check, ...)
		// is a different problem than "genuinely zero xattr records" -- don't
		// let it get misdiagnosed as missing xattr support below.
		dieError(path, "%s: listing xattrs: %v", path, err)
	}
	// Reported here ONLY when there is nothing else to go on. With parseable
	// records present the sweep still runs and VerifyFile returns
	// MalformedEntriesError, which names the same records -- printing them here
	// too put every offender on stderr twice.
	if len(malformed) > 0 && n == 0 {
		fmt.Fprintf(os.Stderr, "%s: %d malformed iotest xattr record(s):\n", path, len(malformed))
		for _, m := range malformed {
			fmt.Fprintf(os.Stderr, "  %s\n", m)
		}
		// Corruption in verifyio's own namespace, not a tool failure: the same
		// finding VerifyFile reports as MalformedEntriesError when there are
		// parseable records to sweep alongside it. Report it as the same
		// verdict either way.
		exitWithLine(path, verdictFail, exitFail, counts{anomalies: len(malformed)})
	}
	if n == 0 {
		fmt.Fprintf(os.Stderr, "%s: no iotest xattr records found on a non-empty file\n", path)
		fmt.Fprintln(os.Stderr, "")
		fmt.Fprintln(os.Stderr, "Possible causes:")
		fmt.Fprintln(os.Stderr, "  - File was not written by an iotest tool")
		fmt.Fprintln(os.Stderr, "  - File was copied without preserving xattrs (use 'cp --preserve=xattr')")
		fmt.Fprintln(os.Stderr, "  - Filesystem does not support user xattrs. On BeeGFS this needs")
		fmt.Fprintln(os.Stderr, "    sysXAttrsEnabled (beegfs-client.conf) and storeClientXAttrs")
		fmt.Fprintln(os.Stderr, "    (beegfs-meta.conf); the live client value is in")
		fmt.Fprintln(os.Stderr, "    /proc/fs/beegfs/<mount-id>/config.")
		exitWithLine(path, verdictError, exitError, counts{})
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
			detail = fmt.Sprintf("  kind=%-10s worker=%-4d cycle=%-6d node=%q\n",
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
		line = fmt.Sprintf("offset=%-10d length=%-8d coverage=contended (%s; skipped)\n",
			s.Offset, s.Length, s.Contended)
		// ok is unused by the caller for this case -- main handles
		// CoverageContended separately, before ok is even consulted, so it is
		// never folded into "checked" or "anomalies".
		return true, line, ""

	default:
		line = fmt.Sprintf("offset=%-10d length=%-8d coverage=unknown(%d)\n",
			s.Offset, s.Length, int(s.Coverage))
		return false, line, ""
	}
}
