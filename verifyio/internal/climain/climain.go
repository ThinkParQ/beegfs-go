// Package climain holds small helpers shared by verifyio's standalone
// cmd/iotest-* binaries: fatal-error exits, a no-args usage check, and IO
// trace logger setup.
//
// # The iotest exit-code vocabulary
//
// One number means one thing across every iotest-* tool, so an operator or a
// wrapper can branch on the status alone. This is the only place the whole
// table is written down; a tool spells only the codes it can produce and cites
// this doc rather than restating what they mean.
//
//	0  the tool ran and the answer is good
//	1  the tool ran and the answer is BAD -- corrupt or wrong data
//	2  the command line was wrong                            (FailUsage)
//	3  inconclusive: too little was measured to judge -- re-run
//	4  there was nothing to act on                           (FailNoData)
//	5  the tool could not do its job; the answer is unknown   (FailEnvironment)
//
// 2, 4 and 5 are reachable through Fail, which is why they have Failure values.
// 0, 1 and 3 are a tool's verdict about what it found, so they are spelled in
// the tool -- a shared helper has no verdict to report. 5 arrives either way:
// a failure in cmd/iotest-smoke and cmd/iotest-posixbench, and in
// cmd/iotest-verify also the printed verdict token ERROR.
//
// # Code 3 makes no claim about anomalies, in either direction
//
// This is the one number narrower than it looks, and its two users sit on
// opposite sides of it. cmd/iotest-verify reaches 3 only with ZERO anomalies:
// its classify tests anomalies first, so any anomaly is 1.
// cmd/iotest-posixbench reaches 3 with AT LEAST ONE, because an anomaly over a
// run that never finished may be data that was never written, which is not
// something to accuse the data of -- and it also reaches 3 on a signal, with
// none.
//
// Both are right for their tool and neither should be changed to match the
// other. What is shared, and all that is shared, is "inconclusive -- re-run":
// a wrapper that retries on 3 is correct against both, and a wrapper that
// infers anything about anomalies from 3 is wrong against one of them.
package climain

import (
	"flag"
	"fmt"
	"os"

	"github.com/thinkparq/beegfs-go/verifyio/trace"
)

// Failure says why a tool is giving up. Every value maps to an exit code that
// is deliberately not 1, so a tool failure cannot be reported as a data FAIL.
// The codes and what they mean are in the package doc above.
//
// That covers every failure routed THROUGH Fail, which is not the same as every
// failure: a caller can still exit on its own. Die used to be the easy way to
// do it -- it exited 1, was reachable from a verdict-bearing tool, and no
// package-scoped go/ast guard could see it, which is why it was deleted rather
// than guarded. ExitIfNoArgs below also exits, at 2; nothing in this package
// produces 1.
//
// What stops a caller exiting 1 inline is verifyio/internal/exitguard, which
// parses every tool directory under cmd/ instead of one package: no os.Exit
// inside an error-conditioned body, no os.Exit argument a tool's policy does
// not name, no log.Fatal* or syscall.Exit, and Fail itself banned in
// cmd/iotest-verify -- that tool reserves five codes and promises a summary
// line, so 2 or 4 from Fail is as wrong there as 1 would be. What still
// escapes is listed in exitguard's own doc; do not restate it here, or the two
// will drift.
type Failure int

const (
	// FailUsage means the command line was wrong: a missing or invalid flag, or
	// an argument naming something the tool will not act on. Nothing is broken
	// and the operator can fix it by retyping the command.
	FailUsage Failure = iota

	// FailNoData means there was nothing to act on: the target holds no
	// artifact this tool can work with. Distinct from FailEnvironment because
	// nothing failed -- the tool looked, correctly, and found nothing -- and
	// distinct from a data verdict because it never got as far as judging any.
	FailNoData

	// FailEnvironment means the tool could not do its job: a file it could not
	// open, a store it could not read, a syscall that failed. The answer is
	// unknown rather than bad.
	FailEnvironment
)

// exit codes for the Failure values, deliberately excluding 1
const (
	codeUsage       = 2
	codeNoData      = 4
	codeEnvironment = 5
)

func (f Failure) code() int {
	switch f {
	case FailUsage:
		return codeUsage
	case FailNoData:
		return codeNoData
	case FailEnvironment:
		return codeEnvironment
	default:
		// An unnamed Failure is a programming error in the caller, not a
		// runtime condition. Report it rather than guessing a category, but
		// still avoid 1: this must not be able to read as FAIL either.
		fmt.Fprintf(os.Stderr, "climain.Fail: unknown Failure value %d; treating as environment\n", int(f))
		return codeEnvironment
	}
}

// Fail prints a formatted error message to stderr and exits with the code for
// f: 2 for FailUsage, 4 for FailNoData, 5 for FailEnvironment.
//
// This calls os.Exit, so deferred functions do NOT run -- check anything that
// matters (a Close whose error is a finding) explicitly at the end rather than
// deferring it.
func Fail(f Failure, format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(f.code())
}

// ExitIfNoArgs prints the flag package's usage message and exits with status
// 2 if the process was invoked with no arguments at all. Call after setting
// a custom flag.Usage (if any) and before flag.Parse.
//
// The 2 here is the same code Fail(FailUsage, ...) produces, so a tool using
// both reports one usage code.
func ExitIfNoArgs() {
	if len(os.Args) == 1 {
		flag.Usage()
		os.Exit(codeUsage)
	}
}

// NewTraceLoggers builds trace.TraceLoggers from level/logFile (typically the
// -iotrace/-iologfile flag values).
//
// Returns the error rather than exiting: this is reached by a verdict-bearing
// tool, and a shared helper cannot know which exit code its caller reserves.
func NewTraceLoggers(level int, logFile string) (trace.TraceLoggers, error) {
	return trace.NewTraceLoggers(trace.TraceConfig{
		Level:   int8(level),
		LogFile: logFile,
	})
}
