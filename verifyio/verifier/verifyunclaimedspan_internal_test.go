// This is a unit test, internal to package verifier so it can call
// verifyUnclaimedSpan directly.
//
// Coverage: the contended arm of verifyUnclaimedSpan -- the gap scanner's
// answer when a lock kept it from reading the range at all.
//
// scanallzero_internal_test.go covers the same contention one layer DOWN, at
// scanAllZero, and verifier_test.go covers the visually near-identical arm in
// verifyOneRecordSpan. Neither reaches this one: no test in the tree drives a
// busy lock through verifyUnclaimedSpan, so every mutation of the arm below --
// including reporting the range as read and all-zero -- passed the whole
// module. That mutant is the worst defect this tool has: a clean verdict over
// bytes nothing ever looked at.
package verifier

import (
	"testing"
)

// TestVerifyUnclaimedSpanContendedIsNotAClean pins that a gap the scanner was
// locked out of is reported as skipped, not as verified.
//
// All three assertions are load-bearing and fail independently:
//
//   - Coverage catches the arm being deleted or its Coverage changed. Deleting
//     it does not fall through to a clean -- the recheck finds no records on a
//     sparse file, so the span lands as CoverageNone with AllZero false, a
//     false FAIL rather than a false PASS. Both are wrong verdicts.
//   - Contended catches the reason being swapped for ContendedStoreChanged,
//     which sends an operator after records moving under the read when the
//     truth is that someone held the range.
//   - AllZero catches the false clean directly. It is asserted separately
//     because Coverage and AllZero are independent fields: a mutant can return
//     CoverageNone with AllZero true and satisfy neither of the two above.
//
// Offset and Length are checked because a zero Span satisfies the AllZero
// assertion while telling the operator the skipped range was empty.
//
// A competing lease on the SAME Store is enough to contend, as in
// TestScanAllZeroLockModeSelectsTheChunkReader: the in-process range table
// refuses any overlap, so the nested acquire gets ErrLockBusy immediately
// rather than blocking. No helper process needed.
func TestVerifyUnclaimedSpanContendedIsNotAClean(t *testing.T) {
	const size = 2 * unclaimedScanChunk
	f, store := openSparse(t, size)

	lease, err := store.TryAcquireExclusive(f.LockFd(), 0, size)
	if err != nil {
		t.Skipf("range locking unavailable here: %v", err)
	}
	defer func() {
		if rerr := lease.Release(); rerr != nil {
			t.Errorf("release: %v", rerr)
		}
	}()

	span, err := verifyUnclaimedSpan(store, f, Options{}, 0, size)
	if err != nil {
		t.Fatalf("verifyUnclaimedSpan: %v", err)
	}
	if span.Coverage != CoverageContended {
		t.Errorf("Coverage = %v, want %v -- a range held under someone else's "+
			"lock was reported as a range this sweep judged", span.Coverage, CoverageContended)
	}
	if span.Contended != ContendedLockBusy {
		t.Errorf("Contended = %v, want %v", span.Contended, ContendedLockBusy)
	}
	if span.AllZero {
		t.Error("AllZero is true for a range the scan never read -- " +
			"this is the false clean the whole tool exists to prevent")
	}
	if span.Offset != 0 || span.Length != size {
		t.Errorf("span = [%d, +%d), want [0, +%d) -- the skipped range must be "+
			"reported as the range that was skipped", span.Offset, span.Length, int64(size))
	}
}
