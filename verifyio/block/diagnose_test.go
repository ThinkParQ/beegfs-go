// This is a unit test.
//
// Coverage: Diagnose correctly distinguishes a coherent-but-stale cross-node
// read (stripe CRCs pass, StripesFailed=0, but the body doesn't match the
// header's seed -- BODY_CORRUPT) from genuine corruption (a body byte
// flipped without updating its stripe CRC -- BODY_CRC_MISMATCH,
// StripesFailed>=1); and every Verdict has a non-empty Explanation, with
// BODY_CORRUPT's explicitly naming the xattr/version distinction.
package block

import (
	"strings"
	"testing"
)

// TestDiagnoseStaleVsCorrupt verifies that Diagnose distinguishes a
// coherent-but-wrong-version block (the cross-node stale-read case: stripe CRCs
// pass, but the body differs from the header's seed) from genuine corruption (a
// body byte altered without updating its stripe CRC).
func TestDiagnoseStaleVsCorrupt(t *testing.T) {
	// BodyLen(4096) -- the default block, and deliberately NOT a multiple of
	// StripeSize: 4064 is eight stripes of which the last covers 480 bytes, so
	// Diagnose's last-stripe clamp runs. At 1024 it never did, and removing the
	// clamp left the whole suite green while panicking on every real block.
	const bodyLen = 4064

	// Two independent, internally consistent blocks with different seeds.
	bufOld := make([]byte, BlockDataSize(bodyLen))
	var hOld Header
	if err := MakeBlock(bufOld, &hOld, KindPRNG, 0x1111, bodyLen); err != nil {
		t.Fatalf("MakeBlock old: %v", err)
	}
	var hNew Header
	if err := MakeBlock(make([]byte, BlockDataSize(bodyLen)), &hNew, KindPRNG, 0x2222, bodyLen); err != nil {
		t.Fatalf("MakeBlock new: %v", err)
	}

	// Stale read: the data on disk is the coherent OLD block while the xattr
	// header has already advanced to the NEW version.
	if v, _ := VerifyBlock(bufOld, &hNew, nil); v != VerdictBodyCorrupt {
		t.Fatalf("stale scenario: got verdict %v, want BODY_CORRUPT", v)
	}
	d := Diagnose(bufOld, &hNew)
	if d.StripesTotal != NumStripes(bodyLen) {
		t.Errorf("StripesTotal = %d, want %d", d.StripesTotal, NumStripes(bodyLen))
	}
	if d.StripesFailed != 0 {
		t.Errorf("stale block should be internally consistent; StripesFailed = %d, want 0", d.StripesFailed)
	}
	if d.ReadBodyCRC != hOld.BodyCRC {
		t.Errorf("ReadBodyCRC = 0x%08x, want the OLD block's CRC 0x%08x", d.ReadBodyCRC, hOld.BodyCRC)
	}
	if d.ReadBodyCRC == hNew.BodyCRC {
		t.Errorf("ReadBodyCRC unexpectedly equals the (new) header CRC 0x%08x", hNew.BodyCRC)
	}
	if d.FirstDiffByte < 0 {
		t.Errorf("FirstDiffByte = %d, want a located divergence >= 0", d.FirstDiffByte)
	}

	// Genuine corruption: flip a body byte without fixing its stripe CRC.
	bufCorrupt := make([]byte, BlockDataSize(bodyLen))
	var hC Header
	if err := MakeBlock(bufCorrupt, &hC, KindPRNG, 0x3333, bodyLen); err != nil {
		t.Fatalf("MakeBlock corrupt: %v", err)
	}
	bufCorrupt[0] ^= 0xFF
	if v, _ := VerifyBlock(bufCorrupt, &hC, nil); v != VerdictBodyCRCMismatch {
		t.Fatalf("corruption scenario: got verdict %v, want BODY_CRC_MISMATCH", v)
	}
	dc := Diagnose(bufCorrupt, &hC)
	if dc.StripesFailed < 1 {
		t.Errorf("corrupt block should fail a stripe CRC; StripesFailed = %d, want >= 1", dc.StripesFailed)
	}
}

// TestVerdictExplanation checks every verdict has a non-empty explanation and
// that BODY_CORRUPT's names the xattr/version distinction (the reason it was
// added — so a reader knows it means "valid data, wrong version").
func TestVerdictExplanation(t *testing.T) {
	// Completeness now lives in TestEveryVerdictHasStringAndExplanation, which
	// ranges over AllVerdicts rather than a hand-written bound. This loop used
	// `v <= VerdictTruncated`, which stopped one short of VerdictHeadTruncated.
	e := VerdictBodyCorrupt.Explanation()
	for _, want := range []string{"xattr", "version"} {
		if !strings.Contains(e, want) {
			t.Errorf("BODY_CORRUPT explanation missing %q: %s", want, e)
		}
	}
}

// TestAllVerdictsIsComplete is the enforcement behind AllVerdicts's claim to be
// the single source of truth. It walks the Verdict iota space and fails if a
// declared verdict is missing from the list.
//
// A comment asking the next author to update the list does not hold; a test
// does. Without this, a new Verdict is simply absent from every tally that
// ranges over AllVerdicts, and nothing complains.
//
// "Declared" is detected via String(): every real verdict has a case, and the
// default arm returns the "verdict(N)" form. So the walk stops at the first value
// with no String() case, which is the first undeclared one.
func TestAllVerdictsIsComplete(t *testing.T) {
	inList := make(map[Verdict]bool, len(AllVerdicts()))
	for _, v := range AllVerdicts() {
		if inList[v] {
			t.Errorf("AllVerdicts lists %v (%d) more than once", v, int(v))
		}
		inList[v] = true
	}

	// Bounded so a bug here cannot spin forever; far above any plausible count.
	const sanityBound = 1000
	declared := 0
	for i := 0; i < sanityBound; i++ {
		v := Verdict(i)
		if strings.HasPrefix(v.String(), "verdict(") {
			break // first value with no String() case: end of the enum
		}
		declared++
		if !inList[v] {
			t.Errorf("Verdict %v (%d) is declared but missing from AllVerdicts() -- "+
				"add it there, or every consumer ranging over the list will tally it invisibly",
				v, int(v))
		}
	}
	if declared == 0 {
		t.Fatal("detected zero declared verdicts: the String()-based probe is broken, " +
			"so this test would silently pass regardless of AllVerdicts's contents")
	}
	if len(AllVerdicts()) != declared {
		t.Errorf("AllVerdicts() has %d entries but %d verdicts are declared",
			len(AllVerdicts()), declared)
	}
}

// TestEveryVerdictHasStringAndExplanation pins that each verdict in the canonical
// list carries real prose, not the fallback arm.
//
// The predecessor of this test looped `for v := VerdictOK; v <= VerdictTruncated`
// -- which silently stopped one short of VerdictHeadTruncated -- and only checked
// the string was non-EMPTY, so a verdict falling through to "unknown verdict."
// passed it. Both holes are closed: range over AllVerdicts (which the test above
// pins to the enum), and reject the default arms explicitly.
func TestEveryVerdictHasStringAndExplanation(t *testing.T) {
	for _, v := range AllVerdicts() {
		name := v.String()
		if strings.HasPrefix(name, "verdict(") {
			t.Errorf("verdict %d has no String() case", int(v))
		}
		exp := strings.TrimSpace(v.Explanation())
		if exp == "" {
			t.Errorf("%s has empty Explanation()", name)
		}
		if exp == "unknown verdict." {
			t.Errorf("%s falls through to the default Explanation() arm", name)
		}
	}
}
