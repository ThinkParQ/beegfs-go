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
	const bodyLen = 1024 // two 512-byte stripes, like the real soak block

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
	for v := VerdictOK; v <= VerdictTruncated; v++ {
		if strings.TrimSpace(v.Explanation()) == "" {
			t.Errorf("verdict %v has empty Explanation()", v)
		}
	}
	e := VerdictBodyCorrupt.Explanation()
	for _, want := range []string{"xattr", "version"} {
		if !strings.Contains(e, want) {
			t.Errorf("BODY_CORRUPT explanation missing %q: %s", want, e)
		}
	}
}
