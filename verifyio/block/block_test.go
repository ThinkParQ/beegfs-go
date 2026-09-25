// This is a unit test.
//
// Coverage: Header marshal/unmarshal round-trip (including zero-value and
// negative/max-value edge cases) and unmarshal error paths (truncated, bad
// magic, bad head CRC, a genuine self-consistent version mismatch, and --
// pinning the fix for a real bug found by review -- a corrupted version
// byte with a stale CRC correctly reporting CRC mismatch rather than being
// misclassified as benign version skew); GenerateBody determinism (same
// seed -> same bytes), seed-sensitivity per Kind, and pinned known-answer
// outputs; MakeBlock+VerifyBlock happy path for every Kind; VerifyBlock's
// verdict classification (OK, Truncated, BodyCorrupt, BodyCRCMismatch);
// Header.String formatting; and Kind<->string round-trip.
package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// roundTripHeader marshals h, unmarshals the result, and returns the
// parsed copy so tests can compare structurally.
func roundTripHeader(t *testing.T, h Header) Header {
	t.Helper()
	buf := make([]byte, HeaderSize)
	if err := MarshalHeader(buf, &h); err != nil {
		t.Fatalf("MarshalHeader: %v", err)
	}
	got, err := UnmarshalHeader(buf)
	if err != nil {
		t.Fatalf("UnmarshalHeader: unexpected error: %v", err)
	}
	return got
}

func TestHeaderRoundTrip(t *testing.T) {
	cases := []Header{
		{}, // all-zero header is fine: HeaderLen and CRC fields are filled by Marshal.
		{
			Version:  HeaderVersion,
			Kind:     KindPRNG,
			TimeNs:   1234567890,
			NodeName: NodeNameFromString("storage01"),
			TID:      4096,
			WorkerID: 7,
			Tag:      42,
			Cycle:    1 << 40,
			Offset:   0x1000,
			BodyLen:  4016,
			Seed:     0xcafebabe,
			BodyCRC:  0x12345678,
		},
		{
			Version:  HeaderVersion,
			Kind:     KindDecimal,
			TimeNs:   -1, // negative is allowed; verifies sign-extension
			TID:      -1,
			WorkerID: -1,
			Tag:      0xffffffff,
			Cycle:    ^uint64(0),
			Offset:   ^uint64(0),
			BodyLen:  0,
			Seed:     ^uint64(0),
			BodyCRC:  0xffffffff,
		},
	}

	for i, want := range cases {
		// MarshalHeader sets Version to HeaderVersion regardless of the input,
		// so normalize for comparison.
		want.Version = HeaderVersion
		got := roundTripHeader(t, want)
		if got != want {
			t.Errorf("case %d: round-trip mismatch\n got: %+v\nwant: %+v", i, got, want)
		}
	}
}

func TestUnmarshalErrors(t *testing.T) {
	good := make([]byte, HeaderSize)
	if err := MarshalHeader(good, &Header{Version: HeaderVersion, Kind: KindZeros}); err != nil {
		t.Fatalf("MarshalHeader setup: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func([]byte) []byte
		wantErr error
	}{
		{
			name:    "short buffer",
			mutate:  func(b []byte) []byte { return b[:HeaderSize-1] },
			wantErr: ErrTruncated,
		},
		{
			name: "bad magic",
			mutate: func(b []byte) []byte {
				out := make([]byte, len(b))
				copy(out, b)
				out[0] ^= 0xFF
				return out
			},
			wantErr: ErrBadMagic,
		},
		{
			name: "bad head crc",
			mutate: func(b []byte) []byte {
				out := make([]byte, len(b))
				copy(out, b)
				// Flip a byte in the BodyCRC field (offset 82); magic and
				// version still validate, so the failure mode is HeadCRC.
				out[82] ^= 0x01
				return out
			},
			wantErr: ErrBadHeadCRC,
		},
		{
			// A genuinely different, but internally self-consistent, on-wire
			// version: the CRC is recomputed over the mutated bytes, exactly
			// as a real differently-versioned writer's own CRC would be.
			// UnmarshalHeader checks CRC before Version specifically so this
			// case -- CRC passes, only Version disagrees -- is what actually
			// earns ErrBadVersion/VerdictHeadBadFormat's "CRC OK" claim.
			name: "bad version (self-consistent CRC)",
			mutate: func(b []byte) []byte {
				out := make([]byte, len(b))
				copy(out, b)
				binary.LittleEndian.PutUint16(out[8:10], 999)
				newCRC := crc32.Checksum(out[:86], crc32cTable)
				binary.LittleEndian.PutUint32(out[86:90], newCRC)
				return out
			},
			wantErr: ErrBadVersion,
		},
		{
			// Regression test: ordinary corruption landing on the Version
			// byte (CRC NOT recomputed, unlike the case above) must report
			// CRC mismatch, not be misclassified as benign version skew.
			name: "version byte corrupted (stale CRC)",
			mutate: func(b []byte) []byte {
				out := make([]byte, len(b))
				copy(out, b)
				binary.LittleEndian.PutUint16(out[8:10], 999)
				return out
			},
			wantErr: ErrBadHeadCRC,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := UnmarshalHeader(tc.mutate(good))
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("got err=%v, want errors.Is(_, %v)", err, tc.wantErr)
			}
		})
	}
}

func TestGenerateBodyDeterministic(t *testing.T) {
	const bodyLen = 256
	kinds := []Kind{KindPRNG, KindRepeat, KindCountUp, KindZeros, KindOnes, KindDecimal}
	for _, k := range kinds {
		t.Run(k.String(), func(t *testing.T) {
			a := make([]byte, bodyLen)
			b := make([]byte, bodyLen)
			if err := GenerateBody(k, 0xabcd1234, a); err != nil {
				t.Fatalf("GenerateBody(a): %v", err)
			}
			if err := GenerateBody(k, 0xabcd1234, b); err != nil {
				t.Fatalf("GenerateBody(b): %v", err)
			}
			if !bytes.Equal(a, b) {
				t.Errorf("two calls produced different bytes")
			}

			// Different seed should produce different bytes for variable
			// kinds (PRNG, Repeat, CountUp, Decimal). Constant kinds
			// (Zeros, Ones) are seed-independent.
			c := make([]byte, bodyLen)
			if err := GenerateBody(k, 0xabcd1235, c); err != nil {
				t.Fatalf("GenerateBody(c): %v", err)
			}
			isConstant := k == KindZeros || k == KindOnes
			eq := bytes.Equal(a, c)
			if isConstant && !eq {
				t.Errorf("constant kind %v: bytes changed with seed", k)
			}
			if !isConstant && eq {
				t.Errorf("variable kind %v: bytes did not change with seed", k)
			}
		})
	}
}

func TestGenerateBodyKnownAnswers(t *testing.T) {
	// Pin a few small known-output cases so accidental changes to body
	// generation are caught loudly.
	tests := []struct {
		kind Kind
		seed uint64
		want []byte
	}{
		{KindZeros, 12345, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{KindOnes, 0, []byte{0xFF, 0xFF, 0xFF, 0xFF}},
		// The two mixSeed kinds have no start an author can read off the seed,
		// so these expectations come from an independent implementation of the
		// splitmix64 finalizer rather than from this package's own output -- a
		// golden copied from the code under test pins nothing.
		// byte(mixSeed(100)) = 68; mixSeed(5) % 512 = 346.
		{KindCountUp, 100, []byte{68, 69, 70, 71, 72}},
		{KindRepeat, 0x0807060504030201, []byte{1, 2, 3, 4, 5, 6, 7, 8, 1, 2}},
		{KindDecimal, 5, []byte{'3', '4', '6', ' ', '3', '4', '7', ' ', '3', '4', '8', ' '}},
		// KindPRNG's vector is captured from this package rather than derived
		// independently, so unlike the two above it pins CHANGE, not correctness --
		// which is the property that matters here. PCG is specified and stable
		// across Go releases, so if these bytes move it is our derivation that
		// changed (the seed -> state-word mapping, or the fill order), and that
		// requires a HeaderVersion bump: without one, VerifyBlock regenerates
		// different data and every existing record reads as BODY_CORRUPT, i.e.
		// format skew wearing the stale-read signature. It was the only kind with
		// no vector at all, and the one the Kind doc tells operators to prefer.
		{KindPRNG, 0x0123456789abcdef, []byte{
			0x7c, 0x70, 0xe5, 0xb3, 0xe6, 0xcd, 0x09, 0xa3,
			0x16, 0x24, 0xe8, 0x37, 0x89, 0x87, 0xc0, 0xbd,
		}},
	}
	for _, tc := range tests {
		t.Run(tc.kind.String(), func(t *testing.T) {
			out := make([]byte, len(tc.want))
			if err := GenerateBody(tc.kind, tc.seed, out); err != nil {
				t.Fatalf("GenerateBody: %v", err)
			}
			if !bytes.Equal(out, tc.want) {
				t.Errorf("got %v, want %v", out, tc.want)
			}
		})
	}
}

// TestGenerateBodyHighSeedBitsAffectOutput pins that a value folded into a
// seed's HIGH bits reaches the generated body. posixbench's blockSeed XORs a
// per-file/worker index in at bit 32 specifically so a misdirected write is
// detectable, and a Kind reading only the low bits cannot see it: two seeds
// differing solely in bits >= 32 would produce byte-identical bodies, so
// overwriting one worker's data file with another's goes undetected in the
// default (decimal) mode.
func TestGenerateBodyHighSeedBitsAffectOutput(t *testing.T) {
	const bodyLen = 4096
	base := uint64(0x00000000_12345678)
	highBitsOnly := base ^ (uint64(1) << 32) // differs from base only at bit 32
	for _, kind := range []Kind{KindCountUp, KindDecimal, KindPRNG} {
		t.Run(kind.String(), func(t *testing.T) {
			a := make([]byte, bodyLen)
			b := make([]byte, bodyLen)
			if err := GenerateBody(kind, base, a); err != nil {
				t.Fatalf("GenerateBody(base): %v", err)
			}
			if err := GenerateBody(kind, highBitsOnly, b); err != nil {
				t.Fatalf("GenerateBody(highBitsOnly): %v", err)
			}
			if bytes.Equal(a, b) {
				t.Errorf("%s produced identical output for two seeds differing only "+
					"in bits >= 32 -- a value folded in at those bits (e.g. a worker "+
					"or file index) is invisible to this kind", kind)
			}
		})
	}
}

func TestMakeBlockAndVerifyOK(t *testing.T) {
	const bodyLen = 4016
	kinds := []Kind{KindPRNG, KindRepeat, KindCountUp, KindZeros, KindOnes, KindDecimal}
	for _, k := range kinds {
		t.Run(k.String(), func(t *testing.T) {
			buf := make([]byte, BlockDataSize(bodyLen))
			h := Header{
				NodeName: NodeNameFromString("n1"),
				TID:      2,
				WorkerID: 3,
				Tag:      0xa5,
				Cycle:    7,
				Offset:   0x1000,
				TimeNs:   1700000000,
			}
			if err := MakeBlock(buf, &h, k, 0xdeadbeef, bodyLen); err != nil {
				t.Fatalf("MakeBlock: %v", err)
			}
			scratch := make([]byte, bodyLen)
			v, err := VerifyBlock(buf, &h, scratch)
			if err != nil {
				t.Fatalf("VerifyBlock returned err: %v", err)
			}
			if v != VerdictOK {
				t.Errorf("verdict = %v, want OK", v)
			}
			if h.Kind != k || h.Seed != 0xdeadbeef || h.BodyLen != bodyLen {
				t.Errorf("populated header off: %+v", h)
			}
		})
	}
}

func TestVerifyBlockOutcomes(t *testing.T) {
	const bodyLen = 64 // fits in one 512-byte stripe
	build := func() ([]byte, Header) {
		buf := make([]byte, BlockDataSize(bodyLen))
		h := Header{}
		if err := MakeBlock(buf, &h, KindCountUp, 1, bodyLen); err != nil {
			t.Fatal(err)
		}
		return buf, h
	}

	tests := []struct {
		name   string
		mutate func([]byte) []byte
		want   Verdict
	}{
		{
			name:   "ok",
			mutate: func(b []byte) []byte { return b },
			want:   VerdictOK,
		},
		{
			name: "truncated body",
			mutate: func(b []byte) []byte {
				return b[:BlockDataSize(bodyLen)-1]
			},
			want: VerdictTruncated,
		},
		{
			name: "body corrupt (matching stripe crc, mismatch bytes)",
			// Replace body with an alternate generator output and recompute
			// the single stripe CRC so the seed-regeneration comparison catches it.
			mutate: func(b []byte) []byte {
				out := append([]byte(nil), b...)
				body := out[:bodyLen]
				if err := GenerateBody(KindOnes, 0, body); err != nil {
					t.Fatal(err)
				}
				newCRC := crc32.Checksum(body, crc32cTable)
				binary.LittleEndian.PutUint32(out[bodyLen:bodyLen+4], newCRC)
				return out
			},
			want: VerdictBodyCorrupt,
		},
		{
			name: "stripe crc mismatch",
			mutate: func(b []byte) []byte {
				out := append([]byte(nil), b...)
				out[0] ^= 0xFF // flip a body byte; stripe CRC now mismatches
				return out
			},
			want: VerdictBodyCRCMismatch,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf, h := build()
			got, _ := VerifyBlock(tc.mutate(buf), &h, nil)
			if got != tc.want {
				t.Errorf("got verdict=%v, want %v", got, tc.want)
			}
		})
	}
}

// TestHeaderStringQuotesNodeName pins that a NodeName read back from disk cannot
// forge output lines. The field is 15 raw bytes off disk and HeadCRC is a
// checksum, not a MAC, so anyone able to write the xattr can plant a newline and
// recompute the CRC. Header.String feeds iotest-dump and iotest-verify, and
// dump's exit status is always 0 -- its printed text IS the verdict -- so an
// unquoted name can emit a byte-exact span line and a line beginning "PASS:".
func TestHeaderStringQuotesNodeName(t *testing.T) {
	var forged [16]byte
	copy(forged[:], "x\nPASS: fine")
	got := Header{Version: HeaderVersion, Kind: KindDecimal, NodeName: forged}.String()

	if strings.Contains(got, "\n") {
		t.Errorf("String emitted a raw newline, so a planted node name can forge lines:\n%s", got)
	}
	if !strings.Contains(got, `\n`) {
		t.Errorf("String did not escape the newline; got: %s", got)
	}
}

func TestHeaderString(t *testing.T) {
	h := Header{
		Version:  HeaderVersion,
		Kind:     KindDecimal,
		TimeNs:   1700000000123456789,
		NodeName: NodeNameFromString("testhost"),
		TID:      4096,
		WorkerID: 7,
		Tag:      0x2a,
		Cycle:    42,
		Offset:   0x1000,
		BodyLen:  4016,
		Seed:     0xcafebabe,
		BodyCRC:  0x12345678,
	}
	got := h.String()
	// Spot-check key fields rather than pinning the entire format string,
	// so trivial whitespace changes don't break the test.
	wantSubstrings := []string{
		// Derived, not literal: this test pins the rendering, not the version
		// number, and a literal here just fails every time HeaderVersion moves.
		fmt.Sprintf("ver=%d", HeaderVersion),
		"kind=decimal",
		"time=2023-11-14T", // partial RFC3339 prefix
		`node="testhost"`,
		"tid=4096",
		"worker=7",
		"tag=0x0000002a",
		"cycle=42",
		"offset=0x1000",
		"bodyLen=4016",
		"seed=0x00000000cafebabe",
		"bodyCRC=0x12345678",
	}
	for _, sub := range wantSubstrings {
		if !strings.Contains(got, sub) {
			t.Errorf("Header.String() missing %q\n  got: %s", sub, got)
		}
	}
}

func TestMarshalHeaderShortBuf(t *testing.T) {
	short := make([]byte, HeaderSize-1)
	if err := MarshalHeader(short, &Header{Version: HeaderVersion}); err == nil {
		t.Errorf("MarshalHeader on undersized buf: expected error, got nil")
	}
}

func TestUnmarshalReturnsHeaderOnCRCFailure(t *testing.T) {
	// Build a valid header, corrupt one byte covered by the HeadCRC (Cycle
	// field, offset 50), then check UnmarshalHeader returns the partially-
	// parsed header for diagnostic use even though ErrBadHeadCRC is returned.
	h := Header{
		Version:  HeaderVersion,
		NodeName: NodeNameFromString("myhost"),
		WorkerID: 11,
		Cycle:    99,
		Offset:   0x4000,
	}
	buf := make([]byte, HeaderSize)
	if err := MarshalHeader(buf, &h); err != nil {
		t.Fatal(err)
	}
	buf[50] ^= 0x01 // mutate first byte of Cycle field (offset 50 in v3 layout)
	got, err := UnmarshalHeader(buf)
	if !errors.Is(err, ErrBadHeadCRC) {
		t.Fatalf("err=%v, want ErrBadHeadCRC", err)
	}
	// Header values should all be present, even though they're untrusted.
	if NodeNameString(got.NodeName) != "myhost" || got.WorkerID != 11 || got.Offset != 0x4000 {
		t.Errorf("populated header missing data on CRC failure: %+v", got)
	}
	// And the corrupted Cycle should reflect what was on the wire (not zero).
	if got.Cycle == 0 {
		t.Errorf("Cycle=0; expected on-wire value (mutated 99 ^ 1)")
	}
}

func TestKindFromStringRoundTrip(t *testing.T) {
	kinds := []Kind{KindPRNG, KindRepeat, KindCountUp, KindZeros, KindOnes, KindDecimal}
	for _, k := range kinds {
		got, err := KindFromString(k.String())
		if err != nil {
			t.Errorf("KindFromString(%q): %v", k.String(), err)
		}
		if got != k {
			t.Errorf("round-trip Kind: got %v want %v", got, k)
		}
	}
	if _, err := KindFromString("not-a-kind"); err == nil {
		t.Errorf("expected error for unknown kind")
	}
}

// TestVerifyBlockUnknownKindIsHeadBadFormat pins the verdict for a header whose
// Kind this build does not recognize.
//
// It must not be VerdictBodyCorrupt, whose Explanation() describes internally
// consistent data that does not match the version its header claims -- "the
// classic stale-read signature". That would report a writer/verifier format
// disagreement as evidence of a BeeGFS cache-coherence bug, sending an operator
// after the wrong problem. The verdict, not the error, is what callers print,
// and several discard the error -- so this belongs in VerifyBlock rather than in
// each call site.
func TestVerifyBlockUnknownKindIsHeadBadFormat(t *testing.T) {
	const bodyLen = 1024
	buf := make([]byte, BlockDataSize(bodyLen))

	// Build a block that is entirely valid apart from its Kind, so the stripe
	// CRCs pass and verification reaches the body-regeneration stage.
	var h Header
	if err := MakeBlock(buf, &h, KindDecimal, 42, bodyLen); err != nil {
		t.Fatalf("MakeBlock: %v", err)
	}
	// Sanity check: as written, it verifies clean.
	if v, err := VerifyBlock(buf, &h, nil); v != VerdictOK || err != nil {
		t.Fatalf("baseline: verdict=%v err=%v, want VerdictOK/nil", v, err)
	}

	h.Kind = Kind(9999) // not a value this build knows
	verdict, err := VerifyBlock(buf, &h, nil)
	if err == nil {
		t.Error("want a non-nil error naming the unrecognized kind")
	}
	if verdict != VerdictHeadBadFormat {
		t.Errorf("verdict = %v, want VerdictHeadBadFormat -- %v would point the "+
			"operator at cache coherence rather than a format mismatch", verdict, verdict)
	}
	if exp := verdict.Explanation(); !strings.Contains(exp, "format mismatch") {
		t.Errorf("explanation = %q, want it to name a format mismatch", exp)
	}
}

// TestBodyLenValidSizesAndAdvice pins which block sizes are actually accepted,
// and that the rejection message does not repeat advice that just failed.
//
// The docs, three flag help strings and the error itself all said "use a power
// of two". 65536 is the one power of two in normal use that does NOT work -- the
// total jumps from 65532 (127 stripes) to 65540 (128) without landing on it --
// so a caller reaching for 64 KiB, an entirely natural BeeGFS block size, was
// rejected and told to do the thing they had just done.
func TestBodyLenValidSizesAndAdvice(t *testing.T) {
	// Every power of two in the plausible range except the known exception.
	for bs := 512; bs <= 1<<26; bs *= 2 {
		_, err := BodyLen(bs)
		if bs == 65536 {
			if err == nil {
				t.Errorf("BodyLen(65536) unexpectedly succeeded; the doc claims it cannot")
			}
			continue
		}
		if err != nil {
			t.Errorf("BodyLen(%d): %v, want success", bs, err)
		}
	}

	// A multiple of StripeSize+4 must always work -- that is the rule the docs
	// and the error message now promise.
	for k := 1; k <= 200; k++ {
		bs := k * (StripeSize + 4)
		if _, err := BodyLen(bs); err != nil {
			t.Fatalf("BodyLen(%d) (=%d * %d): %v, want success", bs, k, StripeSize+4, err)
		}
	}

	// The rejection message must not recommend a power of two, and the sizes it
	// suggests must themselves be valid -- advice that does not work is worse
	// than no advice.
	_, err := BodyLen(65536)
	if err == nil {
		t.Fatal("BodyLen(65536): want an error")
	}
	msg := err.Error()
	if strings.Contains(msg, "power of two") {
		t.Errorf("error still recommends a power of two: %q", msg)
	}
	for _, suggested := range []int{65532, 66048} {
		if !strings.Contains(msg, strconv.Itoa(suggested)) {
			t.Errorf("error does not offer %d as a nearby valid size: %q", suggested, msg)
		}
		if _, err := BodyLen(suggested); err != nil {
			t.Errorf("suggested size %d is itself invalid: %v", suggested, err)
		}
	}
}

// TestVerdictHeadTruncatedIsDistinct pins that a truncated xattr HEADER and a
// truncated DATA block are separate verdicts with separate explanations.
//
// Sharing VerdictTruncated would misdirect: its explanation is entirely about
// the data -- "the on-disk block is shorter than its header claims: an
// incomplete write or a truncated file" -- which for a short header points at
// the wrong half of the store. The metadata is incomplete; the data may be
// perfect.
func TestVerdictHeadTruncatedIsDistinct(t *testing.T) {
	if VerdictHeadTruncated == VerdictTruncated {
		t.Fatal("the two truncation verdicts must not be the same value")
	}
	if got := VerdictHeadTruncated.String(); got != "HEAD_TRUNCATED" {
		t.Errorf("String() = %q, want HEAD_TRUNCATED", got)
	}

	head := VerdictHeadTruncated.Explanation()
	data := VerdictTruncated.Explanation()
	if head == data {
		t.Error("the two verdicts share an explanation; that is the bug")
	}
	// The header verdict must talk about the header, and must not claim the
	// on-disk block is short -- that is what sent readers to the wrong place.
	if !strings.Contains(head, "xattr header") {
		t.Errorf("header explanation does not mention the xattr header: %q", head)
	}
	if strings.Contains(head, "on-disk block is shorter") {
		t.Errorf("header explanation still describes the data block: %q", head)
	}
	if got := VerdictHeadTruncated.Explanation(); got == "unknown verdict." {
		t.Error("VerdictHeadTruncated has no explanation; it was not added to Explanation()")
	}
}

// TestHeaderImpliedSize pins the rule both verifier.verifyOneRecordSpan and
// iotest-dump use to decide that a record contradicts its own header.
//
// It went in without one: the check was added to iotest-dump precisely because
// dump printed verdict=OK for a record the verifier reported as
// SIZE_MISMATCH, and both copies of the arithmetic landed unpinned. They are
// one function now, so this test covers both call sites.
func TestHeaderImpliedSize(t *testing.T) {
	for _, bodyLen := range []int{0, 1, StripeSize - 1, StripeSize, StripeSize + 1, 4016, 4096} {
		h := Header{BodyLen: uint64(bodyLen)}
		if got, want := HeaderImpliedSize(&h), int64(BlockDataSize(bodyLen)); got != want {
			t.Errorf("bodyLen=%d: HeaderImpliedSize = %d, want %d", bodyLen, got, want)
		}
	}
}

// TestHeaderImpliedSizeOversizedBodyLen covers the untrusted end of the range. A
// header's CRC32C is a checksum, not a MAC, so any BodyLen can arrive with a valid
// header CRC, and MaxBodyLen is what stops one being converted rather than
// rejected. Removing that check from HeaderImpliedSize fails this test.
func TestHeaderImpliedSizeOversizedBodyLen(t *testing.T) {
	// Anything a real body cannot be must be rejected rather than arithmetically
	// converted. With the MaxBodyLen check removed, HeaderImpliedSize converts
	// every one of these and returns a non-negative size.
	for _, bad := range []uint64{
		MaxBodyLen + 1,
		math.MaxInt32 - 511, // first value of the band a MaxInt32 ceiling admitted
		math.MaxInt32 - 1,
		math.MaxInt32,
		math.MaxInt32 + 1,
		1 << 35,
		math.MaxUint64,
	} {
		h := Header{BodyLen: bad}
		if got := HeaderImpliedSize(&h); got >= 0 {
			t.Errorf("BodyLen=%d returned a non-negative size (%d); it should be rejected", bad, got)
		}
	}

	// The ceiling must not reject anything real: a body just under it still
	// converts, and posixbench's largest legal block is smaller still.
	for _, ok := range []uint64{MaxBodyLen, MaxBodyLen - 512, 4064} {
		h := Header{BodyLen: ok}
		if got := HeaderImpliedSize(&h); got <= 0 {
			t.Errorf("BodyLen=%d was rejected (%d) but is a legitimate body length", ok, got)
		}
	}
}

// TestBodyLenSuggestionsAreThemselvesValid pins that BodyLen's error names sizes
// that actually work.
//
// The hint is computed, not fixed, and for any blockSize below StripeSize+4 the
// lower suggestion can evaluate to BlockDataSize(0) == 0 -- which BodyLen
// rejects at its first line, so an operator following the advice gets a second
// error from the advice itself.
func TestBodyLenSuggestionsAreThemselvesValid(t *testing.T) {
	re := regexp.MustCompile(`nearest valid: (\d+) or (\d+)`)
	// Small sizes are the ones whose lower suggestion can reach zero; the larger
	// ones keep the ordinary path covered.
	for _, blockSize := range []int{1, 4, 100, 515, 517, 1000, 4096, 65536} {
		if _, err := BodyLen(blockSize); err == nil {
			continue // a valid size has no hint to check
		} else {
			m := re.FindStringSubmatch(err.Error())
			if m == nil {
				t.Errorf("blockSize=%d: error carries no suggestions: %v", blockSize, err)
				continue
			}
			for _, s := range m[1:] {
				n, convErr := strconv.Atoi(s)
				if convErr != nil {
					t.Fatalf("blockSize=%d: unparseable suggestion %q", blockSize, s)
				}
				if _, err := BodyLen(n); err != nil {
					t.Errorf("blockSize=%d suggested %d, which BodyLen itself rejects: %v",
						blockSize, n, err)
				}
			}
		}
	}
}

// goldenHeader is a hand-checked 90-byte on-disk header carrying its version as
// a LITERAL byte rather than as the symbol.
//
// That literal is the point: every other test reference goes through
// HeaderVersion, so mutating the constant moves both sides of every assertion
// together and the suite stays green. Fields, offsets and the CRC are pinned
// alongside it, so this also fails on a silent wire-layout change.
var goldenHeader = []byte{
	0x42, 0x41, 0x44, 0x47, 0x45, 0x52, 0x49, 0x4f, // magic "BADGERIO"
	0x04, 0x00, // Version = 4  <- the literal this test exists for
	0x03, 0x00, 0x00, 0x00, // Kind = KindCountUp (3)
	0x00, 0x00, 0x2a, 0x36, 0xfe, 0x9c, 0x97, 0x17, // TimeNs = 1700000000000000000
	0x67, 0x6f, 0x6c, 0x64, 0x65, 0x6e, 0x2d, 0x6e, // NodeName = "golden-node"
	0x6f, 0x64, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x92, 0x10, 0x00, 0x00, // TID = 4242
	0x07, 0x00, 0x00, 0x00, // WorkerID = 7
	0x03, 0x01, 0x00, 0x00, // Tag = 0x00000103
	0x63, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Cycle = 99
	0x00, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Offset = 0x1000
	0xe0, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // BodyLen = 4064
	0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x01, // Seed = 0x0123456789abcdef
	0xef, 0xbe, 0xad, 0xde, // BodyCRC = 0xdeadbeef
	0xbb, 0xb9, 0xfb, 0x5e, // HeadCRC over [0..86)
}

// TestGoldenHeader pins the on-disk header format against its literal version,
// so bumping HeaderVersion without intending a format change fails loudly here
// instead of silently everywhere else.
//
// A deliberate bump is meant to break this test: replace the vector with one
// captured from the new writer, and keep the old vector as an additional case
// asserting the new build REJECTS it with ErrBadVersion.
func TestGoldenHeader(t *testing.T) {
	if HeaderVersion != 4 {
		t.Fatalf("HeaderVersion is %d, but this golden vector describes version 4.\n"+
			"If the bump is deliberate: capture a new vector from the current writer, and add a "+
			"case asserting this one now fails with ErrBadVersion. If it is not, revert it -- "+
			"a version bump reclassifies every record written by the old build.", HeaderVersion)
	}
	if len(goldenHeader) != HeaderSize {
		t.Fatalf("golden vector is %d bytes, HeaderSize is %d", len(goldenHeader), HeaderSize)
	}

	want := Header{
		Version: 4, Kind: KindCountUp, TimeNs: 1700000000000000000,
		NodeName: NodeNameFromString("golden-node"),
		TID:      4242, WorkerID: 7, Tag: 0x00000103,
		Cycle: 99, Offset: 0x1000, BodyLen: 4064,
		Seed: 0x0123456789abcdef, BodyCRC: 0xdeadbeef,
	}

	// The reader accepts the bytes and recovers every field.
	got, err := UnmarshalHeader(goldenHeader)
	if err != nil {
		t.Fatalf("UnmarshalHeader on the golden vector: %v", err)
	}
	if got != want {
		t.Errorf("golden vector decoded to a different header:\n got %+v\nwant %+v", got, want)
	}

	// And the writer reproduces them byte for byte, which is what pins the
	// field offsets and the CRC rather than merely the version.
	out := make([]byte, HeaderSize)
	if err := MarshalHeader(out, &want); err != nil {
		t.Fatalf("MarshalHeader: %v", err)
	}
	if !bytes.Equal(out, goldenHeader) {
		t.Errorf("writer no longer reproduces the golden vector:\n got % x\nwant % x", out, goldenHeader)
	}
}

// TestBodyLenAgreesWithHeaderImpliedSize pins that the writer side and the
// reader side share one notion of a legal block.
//
// BodyLen sizes every block written in this tree; HeaderImpliedSize decides
// whether a record's stored size is convertible at all, returning -1 past
// MaxBodyLen. Both must enforce the same ceiling. If BodyLen hands out a body
// past it -- BodyLen(1082130948) would give 1073742336, 512 bytes over --
// RecordSelfCheck answers SIZE_MISMATCH for a block written exactly as asked:
// a FAIL reached before a single body byte is read, unreachable by any amount of
// correct writing.
func TestBodyLenAgreesWithHeaderImpliedSize(t *testing.T) {
	// The largest blockSize BodyLen accepts, and the band just past it where the
	// two sides could disagree.
	maxBlockSize := BlockDataSize(MaxBodyLen)
	for _, blockSize := range []int{
		4096, 65540, 1 << 20, 1 << 30,
		maxBlockSize,
		maxBlockSize + 516, // == 1082130948, just past the ceiling
	} {
		bodyLen, err := BodyLen(blockSize)
		if err != nil {
			continue // refused up front, so no record can carry it
		}
		if bodyLen > MaxBodyLen {
			t.Errorf("blockSize=%d: BodyLen returned %d, over MaxBodyLen (%d)",
				blockSize, bodyLen, MaxBodyLen)
			continue
		}
		h := Header{BodyLen: uint64(bodyLen), Offset: 0}
		if got := HeaderImpliedSize(&h); got != int64(blockSize) {
			t.Errorf("blockSize=%d: BodyLen accepted it (body %d) but HeaderImpliedSize says %d",
				blockSize, bodyLen, got)
		}
		if v := RecordSelfCheck(&h, 0, int64(blockSize)); v != VerdictOK {
			t.Errorf("blockSize=%d: correctly-written block self-checks as %v", blockSize, v)
		}
	}
}

// TestMakeBlockEnforcesMaxBodyLen pins the ceiling at the writer entry point, not
// just in BodyLen.
//
// BodyLen refusing an oversized blockSize only helps callers that route through
// it. MakeBlock is what stamps h.BodyLen, so a caller sizing its own blocks could
// get a block whose header HeaderImpliedSize rejects: RecordSelfCheck then answers
// SIZE_MISMATCH for a block written exactly as asked, and that verdict's
// explanation blames the xattr name -- pointing at metadata that is correct.
// Removing the check from MakeBlock fails this test.
func TestMakeBlockEnforcesMaxBodyLen(t *testing.T) {
	// Subtests, because the overflow case panics without the fix and would
	// otherwise mask whether the others discriminate.

	t.Run("oversized is refused by the ceiling, not by the buffer check", func(t *testing.T) {
		// Just past the ceiling, so the arithmetic stays ordinary and only the
		// bound decides. A 1 GiB buffer is never allocated: the ceiling returns
		// first -- which is also why asserting err != nil alone would prove
		// nothing here. Without the ceiling this call still fails, as "buf too
		// small", so the message is what separates the two reasons.
		const oversized = MaxBodyLen + 512
		var h Header
		err := MakeBlock(nil, &h, KindZeros, 7, oversized)
		if err == nil {
			t.Fatalf("MakeBlock accepted bodyLen %d, over MaxBodyLen (%d); its header would "+
				"self-check as %v", oversized, MaxBodyLen,
				RecordSelfCheck(&h, 0, int64(BlockDataSize(oversized))))
		}
		if !strings.Contains(err.Error(), "MaxBodyLen") {
			t.Errorf("bodyLen %d was rejected for the wrong reason (%v); the ceiling must "+
				"reject it before the buffer check, or a caller with a big enough buffer "+
				"gets a block whose header HeaderImpliedSize refuses", oversized, err)
		}
	})

	t.Run("the largest legal body still works", func(t *testing.T) {
		// The bound must not reject anything real, or every writer at the ceiling
		// breaks.
		var h Header
		buf := make([]byte, BlockDataSize(StripeSize))
		if err := MakeBlock(buf, &h, KindZeros, 7, StripeSize); err != nil {
			t.Fatalf("MakeBlock rejected a legitimate body: %v", err)
		}
		if got := HeaderImpliedSize(&h); got != int64(len(buf)) {
			t.Errorf("HeaderImpliedSize = %d, want %d", got, len(buf))
		}
		if v := RecordSelfCheck(&h, 0, int64(len(buf))); v != VerdictOK {
			t.Errorf("a block written exactly as asked self-checks as %v", v)
		}
	})

	t.Run("the overflow band errors instead of panicking", func(t *testing.T) {
		// BlockDataSize(bodyLen) wraps NEGATIVE here, so "len(buf) < needed" is
		// false and the body slice panics. The ceiling precedes that arithmetic.
		const overflows = 9151873028817141884 // 2^63 * 128/129
		var h Header
		if err := MakeBlock(make([]byte, 4096), &h, KindZeros, 1, overflows); err == nil {
			t.Errorf("MakeBlock accepted bodyLen %d, whose BlockDataSize overflows to %d",
				uint64(overflows), BlockDataSize(overflows))
		}
	})
}

// TestVerifyAndDiagnoseSurviveAbsurdBodyLen pins the two anti-panic guards that
// stand between a corrupt BodyLen and a slice panic.
//
// A header's CRC is a checksum, not a MAC, so BodyLen arrives untrusted; and
// int(h.BodyLen) for a value >= 2^63 is NEGATIVE, which makes BlockDataSize
// negative too, so the "buffer too short" comparisons below the guards do not
// fire. Both guards were previously executed only for values where the following
// check returned the same verdict, so removing either left the suite green while
// panicking on this input -- in Diagnose's case, in the diagnostic path itself.
func TestVerifyAndDiagnoseSurviveAbsurdBodyLen(t *testing.T) {
	const bodyLen = 4064
	buf := make([]byte, BlockDataSize(bodyLen))
	var h Header
	if err := MakeBlock(buf, &h, KindPRNG, 0x99, bodyLen); err != nil {
		t.Fatalf("MakeBlock: %v", err)
	}

	for _, bad := range []uint64{math.MaxUint64, 1 << 63, math.MaxInt64} {
		t.Run(fmt.Sprintf("BodyLen=%d", bad), func(t *testing.T) {
			hBad := h
			hBad.BodyLen = bad
			v, err := VerifyBlock(buf, &hBad, nil)
			if v != VerdictTruncated || err != nil {
				t.Errorf("VerifyBlock = (%v, %v), want (TRUNCATED, nil)", v, err)
			}
			d := Diagnose(buf, &hBad)
			if d.FirstDiffByte != -1 || d.StripesTotal != 0 {
				t.Errorf("Diagnose = %+v, want the zero diagnosis with FirstDiffByte=-1", d)
			}
		})
	}
}

// TestVerdictForHeaderErrorMapsEveryArm pins every arm of
// VerdictForHeaderError, and exists for the default one.
//
// The default arm is the ErrBadHeadCRC arm, and it is where real bit-rot lands:
// corrupting any byte the HeadCRC covers, without recomputing it, arrives here
// rather than at one of the three named sentinels. Flipping it to VerdictOK is a
// pure false PASS and was measured surviving every gate -- 500 corrupt headers
// out of 2000 reported PASS at exit 0 -- because nothing anywhere named
// VerdictHeadBadCRC. Both callers depend on this one line
// (verifier.verifyOneRecordSpan and cmd/iotest-smoke), so it decided two tools'
// verdicts untested.
//
// Exact wants and no companion "never VerdictOK" assertion: the equality is
// sufficient on its own, and a second, weaker check is the one a later reader
// ends up trusting.
func TestVerdictForHeaderErrorMapsEveryArm(t *testing.T) {
	// A real UnmarshalHeader failure rather than the bare sentinel, so this also
	// pins that a genuinely corrupt header reaches the default arm instead of
	// one of the named ones. Same recipe as TestUnmarshalReturnsHeaderOnCRCFailure:
	// byte 50 is in the Cycle field, which the HeadCRC covers.
	h := Header{
		Version:  HeaderVersion,
		NodeName: NodeNameFromString("myhost"),
		WorkerID: 11,
		Cycle:    99,
		Offset:   0x4000,
	}
	buf := make([]byte, HeaderSize)
	if err := MarshalHeader(buf, &h); err != nil {
		t.Fatal(err)
	}
	buf[50] ^= 0x01
	_, realCRCErr := UnmarshalHeader(buf)
	if !errors.Is(realCRCErr, ErrBadHeadCRC) {
		t.Fatalf("setup: UnmarshalHeader err=%v, want ErrBadHeadCRC", realCRCErr)
	}

	for _, tc := range []struct {
		name string
		err  error
		want Verdict
	}{
		{"bad magic", ErrBadMagic, VerdictHeadBadMagic},
		{"bad version", ErrBadVersion, VerdictHeadBadFormat},
		{"truncated header xattr", ErrTruncated, VerdictHeadTruncated},
		// Wrapped, because both callers hand this function an error that came up
		// through UnmarshalHeader rather than a bare sentinel -- so the arms have
		// to match with errors.Is and not ==.
		{"wrapped bad magic", fmt.Errorf("record 7: %w", ErrBadMagic), VerdictHeadBadMagic},
		{"real CRC failure from UnmarshalHeader", realCRCErr, VerdictHeadBadCRC},
		// The default arm's other two inputs. An unrecognised error is a header
		// this package could not vouch for; nil is a caller that forgot to check.
		// The doc promises neither reads as a clean record.
		{"unrecognised error", errors.New("something else"), VerdictHeadBadCRC},
		{"nil is a caller bug, not a clean record", nil, VerdictHeadBadCRC},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := VerdictForHeaderError(tc.err); got != tc.want {
				t.Errorf("VerdictForHeaderError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
