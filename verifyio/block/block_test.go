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
	"hash/crc32"
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
				newCRC := crc32.Checksum(out[:86], castagnoli)
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
		{KindCountUp, 100, []byte{100, 101, 102, 103, 104}},
		{KindRepeat, 0x0807060504030201, []byte{1, 2, 3, 4, 5, 6, 7, 8, 1, 2}},
		// KindDecimal: seed % 512 = 5 -> "005 006 007 ..."
		{KindDecimal, 5, []byte{'0', '0', '5', ' ', '0', '0', '6', ' ', '0', '0', '7', ' '}},
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
				newCRC := crc32.Checksum(body, castagnoli)
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
		"ver=3",
		"kind=decimal",
		"time=2023-11-14T", // partial RFC3339 prefix
		"node=testhost",
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
