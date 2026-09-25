// This is a unit test.
//
// Coverage: VerifyFile's validation of Options.Range. Every invalid range used
// to be a silent no-op -- the clamp arithmetic collapsed the sweep and
// VerifyFile returned nil having emitted nothing, so a library caller received
// "no anomalies" for a region that was never read. Range{Offset: X, Length:
// math.MaxInt64} is the sharp case: it is the natural spelling of "from here to
// the end", and the sum wraps negative.
package verifier_test

import (
	"math"
	"os"
	"testing"

	"github.com/thinkparq/beegfs-go/verifyio/block"
	"github.com/thinkparq/beegfs-go/verifyio/fileops"
	"github.com/thinkparq/beegfs-go/verifyio/verifier"
	"github.com/thinkparq/beegfs-go/verifyio/xattrstore"
)

func TestVerifyFileRejectsInvalidRange(t *testing.T) {
	const bs = 1024
	path, store := testEnv(t)

	f, err := fileops.Open(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()
	w, err := xattrstore.NewWriter(xattrstore.WriterConfig{File: f, Store: store, WorkerID: 0, Kind: block.KindDecimal, BlockSize: bs, Locking: xattrstore.LockNone, Log: nil})
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	for _, off := range []int64{0, bs, 2 * bs} {
		if err := w.WriteBlock(off, fileops.IOTypeBuffered); err != nil {
			t.Fatalf("WriteBlock %d: %v", off, err)
		}
	}

	sweep := func(t *testing.T, opts verifier.Options) (int, error) {
		t.Helper()
		spans := 0
		err := verifier.VerifyFile(store, f, opts, func(verifier.Span) error {
			spans++
			return nil
		})
		return spans, err
	}

	for _, tc := range []struct {
		name string
		r    verifier.ByteRange
	}{
		{"length overflows int64", verifier.ByteRange{Offset: bs, Length: math.MaxInt64}},
		{"zero length", verifier.ByteRange{}},
		{"negative length", verifier.ByteRange{Offset: bs, Length: -bs}},
		{"negative offset", verifier.ByteRange{Offset: -1, Length: bs}},
		{"offset at int64 max", verifier.ByteRange{Offset: math.MaxInt64, Length: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spans, err := sweep(t, verifier.Options{Range: &tc.r})
			if err == nil {
				t.Errorf("VerifyFile accepted Range%+v and returned nil after %d span(s); "+
					"a caller reads that as \"no anomalies\" for a range it never verified", tc.r, spans)
			}
		})
	}

	// The two shapes that must keep working, so the validation cannot pass by
	// rejecting everything.
	t.Run("nil Range verifies the whole file", func(t *testing.T) {
		spans, err := sweep(t, verifier.Options{})
		if err != nil {
			t.Fatalf("VerifyFile: %v", err)
		}
		if spans == 0 {
			t.Error("nil Range emitted no spans")
		}
	})

	t.Run("a valid explicit range still sweeps", func(t *testing.T) {
		r := verifier.ByteRange{Offset: bs, Length: bs}
		spans, err := sweep(t, verifier.Options{Range: &r})
		if err != nil {
			t.Fatalf("VerifyFile: %v", err)
		}
		if spans == 0 {
			t.Error("a valid range emitted no spans")
		}
	})
}
