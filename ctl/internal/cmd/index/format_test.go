package index

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCeilDiv(t *testing.T) {
	tests := []struct {
		name    string
		n       int64
		divisor int64
		want    int64
	}{
		{"zero", 0, 1024, 0},
		{"one rounds up", 1, 1024, 1},
		{"exact multiple", 1024, 1024, 1},
		{"just over multiple", 1025, 1024, 2},
		// The n+divisor-1 form would overflow here; ceilDiv must not.
		{"MaxInt64 does not overflow", math.MaxInt64, 1024, 9007199254740992},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, ceilDiv(tc.n, tc.divisor))
		})
	}
}

func TestFmtBlockSize(t *testing.T) {
	tests := []struct {
		name, blocksStr, unit, want string
	}{
		{"1KiB in K", "2", "K", "1"},
		{"rounds up", "3", "K", "2"},
		{"non-numeric returns input", "abc", "K", "abc"},
		{"unknown unit returns input", "2", "X", "2"},
		{"negative returns input", "-1", "K", "-1"},
		// Above the guard, n*512 would overflow, so the raw string is returned.
		{"above guard returns input", "18014398509481984", "K", "18014398509481984"},
		// Largest n the guard admits: scaleBytes must not overflow rounding it.
		{"max admitted does not overflow", "18014398509481983", "K", "9007199254740992"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, fmtBlockSize(tc.blocksStr, tc.unit))
		})
	}
}

func TestFmtSizeWithBlockSize(t *testing.T) {
	tests := []struct {
		name, sizeStr, unit, want string
	}{
		{"rounds up to K", "1500", "K", "2"},
		{"exact K", "1024", "K", "1"},
		{"unknown unit returns input", "1500", "X", "1500"},
		{"non-numeric returns input", "x", "K", "x"},
		// Regression: a MaxInt64 size must not overflow to a negative figure.
		{"MaxInt64 does not overflow", strconv.FormatInt(math.MaxInt64, 10), "K", "9007199254740992"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, fmtSizeWithBlockSize(tc.sizeStr, tc.unit))
		})
	}
}
