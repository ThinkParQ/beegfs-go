package cmdfmt

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

func ptr[T any](v T) *T { return &v }

func TestFormatSpace(t *testing.T) {
	tests := []struct {
		name  string
		bytes *uint64
		raw   bool
		want  string
	}{
		{"not reported", nil, false, NotAvailable},
		{"not reported raw", nil, true, NotAvailable},
		{"zero", ptr[uint64](0), false, "0.0B"},
		{"IEC prefix", ptr[uint64](1 << 30), false, "1.0GiB"},
		{"raw is the exact byte count", ptr[uint64](1 << 30), true, "1073741824"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			viper.Set(config.RawKey, tc.raw)
			t.Cleanup(func() { viper.Set(config.RawKey, false) })
			assert.Equal(t, tc.want, FormatSpace(tc.bytes))
		})
	}
}

func TestFormatSpaceUsed(t *testing.T) {
	tests := []struct {
		name        string
		total, free *uint64
		raw         bool
		want        string
	}{
		{"total not reported", nil, ptr[uint64](10), false, NotAvailable},
		{"free not reported", ptr[uint64](10), nil, false, NotAvailable},
		{"half used", ptr[uint64](2 << 30), ptr[uint64](1 << 30), false, "1.0GiB (50.00%)"},
		{"fully used", ptr[uint64](1 << 30), ptr[uint64](0), false, "1.0GiB (100.00%)"},
		{"none used", ptr[uint64](1 << 30), ptr[uint64](1 << 30), false, "0.0B (0.00%)"},
		{"raw is the exact byte count", ptr[uint64](2 << 30), ptr[uint64](1 << 30), true, "1073741824 (50.00%)"},
		// A zero total makes the percentage undefined; it must be omitted rather than print "NaN%".
		{"zero total omits the percentage", ptr[uint64](0), ptr[uint64](0), false, "0.0B"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			viper.Set(config.RawKey, tc.raw)
			t.Cleanup(func() { viper.Set(config.RawKey, false) })
			assert.Equal(t, tc.want, FormatSpaceUsed(tc.total, tc.free))
		})
	}
}

func TestFormatInodes(t *testing.T) {
	tests := []struct {
		name   string
		inodes *uint64
		raw    bool
		want   string
	}{
		{"not reported", nil, false, NotAvailable},
		{"SI prefix with no unit suffix", ptr[uint64](1_500_000), false, "1.5M"},
		{"raw is the exact count", ptr[uint64](1_500_000), true, "1500000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			viper.Set(config.RawKey, tc.raw)
			t.Cleanup(func() { viper.Set(config.RawKey, false) })
			assert.Equal(t, tc.want, FormatInodes(tc.inodes))
		})
	}
}

func TestFormatInodesUsed(t *testing.T) {
	tests := []struct {
		name        string
		total, free *uint64
		want        string
	}{
		{"total not reported", nil, ptr[uint64](10), NotAvailable},
		{"free not reported", ptr[uint64](10), nil, NotAvailable},
		{"half used", ptr[uint64](2_000_000), ptr[uint64](1_000_000), "1.0M (50.00%)"},
		// Seen on the metadata targets of a dev cluster where inodes are reported as zero.
		{"zero total omits the percentage", ptr[uint64](0), ptr[uint64](0), "0.0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, FormatInodesUsed(tc.total, tc.free))
		})
	}
}
