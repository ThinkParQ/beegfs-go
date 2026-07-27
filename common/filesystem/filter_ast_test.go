package filesystem

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFieldTableMatchesTags is the drift guard for the two sources of truth that must agree:
// FileInfo's expr struct tags (DSL name → Go field) and dslFields (DSL name → required fetch).
// Adding a field to one but not the other silently breaks either resolution or needs-detection,
// so this test fails loudly the moment they diverge.
func TestFieldTableMatchesTags(t *testing.T) {
	tags := map[string]struct{}{}
	rt := reflect.TypeOf(FileInfo{})
	for i := 0; i < rt.NumField(); i++ {
		tag := rt.Field(i).Tag.Get("expr")
		require.NotEmptyf(t, tag, "FileInfo.%s is missing an expr tag", rt.Field(i).Name)
		require.Equalf(t, strings.ToLower(tag), tag, "expr tag %q must be lowercase (DSL names are case-folded to lowercase)", tag)
		tags[tag] = struct{}{}
	}

	for name := range tags {
		_, ok := dslFields[name]
		assert.Truef(t, ok, "field %q has an expr tag but no dslFields entry (needs-detection would miss it)", name)
	}
	for name := range dslFields {
		_, ok := tags[name]
		assert.Truef(t, ok, "dslFields has %q but no FileInfo field carries that expr tag (it would never resolve)", name)
	}
}

// TestFilterCaseInsensitiveFields locks in the backwards-compatible behavior that field names
// are case-insensitive. Before the AST refactor a case-insensitive regex lowercased identifiers;
// now fieldVisitor does it structurally. Uppercase/mixed-case field names must still evaluate and
// still drive needs-detection identically to their lowercase spelling.
func TestFilterCaseInsensitiveFields(t *testing.T) {
	fi := FileInfo{
		Name:             "report.txt",
		Size:             500,
		Mode:             0o100644, // regular file
		Uid:              0,
		Mtime:            time.Now().Add(-2 * time.Hour),
		Offloaded:        true,
		AllocatedTargets: []int{5, 9},
	}

	cases := []struct {
		name       string
		query      string
		wantKeep   bool
		wantStat   bool
		wantEntry  bool
		wantDetail bool
	}{
		{"upper stat field", `SIZE > 100`, true, true, false, false},
		{"mixed-case posix", `Uid == 0 and MTIME < ago("1h")`, true, true, false, false},
		{"upper bare bool entry field", `OFFLOADED`, true, false, true, true},
		{"upper list field needs stat+detail", `5 in ALLOCATEDTARGETS`, true, true, true, true},
		{"upper type sugar", `TYPE == file`, true, true, false, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := Compile(tc.query)
			require.NoError(t, err)

			assert.Equal(t, tc.wantStat, f.NeedsStat(), "NeedsStat")
			assert.Equal(t, tc.wantEntry, f.NeedsEntryInfo(), "NeedsEntryInfo")
			assert.Equal(t, tc.wantDetail, f.NeedsEntryDetails(), "NeedsEntryDetails")

			keep, err := f.Evaluate(fi)
			require.NoError(t, err)
			assert.Equal(t, tc.wantKeep, keep)
		})
	}
}
