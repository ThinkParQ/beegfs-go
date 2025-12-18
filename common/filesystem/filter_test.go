package filesystem

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompileFilter_ValidExpressions(t *testing.T) {
	now := time.Now()
	fi := FileInfo{
		Path:  "/tmp/testfile.txt",
		Name:  "testfile.txt",
		Size:  123456,
		Mode:  0100644,
		Perm:  0644,
		Mtime: now.Add(-2 * time.Hour),
		Atime: now.Add(-1 * time.Hour),
		Ctime: now.Add(-3 * time.Hour),
		Uid:   1000,
		Gid:   1000,
	}

	tests := []struct {
		name string
		expr string
		want bool
	}{
		{"MatchName", `name == "testfile.txt"`, true},
		{"NoMatchName", `name == "other.txt"`, false},
		{"MatchPath", `path == "/tmp/testfile.txt"`, true},
		{"NoMatchPath", `path == "/notfound"`, false},
		{"MatchSize", `size == 123456`, true},
		{"NoMatchSize", `size > 200000`, false},
		{"MatchModeDecimal", `mode == 33188`, true},              // Decimals are allowed for mode.
		{"MatchModeOctalNoLeadingZero", `mode == 100644`, false}, // A leading zero is required for mode when specifying octal (to allow decimals as well).
		{"MatchModeOctalLeadingZero", `mode == 0100644`, true},
		{"MatchModeOctalLeading0o", `mode == 0o0100644`, true},
		{"MatchPermDecimal", `perm == 420`, false}, // Decimals are not allowed for perm so it can be specified without a leading zero.
		{"MatchPermOctalNoLeadingZero", `perm == 644`, true},
		{"MatchPermOctalLeadingZero", `perm == 0644`, true},
		{"MatchPermOctalLeading0o", `perm == 0o644`, true},
		{"MatchUid", `uid == 1000`, true},
		{"MatchGid", `gid == 1000`, true},
		{"MtimeOlderThan1h", `mtime > 1h`, true},
		{"AtimeLessThan30m", `atime < 30m`, false},
		{"CtimeOlderThan2h", `ctime > 2h`, true},
		{"SizeUnitsKB", `size >= 120KB`, true},
		{"SizeUnitsMiB", `size < 1MiB`, true},
		{"GlobName", `glob(name, "*file.txt")`, true},
		{"NoGlobMatch", `glob(name, "foo*")`, false},
		{"RegexNameFunc", `regex(name, "test.*\\.txt")`, true},
		{"NoRegexFunc", `regex(name, "^foo")`, false},
		{"Combined", `name == "testfile.txt" && size > 100000 && perm == 0644`, true},
		{"ComplexCond1", `(size > 100000 && perm == 0644) || (uid == 0 && gid == 0)`, true},
		{"ComplexCond2", `(mtime > 1h && mtime < 3h)`, true},
		{"ComplexCond3", `size >= 100KB && size <= 200KB`, true},
		{"ComplexCond4", `uid == 1000 && gid == 1000 && glob(path, "/tmp/*.txt")`, true},
		{"ComplexCond5", `!(perm != 0644)`, true},
		{"ComplexSizeTime", `size > bytes("100KB") && mtime > ago("3h")`, true},
		{"ComplexNeg1", `size < 100000 && perm == 0644`, false},
		{"ComplexNeg2", `uid == 0 || gid == 0`, false},
		{"ComplexNeg3", `regex(name, "^foo") || glob(path, "*.md")`, false},
		{"ComplexNeg4", `!(size == 123456)`, false},
		{"ComplexNeg5", `(mtime < 1h) || (atime > 2h)`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := CompileFilter(tt.expr)
			assert.NoError(t, err, "compileFilter(%q) returned error", tt.expr)
			ok, err := filter(fi)
			assert.NoError(t, err, "filter(%q) returned error", tt.expr)
			assert.Equal(t, tt.want, ok, "filter(%q) = %v, want %v", tt.expr, ok, tt.want)
		})
	}
}

func TestCompileFilter_InvalidExpression(t *testing.T) {
	_, err := CompileFilter("not_a_valid_expr(")
	assert.Error(t, err)
}

func TestCompileFilter_TimeAndSizeUnits(t *testing.T) {
	now := time.Now()
	fi := FileInfo{Mtime: now.Add(-48 * time.Hour), Size: 2 * 1024 * 1024}

	// Time unit days
	filter, err := CompileFilter(`mtime > 1d`)
	assert.NoError(t, err)
	ok, err := filter(fi)
	assert.NoError(t, err)
	assert.True(t, ok)

	// Size unit MiB
	filter, err = CompileFilter(`size >= 2MiB`)
	assert.NoError(t, err)
	ok, err = filter(fi)
	assert.NoError(t, err)
	assert.True(t, ok)
}

func TestPreprocessDSL_Rewrites(t *testing.T) {
	cases := []struct {
		input    string
		contains string
	}{
		{`perm == 0755`, `Perm == 493`},
		{`mtime > 1d`, `Mtime < ago("1d")`},
		{`size >= 2MiB`, `Size >= bytes("2MiB")`},
	}

	for _, tt := range cases {
		t.Run(tt.input, func(t *testing.T) {
			out := preprocessDSL(tt.input)
			assert.Contains(t, out, tt.contains)
		})
	}
}
