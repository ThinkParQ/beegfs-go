package filesystem

import (
	"errors"
	"syscall"
	"testing"
)

// TestCompileNeedsDetection verifies that a compiled filter reports which data it needs based on the
// fields the expression references. This drives the routing that keeps POSIX-only filters cheap and
// only fetches entry info when a BeeGFS field is used.
func TestCompileNeedsDetection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		expr         string
		wantStat     bool
		wantEntry    bool
		wantDetails  bool
		wantCompile  bool // false => expect a compile error
		wantRejected bool // CompileFilter should reject (references BeeGFS fields)
	}{
		{name: "posix only", expr: "uid == 0 and size > 1MB", wantStat: true, wantCompile: true},
		{name: "name only skips stat", expr: `glob(name, "*.txt")`, wantCompile: true},
		{name: "path only skips stat", expr: `glob(path, "/foo/*")`, wantCompile: true},
		{name: "entry level not detail", expr: `entryid == "abc"`, wantEntry: true, wantCompile: true, wantRejected: true},
		{name: "offloaded is detail", expr: "offloaded", wantEntry: true, wantDetails: true, wantCompile: true, wantRejected: true},
		{name: "targets is detail", expr: "5 in targets", wantEntry: true, wantDetails: true, wantCompile: true, wantRejected: true},
		{name: "mixed or", expr: "uid == 0 or offloaded", wantStat: true, wantEntry: true, wantDetails: true, wantCompile: true, wantRejected: true},
		{name: "allocatedtargets needs stat and details", expr: "3 in allocatedtargets", wantStat: true, wantEntry: true, wantDetails: true, wantCompile: true, wantRejected: true},
		{name: "chunksize with units", expr: "chunksize > 1MB", wantEntry: true, wantDetails: true, wantCompile: true, wantRejected: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, err := Compile(tc.expr)
			if !tc.wantCompile {
				if err == nil {
					t.Fatalf("Compile(%q) = nil error, want error", tc.expr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Compile(%q) error: %v", tc.expr, err)
			}
			if f.NeedsStat() != tc.wantStat {
				t.Errorf("NeedsStat() = %v, want %v", f.NeedsStat(), tc.wantStat)
			}
			if f.NeedsEntryInfo() != tc.wantEntry {
				t.Errorf("NeedsEntryInfo() = %v, want %v", f.NeedsEntryInfo(), tc.wantEntry)
			}
			if f.NeedsEntryDetails() != tc.wantDetails {
				t.Errorf("NeedsEntryDetails() = %v, want %v", f.NeedsEntryDetails(), tc.wantDetails)
			}
			// CompileFilter (POSIX-only contexts) must reject expressions referencing BeeGFS fields.
			if _, err := CompileFilter(tc.expr); (err != nil) != tc.wantRejected {
				t.Errorf("CompileFilter(%q) rejected=%v, want %v (err=%v)", tc.expr, err != nil, tc.wantRejected, err)
			}
		})
	}
}

// TestCompileLiteralProtection ensures identifier rewrites never reach inside quoted string
// literals, so field-like words in values (poolname == "fast-targets") or glob patterns
// (glob(name, "*targets*")) are neither rewritten nor counted as referenced fields.
func TestCompileLiteralProtection(t *testing.T) {
	t.Parallel()

	// A field-like word inside a string value must not turn the expression into a BeeGFS filter.
	globExpr := `glob(name, "*targets*")`
	f, err := Compile(globExpr)
	if err != nil {
		t.Fatalf("Compile(%q) error: %v", globExpr, err)
	}
	if f.NeedsEntryInfo() {
		t.Errorf("Compile(%q).NeedsEntryInfo() = true, want false (targets appears only inside a literal)", globExpr)
	}
	if keep, err := f.Evaluate(FileInfo{Name: "my_targets_file"}); err != nil || !keep {
		t.Errorf("Evaluate(name=my_targets_file) = (%v, %v), want (true, nil)", keep, err)
	}

	// A string value containing a field name must compare literally, not be rewritten.
	poolExpr := `poolname == "fast-targets"`
	pf, err := Compile(poolExpr)
	if err != nil {
		t.Fatalf("Compile(%q) error: %v", poolExpr, err)
	}
	if keep, err := pf.Evaluate(FileInfo{PoolName: "fast-targets"}); err != nil || !keep {
		t.Errorf("Evaluate(poolname=fast-targets) = (%v, %v), want (true, nil)", keep, err)
	}
	if keep, _ := pf.Evaluate(FileInfo{PoolName: "slow"}); keep {
		t.Errorf("Evaluate(poolname=slow) = true, want false")
	}
}

// TestEvaluateBeeGFSFields exercises evaluation against populated BeeGFS FileInfo fields.
func TestEvaluateBeeGFSFields(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		expr string
		fi   FileInfo
		want bool
	}{
		{"target in list", "5 in targets", FileInfo{Targets: []int{3, 5}}, true},
		{"target not in list", "9 in targets", FileInfo{Targets: []int{3, 5}}, false},
		{"offloaded true", "offloaded", FileInfo{Offloaded: true}, true},
		{"not offloaded", "not offloaded", FileInfo{Offloaded: false}, true},
		{"pattern equals", `pattern == "buddymirror"`, FileInfo{Pattern: "buddymirror"}, true},
		{"datastate", `datastate == "manualrestore"`, FileInfo{DataState: "manualrestore"}, true},
		{"mixed or matches beegfs", "uid == 999 or offloaded", FileInfo{Uid: 0, Offloaded: true}, true},
		{"mixed or matches posix", "uid == 0 or offloaded", FileInfo{Uid: 0, Offloaded: false}, true},
		{"mixed and", "pool == 2 and locked", FileInfo{Pool: 2, Locked: true}, true},
		{"mixed and fails", "pool == 2 and locked", FileInfo{Pool: 2, Locked: false}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, err := Compile(tc.expr)
			if err != nil {
				t.Fatalf("Compile(%q) error: %v", tc.expr, err)
			}
			got, err := f.Evaluate(tc.fi)
			if err != nil {
				t.Fatalf("Evaluate error: %v", err)
			}
			if got != tc.want {
				t.Errorf("Evaluate(%q) = %v, want %v", tc.expr, got, tc.want)
			}
		})
	}
}

// TestDecide covers the short-circuit / fetch-once / skip contract of Decide.
func TestDecide(t *testing.T) {
	t.Parallel()

	newCounters := func() (lstatN, fetchN, toInfoN *int, lstat func() (*syscall.Stat_t, error), fetch func() (string, error), toInfo func(string, *syscall.Stat_t, string) (FileInfo, error)) {
		var ln, fn, tn int
		lstat = func() (*syscall.Stat_t, error) { ln++; return &syscall.Stat_t{Uid: 0}, nil }
		fetch = func() (string, error) { fn++; return "fetched", nil }
		toInfo = func(_ string, _ *syscall.Stat_t, s string) (FileInfo, error) {
			tn++
			return FileInfo{Offloaded: true}, nil
		}
		return &ln, &fn, &tn, lstat, fetch, toInfo
	}

	t.Run("nil filter always fetches, never skips", func(t *testing.T) {
		t.Parallel()
		_, fetchN, _, lstat, fetch, toInfo := newCounters()
		v, skip, err := Decide(nil, "/p", lstat, fetch, toInfo)
		if err != nil || skip || v != "fetched" || *fetchN != 1 {
			t.Fatalf("got (v=%q skip=%v err=%v fetches=%d), want (fetched,false,nil,1)", v, skip, err, *fetchN)
		}
	})

	t.Run("posix-only non-match skips without fetching", func(t *testing.T) {
		t.Parallel()
		f, _ := Compile("uid == 42")
		lstatN, fetchN, _, lstat, fetch, toInfo := newCounters()
		v, skip, err := Decide(f, "/p", lstat, fetch, toInfo)
		if err != nil || !skip || v != "" {
			t.Fatalf("got (v=%q skip=%v err=%v), want (\"\",true,nil)", v, skip, err)
		}
		if *fetchN != 0 {
			t.Errorf("fetch called %d times, want 0 (non-match must not fetch)", *fetchN)
		}
		if *lstatN != 1 {
			t.Errorf("lstat called %d times, want 1", *lstatN)
		}
	})

	t.Run("posix-only match fetches once", func(t *testing.T) {
		t.Parallel()
		f, _ := Compile("uid == 0")
		_, fetchN, toInfoN, lstat, fetch, toInfo := newCounters()
		v, skip, err := Decide(f, "/p", lstat, fetch, toInfo)
		if err != nil || skip || v != "fetched" {
			t.Fatalf("got (v=%q skip=%v err=%v), want (fetched,false,nil)", v, skip, err)
		}
		if *fetchN != 1 {
			t.Errorf("fetch called %d times, want 1", *fetchN)
		}
		if *toInfoN != 0 {
			t.Errorf("toInfo called %d times, want 0 (posix-only path must not call toInfo)", *toInfoN)
		}
	})

	t.Run("beegfs filter fetches once then evaluates whole", func(t *testing.T) {
		t.Parallel()
		f, _ := Compile("offloaded")
		lstatN, fetchN, toInfoN, lstat, fetch, toInfo := newCounters()
		v, skip, err := Decide(f, "/p", lstat, fetch, toInfo)
		if err != nil || skip || v != "fetched" {
			t.Fatalf("got (v=%q skip=%v err=%v), want (fetched,false,nil)", v, skip, err)
		}
		if *fetchN != 1 || *toInfoN != 1 {
			t.Errorf("fetch=%d toInfo=%d, want 1 and 1", *fetchN, *toInfoN)
		}
		if *lstatN != 0 {
			t.Errorf("lstat called %d times, want 0 (offloaded does not need stat)", *lstatN)
		}
	})

	t.Run("toInfo error is returned with fetched value", func(t *testing.T) {
		t.Parallel()
		f, _ := Compile("offloaded")
		sentinel := errors.New("details unavailable")
		lstat := func() (*syscall.Stat_t, error) { return &syscall.Stat_t{}, nil }
		fetch := func() (string, error) { return "fetched", nil }
		toInfo := func(string, *syscall.Stat_t, string) (FileInfo, error) { return FileInfo{}, sentinel }
		v, skip, err := Decide(f, "/p", lstat, fetch, toInfo)
		if !errors.Is(err, sentinel) {
			t.Fatalf("err = %v, want %v", err, sentinel)
		}
		if skip || v != "fetched" {
			t.Errorf("got (v=%q skip=%v), want (fetched,false) so caller can apply its own policy", v, skip)
		}
	})
}
