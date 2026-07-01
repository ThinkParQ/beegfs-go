// This is a unit test.
//
// Coverage: parseNodesFile's inline-comment stripping -- a regression test
// for a line like "node1   # rack 9" becoming the literal bogus hostname
// "node1   # rack 9" instead of just "node1" -- plus whole-line comments,
// blank lines, and comma-separated names on one line, all combined in one
// file; and buildRemoteArgsForWorkload's leading-positional-only stripping
// -- a regression test for a flag value that happens to equal another
// workload's name being silently dropped, since the old implementation
// matched by value anywhere in the arg list rather than by position.
package iotest

import (
	"os"
	"path/filepath"
	"testing"
)

func nodeNames(specs []NodeSpec) []string {
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = s.Name
	}
	return names
}

func TestParseNodesFileInlineComments(t *testing.T) {
	content := "node1   # rack 9\n" +
		"# a whole-line comment\n" +
		"\n" +
		"node2,node3  # both in rack 10\n" +
		"node4#no space before the hash\n"
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes.txt")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got, err := parseNodesFile(path)
	if err != nil {
		t.Fatalf("parseNodesFile: %v", err)
	}
	want := []string{"node1", "node2", "node3", "node4"}
	names := nodeNames(got)
	if len(names) != len(want) {
		t.Fatalf("got %v, want %v", names, want)
	}
	for i, w := range want {
		if names[i] != w {
			t.Errorf("node %d: got %q, want %q", i, names[i], w)
		}
	}
}

// TestBuildRemoteArgsForWorkloadValueNotConfusedWithWorkloadName is a
// regression test: a flag value that happens to equal another selected
// workload's name must be passed through untouched, since it appears after
// a flag token and is therefore not a positional workload-name arg.
func TestBuildRemoteArgsForWorkloadValueNotConfusedWithWorkloadName(t *testing.T) {
	osArgs := []string{
		"beegfs", "iotest", "start", "soak", "bench",
		"--path", "/mnt/x",
		"--iolog-file", "bench", // a flag value that happens to equal a workload name
	}
	got := buildRemoteArgsForWorkload(osArgs, []string{"soak", "bench"}, "soak")

	want := []string{"iotest", "start", "soak", "--path", "/mnt/x", "--iolog-file", "bench"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("arg %d: got %q, want %q (full: %v)", i, got[i], w, got)
		}
	}
}

func TestBuildRemoteArgsForWorkloadStripsLeadingWorkloadNames(t *testing.T) {
	osArgs := []string{"beegfs", "iotest", "start", "soak", "bench", "--path", "/mnt/x"}
	got := buildRemoteArgsForWorkload(osArgs, []string{"soak", "bench"}, "bench")

	want := []string{"iotest", "start", "bench", "--path", "/mnt/x"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("arg %d: got %q, want %q (full: %v)", i, got[i], w, got)
		}
	}
}
