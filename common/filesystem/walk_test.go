package filesystem

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/kvstore"
)

func TestWalkDirLexicographically(t *testing.T) {
	// fsEntry defines a file system entry relative to the test root.
	// The field isDir indicates whether the entry is a directory.
	type fsEntry struct {
		path  string // Relative path from the test root.
		isDir bool   // true for directories, false for files.
	}

	// Define the file system structure.
	// (Note: order in this slice doesn't affect the resulting file system.)
	entries := []fsEntry{
		{path: "arm", isDir: true},
		{path: "arm/rockchip", isDir: true},
		{path: "arm/rockchip.yaml", isDir: false},
		{path: "arm/rockchip/pmu.yaml", isDir: false},
		{path: "arm/rtsm-dcscb.txt", isDir: false},
		{path: "bar", isDir: true},
		{path: "bar/rockchip", isDir: false},
		{path: "bar/rockchip.yaml", isDir: false},
		{path: "bar/rtsm-dcscb.txt", isDir: false},
		// Add more entries here as needed.
	}

	// Define the expected walk order relative to the test root.
	// Based on our algorithm, for each directory the order is:
	//   (directory itself) then its children (sorted using the name with
	//    a trailing "/" appended for directories).
	//
	// For the "arm" directory, its children are sorted as:
	// "rockchip.yaml" (file, compared as "rockchip.yaml"),
	// "rockchip" (directory, compared as "rockchip/"),
	// "rtsm-dcscb.txt" (file).
	//
	// The overall walk order will then be:
	//  1. "arm"                        (the "arm" directory itself)
	//  2. "arm/rockchip.yaml"          (child file)
	//  3. "arm/rockchip"               (child directory)
	//  4. "arm/rockchip/pmu.yaml"      (child file of "arm/rockchip")
	//  5. "arm/rtsm-dcscb.txt"         (child file)
	expectedOrder := []string{
		"arm",
		"arm/rockchip.yaml",
		"arm/rockchip", // If rockchip is a directory it will be walked after rockchip.yaml.
		"arm/rockchip/pmu.yaml",
		"arm/rtsm-dcscb.txt",
		"bar",
		"bar/rockchip", // If rockchip is a file it will be walked before rockchip.yaml.
		"bar/rockchip.yaml",
		"bar/rtsm-dcscb.txt",
	}

	// Create a temporary directory to serve as the test root.
	root := t.TempDir()

	mapstoreDir := t.TempDir()
	ms, closeDB, err := kvstore.NewMapStore[map[string]int](badger.DefaultOptions(mapstoreDir))
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, closeDB())
	}()

	// Build the file system structure.
	for _, entry := range entries {
		fullPath := filepath.Join(root, entry.path)
		if entry.isDir {
			if err := os.MkdirAll(fullPath, 0755); err != nil {
				t.Fatalf("Failed to create directory %q: %v", fullPath, err)
			}
		} else {
			// Ensure the parent directory exists.
			if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
				t.Fatalf("Failed to create parent directory for %q: %v", fullPath, err)
			}
			if err := os.WriteFile(fullPath, []byte("test content"), 0644); err != nil {
				t.Fatalf("Failed to create file %q: %v", fullPath, err)
			}
		}

		// Directories inserted into BadgerDB must have a slash to distinguish them from regular
		// entries and ensure consistent sorting order.
		badgerPath := entry.path
		if entry.isDir {
			badgerPath += "/"
		}

		// Add the same paths to BadgerDB:
		_, _, release, err := ms.CreateAndLockEntry(badgerPath)
		assert.NoError(t, err, "unable to create path", badgerPath)
		assert.NoError(t, release(), "unable to release path", badgerPath)
	}

	// Walk the directory tree using the custom WalkDirLexicographically function.
	var walked []string
	err = WalkDirLexicographically(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// Compute the path relative to the temporary root.
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		// Skip the root itself.
		if rel == "." {
			return nil
		}
		walked = append(walked, rel)
		return nil
	})
	if err != nil {
		t.Fatalf("WalkDirLexicographically returned error: %v", err)
	}

	// Compare the walk order with our expected order.
	if !reflect.DeepEqual(walked, expectedOrder) {
		t.Errorf("Walk order mismatch:\nGot:  %v\nWant: %v", walked, expectedOrder)
	}

	// Check BadgerDB returns entries in the same order they were walked.
	getNext, cleanupIterator, err := ms.GetEntries()
	require.NoError(t, err)
	defer cleanupIterator()

	var iterated []string
	for _, fsPath := range walked {
		dbPath, err := getNext()
		require.NoError(t, err, "error getting next path to compare", fsPath)
		// Even though directories are walked as though they end in a slash, they are not returned
		// with a slash. We must clean all file paths returned by BadgerDB to ensure they match the
		// paths returned by WalkDirLexicographically.
		iterated = append(iterated, filepath.Clean(dbPath.Key))
	}

	if !reflect.DeepEqual(walked, iterated) {
		t.Errorf("WalkDirLexicographically and BadgerDB disagree about the order:\nWalkDir:  %v\nBadgerDB: %v", walked, iterated)
	}

}

func TestWalkSortedPathFileAndDirectory(t *testing.T) {
	type testCase struct {
		name          string
		pattern       string
		startAfter    string
		expectedPaths []string
	}

	commonTestPaths := []string{
		"/a/a/a.txt",
		"/data/a/a.txt",
		"/data/a/z.txt",
		"/data/b.txt",
		"/data/b/b.txt",
		"/data/b/b/b.txt",
		"/data/b/y.txt",
		"/data/b0.txt",
		"/data/c/c.txt",
		"/data/c/c.txt2",
		"/data/c/x.txt",
		"/data1/a/a.txt",
		"/data2/a/a.txt",
		"/z/a/a.txt",
		"/deep/a/b0",
		"/deep/a/b.txt",
		"/deep/a/b/c0",
		"/deep/a/b/c.txt",
		"/deep/a/b/c/d0",
		"/deep/a/b/c/d.txt",
		"/deep/a/b/c/d/e0",
		"/deep/a/b/c/d/e.txt",
		"/deep/a/b/c/d/e/f0",
		"/deep/a/b/c/d/e/f.txt",
		"/brackets/file[abc",
	}

	mountDir := t.TempDir()
	for _, path := range commonTestPaths {
		path := filepath.Join(mountDir, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(""), 0o644))
	}
	provider := BeeGFS{MountPoint: mountDir}
	tests := []testCase{
		{
			name:    "basic-directory-walk",
			pattern: "/data",
			expectedPaths: []string{
				"/data/a/a.txt",
				"/data/a/z.txt",
				"/data/b.txt",
				"/data/b/b.txt",
				"/data/b/b/b.txt",
				"/data/b/y.txt",
				"/data/b0.txt",
				"/data/c/c.txt",
				"/data/c/c.txt2",
				"/data/c/x.txt",
			},
		},
		{
			name:          "glob-no-matching-target",
			pattern:       "/data/**/target.txt",
			expectedPaths: []string{},
		},
		{
			name:    "glob-all-data-text-files",
			pattern: "/data/**/*.txt",
			expectedPaths: []string{
				"/data/a/a.txt",
				"/data/a/z.txt",
				"/data/b.txt",
				"/data/b/b.txt",
				"/data/b/b/b.txt",
				"/data/b/y.txt",
				"/data/b0.txt",
				"/data/c/c.txt",
				"/data/c/x.txt",
			},
		},
		{
			// '?' matches exactly one non-separator character. The full pattern
			// must reach the file depth; /data? alone only matches the sibling
			// directories themselves.
			name:    "single-character-wildcard-data-siblings",
			pattern: "/data?/a/a.txt",
			expectedPaths: []string{
				"/data1/a/a.txt",
				"/data2/a/a.txt",
			},
		},
		{
			// '*' matches zero or more non-separator characters, so /data*
			// covers /data, /data1, /data2. A deeper pattern is required to
			// reach files inside those directories.
			name:    "root-directory-with-wildcard",
			pattern: "/data*/a/a.txt",
			expectedPaths: []string{
				"/data/a/a.txt",
				"/data1/a/a.txt",
				"/data2/a/a.txt",
			},
		},
		{
			// The trailing '*' does not cross '/': /data/b/b/b.txt requires two
			// path components after /b/ and is no longer matched.
			name:    "files-from-common-directories-in-root-directories",
			pattern: "/data*/b/*",
			expectedPaths: []string{
				"/data/b/b.txt",
				"/data/b/y.txt",
			},
		},
		{
			name:    "single file",
			pattern: "/data/a/a.txt",
			expectedPaths: []string{
				"/data/a/a.txt",
			},
		},
		{
			// An invalid glob pattern (unclosed bracket) is not a glob: it is
			// treated as a literal file path and streamed back if the file exists.
			name:    "invalid-glob-treated-as-literal-path",
			pattern: "/brackets/file[abc",
			expectedPaths: []string{
				"/brackets/file[abc",
			},
		},
		{
			name:       "resume-directory-walk",
			pattern:    "/data",
			startAfter: "/data/b/b.txt",
			expectedPaths: []string{
				"/data/b/b/b.txt",
				"/data/b/y.txt",
				"/data/b0.txt",
				"/data/c/c.txt",
				"/data/c/c.txt2",
				"/data/c/x.txt",
			},
		},
		{
			name:       "complete-directory-walk-after-resume",
			pattern:    "/data",
			startAfter: "/data/a/z.txt",
			expectedPaths: []string{
				"/data/b.txt",
				"/data/b/b.txt",
				"/data/b/b/b.txt",
				"/data/b/y.txt",
				"/data/b0.txt",
				"/data/c/c.txt",
				"/data/c/c.txt2",
				"/data/c/x.txt",
			},
		},
		{
			name:       "glob-range-excludes-start-after-match",
			pattern:    "/data/c/[a-c]*",
			startAfter: "/data/c/c.txt",
			expectedPaths: []string{
				"/data/c/c.txt2",
			},
		},
		{
			name:    "doublestar-to-grab-all-txt-files",
			pattern: "/data/**/*.txt",
			expectedPaths: []string{
				"/data/a/a.txt",
				"/data/a/z.txt",
				"/data/b.txt",
				"/data/b/b.txt",
				"/data/b/b/b.txt",
				"/data/b/y.txt",
				"/data/b0.txt",
				"/data/c/c.txt",
				"/data/c/x.txt",
			},
		},
		{
			name:       "doublestar-to-grab-all-txt-files-after-resume",
			pattern:    "/**/*.txt",
			startAfter: "/data/b/y.txt",
			expectedPaths: []string{
				"/data/b0.txt",
				"/data/c/c.txt",
				"/data/c/x.txt",
				"/data1/a/a.txt",
				"/data2/a/a.txt",
				"/deep/a/b.txt",
				"/deep/a/b/c.txt",
				"/deep/a/b/c/d.txt",
				"/deep/a/b/c/d/e.txt",
				"/deep/a/b/c/d/e/f.txt",
				"/z/a/a.txt",
			},
		},
		{
			// '*' does not cross path separators: deep/a/* matches only the
			// direct file children of deep/a/, not files in deeper subdirs.
			name:    "deeply-nested-glob-pattern",
			pattern: "deep/a/*",
			expectedPaths: []string{
				"/deep/a/b0",
				"/deep/a/b.txt",
			},
		},
		{
			name:    "deeply-nested-glob-pattern-with-doublestar",
			pattern: "deep/**/*0",
			expectedPaths: []string{
				"/deep/a/b0",
				"/deep/a/b/c0",
				"/deep/a/b/c/d0",
				"/deep/a/b/c/d/e0",
				"/deep/a/b/c/d/e/f0",
			},
		},
	}

	ctx := context.Background()
	for _, test := range tests {

		t.Run(test.name, func(t *testing.T) {
			responseChan, _, err := StreamPathsLexicographically(ctx, provider, test.pattern, test.startAfter, 0, nil)
			require.NoError(t, err)

			paths := []string{}
			for resp := range responseChan {
				require.NoError(t, resp.Err)
				paths = append(paths, resp.Path)
			}

			slices.Sort(test.expectedPaths)
			if test.expectedPaths != nil {
				assert.Equal(t, test.expectedPaths, paths)
			} else {
				assert.Equal(t, commonTestPaths, paths)
			}
		})
	}
}

// TestWalkResumeAcrossNestedDirectory reproduces a bug where the resume token emitted when a walk
// is cut off can point earlier than the last path actually sent. This happens when the cutoff
// lands on a directory entry's sibling immediately after a nested subdirectory: the parent
// directory's resume token only advances when it emits a file directly, so it does not account for
// paths already sent from within the subdirectory. Resuming from that stale token causes the
// subdirectory to be walked again and its already-sent paths to be duplicated.
func TestWalkResumeAcrossNestedDirectory(t *testing.T) {
	mountDir := t.TempDir()
	paths := []string{
		"/data/a/a.txt",
		"/data/a/z.txt",
		"/data/b.txt",
		"/data/b/b.txt",
		"/data/b/b/b.txt",
		"/data/b/y.txt",
		"/data/b0.txt",
	}
	for _, path := range paths {
		path := filepath.Join(mountDir, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(""), 0o644))
	}
	provider := BeeGFS{MountPoint: mountDir}
	ctx := context.Background()

	// Cut the walk off right after /data/b/b/b.txt, which is sent from within the nested "b/b"
	// subdirectory rather than directly from "b". This is the boundary that triggers the stale
	// resume token.
	firstChan, stopWalk, err := StreamPathsLexicographically(ctx, provider, "/data", "", 0, nil)
	require.NoError(t, err)

	firstPass, resumeToken := consumeWalkThen(t, firstChan, stopWalk, 5)
	require.Equal(t, []string{
		"/data/a/a.txt",
		"/data/a/z.txt",
		"/data/b.txt",
		"/data/b/b.txt",
		"/data/b/b/b.txt",
	}, firstPass)
	require.Equal(t, "data/b/b/b.txt", resumeToken, "the resume token must name the last path actually sent, not an earlier one from the parent directory")

	secondChan, _, err := StreamPathsLexicographically(ctx, provider, "/data", resumeToken, 0, nil)
	require.NoError(t, err)

	var secondPass []string
	for resp := range secondChan {
		require.NoError(t, resp.Err)
		secondPass = append(secondPass, resp.Path)
	}

	seen := make(map[string]bool, len(firstPass))
	for _, path := range firstPass {
		seen[path] = true
	}
	for _, path := range secondPass {
		assert.Falsef(t, seen[path], "path %q was sent in both the first and resumed pass", path)
	}
}

// TestWalkWithDirsDirectoriesDoNotAnchorResume guards against a bug specific to
// StreamPathsLexicographicallyWithDirs: a directory can be a prefix of a sibling file's name (e.g.
// directory "b" next to file "b.txt"), and under this package's directory-aware ordering (a period
// sorts before a slash) the file sorts before the directory. If the directory's own bare path were
// ever used as the resume anchor, plain string comparison of that sibling file's full path against
// it would disagree with the true walk order and the file could be resent. Directories never produce
// a job request on their own (see jobRequestBuilder.Process) and are idempotent to reprocess, so
// they're emitted for free without ever becoming the resume anchor - this confirms the resume token
// always lands on a file even when a directory sits directly at the cutoff boundary, and that the
// directory is still picked up by the resumed walk.
func TestWalkWithDirsDirectoriesDoNotAnchorResume(t *testing.T) {
	mountDir := t.TempDir()
	paths := []string{
		"/data/a/a.txt",
		"/data/a/z.txt",
		"/data/b.txt",
		"/data/b/b.txt",
		"/data/b/b/b.txt",
		"/data/b/y.txt",
		"/data/b0.txt",
	}
	for _, path := range paths {
		path := filepath.Join(mountDir, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(""), 0o644))
	}
	provider := BeeGFS{MountPoint: mountDir}
	ctx := context.Background()

	// Stop the walk on the "/data/b" directory itself, which follows the file "/data/b.txt"
	// directly in directory-aware sort order - so a directory sits exactly at the cutoff boundary.
	firstChan, stopWalk, err := StreamPathsLexicographicallyWithDirs(ctx, provider, "/data", "", 0, nil)
	require.NoError(t, err)

	firstPass, resumeToken := consumeWalkThen(t, firstChan, stopWalk, 5)
	require.Equal(t, []string{
		"/data",
		"/data/a",
		"/data/a/a.txt",
		"/data/a/z.txt",
		"/data/b.txt",
	}, firstPass)
	require.Equal(t, "data/b.txt", resumeToken, "the resume token should anchor on the last file sent, not the directory sitting at the cutoff")

	secondChan, _, err := StreamPathsLexicographicallyWithDirs(ctx, provider, "/data", resumeToken, 0, nil)
	require.NoError(t, err)

	var secondPass []string
	for resp := range secondChan {
		require.NoError(t, resp.Err)
		secondPass = append(secondPass, resp.Path)
	}
	// "/data/b" is re-emitted even though its bare path sorts before the "/data/b.txt" anchor:
	// directories are filtered by sortName ("data/b/"), matching the order the walk emits in, so
	// the resumed pass picks up exactly where the first one stopped.
	require.Equal(t, []string{
		"/data/b",
		"/data/b/b.txt",
		"/data/b/b",
		"/data/b/b/b.txt",
		"/data/b/y.txt",
		"/data/b0.txt",
	}, secondPass)

	seenFiles := map[string]bool{}
	for _, path := range firstPass {
		if !strings.HasSuffix(path, ".txt") {
			continue
		}
		seenFiles[path] = true
	}
	for _, path := range secondPass {
		if !strings.HasSuffix(path, ".txt") {
			continue
		}
		assert.Falsef(t, seenFiles[path], "file %q was sent in both the first and resumed pass", path)
	}
}

// TestWalkWithDirsResumeKeepsDirectorySortingBeforeAnchor covers the harder half of the prefix
// problem the test above describes: here the walk is stopped on a *file*, and the directory that
// would have come next is the one at risk. With "/data/b.tx", "/data/b.txt" and the "/data/b"
// directory as siblings, sortName order is b.tx, b.txt, b/ - so stopping on "/data/b.txt" anchors
// the resume at "/data/b.tx" while "/data/b" has not been emitted yet. Comparing the directory's
// bare path against that anchor would place it before the anchor and drop it entirely, even though
// the walk had not reached it. Never stopping on a directory would not help; only comparing by
// sortName does.
func TestWalkWithDirsResumeKeepsDirectorySortingBeforeAnchor(t *testing.T) {
	mountDir := t.TempDir()
	for _, path := range []string{"/data/b.tx", "/data/b.txt", "/data/b/b.txt"} {
		path := filepath.Join(mountDir, path)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
		require.NoError(t, os.WriteFile(path, []byte(""), 0o644))
	}
	provider := BeeGFS{MountPoint: mountDir}
	ctx := context.Background()

	firstChan, stopWalk, err := StreamPathsLexicographicallyWithDirs(ctx, provider, "/data", "", 0, nil)
	require.NoError(t, err)

	// Stop on the file "/data/b.txt", leaving the "/data/b" directory unemitted.
	firstPass, resumeToken := consumeWalkThen(t, firstChan, stopWalk, 2)
	require.Equal(t, []string{"/data", "/data/b.tx"}, firstPass)
	require.Equal(t, "data/b.tx", resumeToken)

	secondChan, _, err := StreamPathsLexicographicallyWithDirs(ctx, provider, "/data", resumeToken, 0, nil)
	require.NoError(t, err)

	var secondPass []string
	for resp := range secondChan {
		require.NoError(t, resp.Err)
		secondPass = append(secondPass, resp.Path)
	}
	require.Equal(t, []string{
		"/data/b.txt",
		"/data/b",
		"/data/b/b.txt",
	}, secondPass, "the resumed walk must pick up every path after the anchor, including /data/b")
}

// consumeWalkThen consumes consume results, then stops the walk on the next one and returns the
// consumed paths along with that result's ResumeToken. This mirrors how requestBuildController
// drives a walk: the result it stops on is deliberately left unconsumed, and its ResumeToken names
// the last file sent before it so resuming from the token re-emits that result. The remainder is
// drained in the background because a walk parked mid-send can only observe the stop once it makes
// progress.
func consumeWalkThen(t *testing.T, walk <-chan *StreamPathResult, stopWalk func(), consume int) (paths []string, resumeToken string) {
	t.Helper()

	for resp := range walk {
		require.NoError(t, resp.Err)
		if len(paths) == consume {
			resumeToken = resp.ResumeToken
			stopWalk()
			go func() {
				for range walk { //nolint:revive // Drain so the walk can observe the stop and finish.
				}
			}()
			return paths, resumeToken
		}
		paths = append(paths, resp.Path)
	}
	t.Fatalf("walk ended after %d results before reaching the %d needed to stop on", len(paths), consume+1)
	return nil, ""
}

func TestIsGlobPattern(t *testing.T) {
	tests := []struct {
		pattern string
		want    bool
	}{
		{"plain.txt", false},
		{"*.txt", true},
		{"a?b", true},
		{"a[bc]", true},
		{"a[bc", false},        // unclosed bracket is not a glob
		{"abc]", false},        // closing bracket without an opener is literal
		{`foo\*.txt`, false},   // escaped meta char is literal
		{`foo\\*.txt`, true},   // escaped backslash, bare meta char
		{`foo\\\*.txt`, false}, // escaped backslash + escaped meta char
		{`a\?b`, false},
		{`a\[b`, false},
		{`a\[b]`, false}, // escaped '[' is not a glob opener; trailing ']' is literal
		{`{a,b}`, true},  // valid alternation expression
		{`{a,b`, false},  // unclosed brace is not a glob
		{`dir/*/file`, true},
		{`dir/**/file`, true},
	}
	for _, tc := range tests {
		t.Run(tc.pattern, func(t *testing.T) {
			assert.Equal(t, tc.want, IsGlobPattern(tc.pattern))
		})
	}
}

func TestStripGlobPattern(t *testing.T) {
	tests := []struct {
		pattern string
		want    string
	}{
		{"plain/path.txt", "plain/path.txt"}, // no meta characters: returned as is
		{"prefix/*.txt", "prefix/"},
		{"a/b?c", "a/b"},
		{"a/b[cd]e", "a/b"},  // strip at the start of the bracket expression
		{"a/b[cd", "a/b[cd"}, // unclosed bracket: not a glob, returned as is
		{`a/b\*c`, `a/b\*c`},
		{"data/2024/file", "data/2024/file"},
		{"data/20??/file", "data/20"},
		{"{a,b}/file", ""},          // '{' at index 0: entire pattern is the glob
		{"prefix/{a,b}", "prefix/"}, // strip at the start of the alternation expression
	}
	for _, tc := range tests {
		t.Run(tc.pattern, func(t *testing.T) {
			assert.Equal(t, tc.want, StripGlobPattern(tc.pattern))
		})
	}
}

func TestUnescape(t *testing.T) {
	tests := []struct {
		pattern string
		want    string
	}{
		{"plain/path.txt", "plain/path.txt"},             // no backslash: fast path
		{`files/\*.txt`, "files/*.txt"},                  // escaped wildcard
		{`files/\?.txt`, "files/?.txt"},                  // escaped '?'
		{`files/\[data\].txt`, "files/[data].txt"},       // escaped brackets
		{`files/\{a,b\}`, `files/{a,b}`},                 // escaped '{' and '}'
		{`a\\b`, `a\b`},                                  // escaped backslash
		{`prefix/\[2024\]/*.csv`, "prefix/[2024]/*.csv"}, // escaped prefix, real glob suffix
		{`a\bc`, `abc`},                                  // ordinary characters can be escaped
		{`trailing\`, `trailing\`},                       // trailing backslash: kept as-is
	}
	for _, tc := range tests {
		t.Run(tc.pattern, func(t *testing.T) {
			assert.Equal(t, tc.want, Unescape(tc.pattern))
		})
	}
}

// TestGlobMatchDirs verifies shell-style glob expansion for index globs: the
// pattern matches itself (no implicit descent into matched subtrees), only
// directories are returned (the per-directory db.db and sibling files are
// skipped), and results are sorted.
func TestGlobMatchDirs(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"proj-a/sub/deep", "proj-b", "other"} {
		require.NoError(t, os.MkdirAll(filepath.Join(root, dir), 0o755))
	}
	for _, f := range []string{"db.db", "proj-a.txt", "proj-a/db.db"} {
		require.NoError(t, os.WriteFile(filepath.Join(root, f), nil, 0o644))
	}

	tests := []struct {
		name    string
		pattern string
		want    []string
	}{
		{name: "single-level glob returns matching dirs, not files or descendants", pattern: "proj-*", want: []string{"proj-a", "proj-b"}},
		{name: "star matches every immediate dir and skips files", pattern: "*", want: []string{"other", "proj-a", "proj-b"}},
		{name: "multi-segment glob matches at that depth only", pattern: "proj-*/sub", want: []string{"proj-a/sub"}},
		// A '**' token matches the directory AND every descendant directory; callers
		// that turn each match into a recursive search root must dedupe these (see
		// pruneNestedDirs in the index command) to avoid duplicated output.
		{name: "doublestar returns a dir and all its descendants", pattern: "proj-a/**", want: []string{"proj-a", "proj-a/sub", "proj-a/sub/deep"}},
		{name: "no match returns empty", pattern: "nope-*", want: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := GlobMatchDirs(context.Background(), root, tc.pattern)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
