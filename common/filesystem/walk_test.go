package filesystem

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
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
		maxPaths      int
		expectMore    bool
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
			name:     "basic-directory-walk",
			pattern:  "/data",
			maxPaths: -1,
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
			maxPaths:      -1,
			expectedPaths: []string{},
		},
		{
			name:     "glob-all-data-text-files",
			pattern:  "/data/**/*.txt",
			maxPaths: -1,
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
			name:     "single-character-wildcard-data-siblings",
			pattern:  "/data?",
			maxPaths: -1,
			expectedPaths: []string{
				"/data1/a/a.txt",
				"/data2/a/a.txt",
			},
		},
		{
			name:     "root-directory-with-wildcard",
			pattern:  "/data*",
			maxPaths: -1,
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
				"/data1/a/a.txt",
				"/data2/a/a.txt",
			},
		},
		{
			name:     "files-from-common-directories-in-root-directories",
			pattern:  "/data*/b/*",
			maxPaths: -1,
			expectedPaths: []string{
				"/data/b/b.txt",
				"/data/b/b/b.txt",
				"/data/b/y.txt",
			},
		},
		{
			name:     "single file",
			pattern:  "/data/a/a.txt",
			maxPaths: -1,
			expectedPaths: []string{
				"/data/a/a.txt",
			},
		},
		{
			name:       "resume-directory-walk",
			pattern:    "/data",
			startAfter: "/data/b/b.txt",
			maxPaths:   4,
			expectMore: true,
			expectedPaths: []string{
				"/data/b/b/b.txt",
				"/data/b/y.txt",
				"/data/b0.txt",
				"/data/c/c.txt",
			},
		},
		{
			name:     "directory-walk-limited-results",
			pattern:  "/data",
			maxPaths: 2,
			expectedPaths: []string{
				"/data/a/a.txt",
				"/data/a/z.txt",
			},
			expectMore: true,
		},
		{
			name:       "complete-directory-walk-after-resume",
			pattern:    "/data",
			startAfter: "/data/a/z.txt",
			expectMore: false,
			maxPaths:   -1,
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
			maxPaths:   -1,
			expectedPaths: []string{
				"/data/c/c.txt2",
			},
		},
		{
			name:     "doublestar-to-grab-all-txt-files",
			pattern:  "/data/**/*.txt",
			maxPaths: -1,
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
			maxPaths:   -1,
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
			name:     "deeply-nested-glob-pattern",
			pattern:  "deep/*",
			maxPaths: -1,
			expectedPaths: []string{
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
			},
		},
		{
			name:     "deeply-nested-glob-pattern-with-doublestar",
			pattern:  "deep/**/*0",
			maxPaths: -1,
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
			maxPaths := test.maxPaths
			if maxPaths == -1 {
				maxPaths = len(commonTestPaths)
			}

			responseChan, err := StreamPathsLexicographically(ctx, provider, test.pattern, test.startAfter, maxPaths, 0, nil)
			require.NoError(t, err)

			paths := []string{}
			moreWork := false
			for resp := range responseChan {
				if resp.ResumeToken != "" {
					moreWork = true
					break
				}
				paths = append(paths, resp.Path)
			}
			assert.Equal(t, test.expectMore, moreWork)

			slices.Sort(test.expectedPaths)
			if test.expectedPaths != nil {
				assert.Equal(t, test.expectedPaths, paths)
			} else {
				assert.Equal(t, commonTestPaths, paths)
			}
		})
	}
}
