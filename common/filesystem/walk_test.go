package filesystem

import (
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/kvstore"
)

func TestWalkDirLExicographically(t *testing.T) {
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
