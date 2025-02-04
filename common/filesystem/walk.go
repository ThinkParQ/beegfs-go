// Adapted from the Go standard library's filepath.WalkDir implementation:
// https://cs.opensource.google/go/go/+/refs/tags/go1.23.5:src/path/filepath/path.go;l=395
//
// Original implementation Copyright 2009 The Go Authors. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file distributed with the original source.
//
// Modifications include sorting directory entries in a way that mimics BadgerDB's lexicographical
// key ordering by appending "/" to directory names.
package filesystem

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
)

type WalkOptions struct {
	Lexicographically bool
}

type WalkOption func(*WalkOptions)

func Lexicographically(l bool) WalkOption {
	return func(args *WalkOptions) {
		args.Lexicographically = l
	}
}

// WalkDirLexicographically works the same as filepath.WalkDir except that it appends a slash to
// directories when sorting entries so that paths are walked in lexicographical order. This is
// notably required when you need to walk paths in the same order they are inserted into BadgerDB.
//
// Specifically the built in filepath.WalkDir will return paths like:
//
//	/arm/rockchip <<< Directory!
//	/arm/rockchip/pmu.yaml
//	/arm/rockchip.yaml
//	/arm/rtsm-dcscb.txt
//
// But these same paths would be in the following order in BadgerDB:
//
//	/arm/rockchip.yaml
//	/arm/rockchip/pmu.yaml
//	/arm/rtsm-dcscb.txt
//
// The issue is a period (.) is Unicode character 46 and a slash (/) is Unicode character 47 meaning
// BadgerDB sorts rockchip.yaml ahead of files that share the same prefix like rockchip/pmu.yaml.
func WalkDirLexicographically(root string, fn fs.WalkDirFunc) error {
	// Get the FileInfo for root to check for errors.
	info, err := os.Lstat(root)
	if err != nil {
		return fn(root, nil, err)
	} else {
		err = walkDirLexicographically(root, fs.FileInfoToDirEntry(info), fn)
	}
	if err == filepath.SkipDir || err == filepath.SkipAll {
		return nil
	}
	return err
}

// walkDirLexicographically is the recursive helper for WalkDirLexicographically.
func walkDirLexicographically(path string, d fs.DirEntry, walkDirFn fs.WalkDirFunc) error {
	if err := walkDirFn(path, d, nil); err != nil || !d.IsDir() {
		if err == filepath.SkipDir && d.IsDir() {
			// Successfully skipped directory.
			err = nil
		}
		return err
	}

	// The way the directory is opened and the additional sorting are only changes from the standard
	// library's filepath.walkDir function.

	// Instead of using the free standing os.ReadDir() which accepts a directory name then handles
	// sorting, open the directory ourselves and call its ReadDir method to avoid duplicate sorting.
	fd, err := os.Open(path)
	if err != nil {
		// Second call to report the os.Open error.
		err = walkDirFn(path, d, err)
		if err != nil {
			if err == filepath.SkipDir && d.IsDir() {
				err = nil
			}
			return err
		}
	}
	dirs, err := fd.ReadDir(-1)
	fd.Close()
	if err != nil {
		// Second call, to report the ReadDir error.
		err = walkDirFn(path, d, err)
		if err != nil {
			if err == filepath.SkipDir && d.IsDir() {
				err = nil
			}
			return err
		}
	}

	slices.SortFunc(dirs, func(a, b os.DirEntry) int {
		nameA := a.Name()
		nameB := b.Name()
		if a.IsDir() {
			nameA += "/"
		}
		if b.IsDir() {
			nameB += "/"
		}
		if nameA < nameB {
			return -1
		} else if nameA == nameB {
			return 0
		}
		return 1
	})

	for _, d1 := range dirs {
		path1 := filepath.Join(path, d1.Name())
		if err := walkDirLexicographically(path1, d1, walkDirFn); err != nil {
			// If we’re skipping a directory, continue to the next.
			if err == fs.SkipDir {
				break
			}
			return err
		}
	}
	return nil
}
