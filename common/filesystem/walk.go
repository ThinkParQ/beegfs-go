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
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	doublestar "github.com/bmatcuk/doublestar/v4"
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

	dirs, err := readDir("", path, "")
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

var ErrStreamPathLimitReached = errors.New("stream limit was reached")

// Sentinel StreamPathLimitError error which signals the processor that the maximum number of paths had been
// reached. The error also contains the ResumeToken.
type StreamPathLimitError struct {
	ResumeToken string
}

func (e StreamPathLimitError) Error() string {
	return fmt.Errorf("%s: %w", e.ResumeToken, ErrStreamPathLimitReached).Error()
}

func (e StreamPathLimitError) Unwrap() error {
	return ErrStreamPathLimitReached
}

type StreamPathResult struct {
	Path string
	Err  error
}

// StreamPathsLexicographically returns a *StreamPathResult channel that returns the pattern's paths in a
// lexicographically increasing order. If startAfter != "" then only files lexically greater than
// will be considered. maxPaths limits the number of paths returned and can be set to -1 for all
// paths. chanSize is the buffer size for the returned *StreamPathResult channel.
func StreamPathsLexicographically(ctx context.Context, mountPoint Provider, pattern string, startAfter string, maxPaths int, chanSize int) (<-chan *StreamPathResult, error) {
	if maxPaths != -1 && maxPaths <= 0 {
		return nil, fmt.Errorf("maxPaths must be greater than zero or -1")
	}

	preparePath := func(path string) string {
		path = strings.TrimLeft(path, "/")
		path = filepath.Clean("/" + path)
		path = strings.TrimPrefix(path, "/")
		return strings.TrimRight(path, "/")
	}
	pattern = preparePath(pattern)
	startAfter = preparePath(startAfter)

	isGlob := IsGlobPattern(pattern)
	if !isGlob {
		if stat, err := mountPoint.Lstat(pattern); err != nil {
			return nil, fmt.Errorf("unable walk path: %w", err)
		} else if !stat.IsDir() {
			// prefix is a file path so only stream it back if it's a match.
			walkChan := make(chan *StreamPathResult, 1)
			go func() {
				defer close(walkChan)
				if pattern > startAfter {
					select {
					case <-ctx.Done():
						walkChan <- &StreamPathResult{Err: ctx.Err()}
					case walkChan <- &StreamPathResult{Path: "/" + pattern}:
					}
				}
			}()
			return walkChan, nil
		}
	} else {
		// Adding '/**' forces doublestar to match the entire path (only whole directories, never
		// partial filenames) rather than just the prefix used to pick the walk root.
		pattern += "/**"
	}

	// Recursively walk the local path and send matching paths to walkChan. Any encountered errors
	// will terminate the walk. Paths lexicographically less than or equal to startAfter will be
	// ignored which also avoids walking directories unnecessarily.
	mountPath := mountPoint.GetMountPath()
	walkChan := make(chan *StreamPathResult, chanSize)
	go func() {
		defer close(walkChan)

		var walkDir func(string) bool
		walkDir = func(directory string) bool {
			if err := ctx.Err(); err != nil {
				walkChan <- &StreamPathResult{Err: err}
				return false
			}

			entries, err := readDir(mountPath, directory, startAfter)
			if err != nil {
				walkChan <- &StreamPathResult{Err: fmt.Errorf("unable to read directory, %q: %w", directory, err)}
				return false
			}

			lastPath := directory
			for _, entry := range entries {
				path := filepath.Join(directory, entry.Name())
				if entry.IsDir() {
					if !walkDir(path) {
						return false
					}
					continue
				} else if path <= startAfter {
					continue
				}

				if isGlob {
					if match, err := doublestar.Match(pattern, path); err != nil {
						walkChan <- &StreamPathResult{Err: err}
						return false
					} else if !match {
						continue
					}
				}

				if maxPaths == 0 {
					walkChan <- &StreamPathResult{Err: StreamPathLimitError{lastPath}}
					return false
				}

				select {
				case <-ctx.Done():
					walkChan <- &StreamPathResult{Err: ctx.Err()}
					return false

				case walkChan <- &StreamPathResult{Path: "/" + path}:
				}
				lastPath = path
				maxPaths--
			}

			return true
		}

		root := pattern
		if isGlob {
			// Find the root directory of the file glob by stepping back until a directory is found.
			for {
				if _, err := mountPoint.Lstat(root); err == nil {
					break
				}
				root = filepath.Dir(root)
			}
		}
		walkDir(root)
	}()

	return walkChan, nil
}

// readDir returns a lexically sorted directory list of files that come after startAfter. It should
// be used instead of fs.ReadDir/os.ReadDir to avoid their non-lexical path sort.
func readDir(root string, directory string, startAfter string) (entries []os.DirEntry, err error) {
	var f *os.File
	if f, err = os.Open(filepath.Join(root, directory)); err != nil {
		return
	}
	defer f.Close()

	// Directories receive a trailing '/' so they sort distinctly from files with the same prefix.
	// Without the '/', a directory sortName may sort incorrectly relative to files prefixed with the
	// same sortName.
	sortName := func(entry os.DirEntry) string {
		if entry.IsDir() {
			return entry.Name() + "/"
		}
		return entry.Name()
	}

	if entries, err = f.ReadDir(-1); err != nil {
		return
	}

	// Filter any entries that come after startAfter in-place so they are not part of the sorting
	if startAfter != "" && strings.HasPrefix(startAfter, directory+"/") {
		relative := strings.TrimPrefix(startAfter, directory)
		relative = strings.TrimLeft(relative, "/")
		filtered := entries[:0]
		for _, entry := range entries {
			name := sortName(entry)
			if name > relative || strings.HasPrefix(relative, name) {
				filtered = append(filtered, entry)
			}
		}
		entries = filtered
	}

	sort.Slice(entries, func(i, j int) bool {
		return sortName(entries[i]) < sortName(entries[j])
	})
	return
}

const globCharacters = "*?["

// IsGlobPattern returns whether the pattern contains a glob pattern.
func IsGlobPattern(pattern string) bool {
	return strings.ContainsAny(pattern, globCharacters)
}

// StripGlobPattern extracts the longest leading substring from the given pattern that contains
// no glob characters (e.g., '*', '?', or '['). This base prefix is used to efficiently list
// objects in an S3 bucket, while the original glob pattern is later applied to filter the results.
func StripGlobPattern(pattern string) string {
	position := 0
	for {
		index := strings.IndexAny(pattern[position:], globCharacters)
		if index == -1 {
			return pattern
		}
		candidate := position + index

		// Check for escape characters
		backslashCount := 0
		for i := candidate - 1; i >= 0 && pattern[i] == '\\'; i-- {
			backslashCount++
		}
		if backslashCount%2 == 0 {
			return pattern[:candidate]
		}

		// Check whether the last character was escaped
		position = candidate + 1
		if position >= len(pattern) {
			return pattern
		}
	}
}
