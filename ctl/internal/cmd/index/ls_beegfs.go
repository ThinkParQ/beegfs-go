package index

import (
	"bufio"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
)

type beegfsEntryInfo struct {
	EntryID           string
	Name              string
	ParentID          string
	OwnerID           string
	EntryType         string
	FeatureFlag       string
	StripePatternType string
	ChunkSize         string
	NumTargets        string
	PINode            string
}

type beegfsMetaIndex struct {
	byDir  map[string]map[string]beegfsEntryInfo
	byPath map[string]beegfsEntryInfo
}

const beegfsPrefixFieldCount = 5

func newBeeGFSMetadataIndex() *beegfsMetaIndex {
	return &beegfsMetaIndex{
		byDir:  make(map[string]map[string]beegfsEntryInfo),
		byPath: make(map[string]beegfsEntryInfo),
	}
}

func fetchBeeGFSMetadata(backend indexBackend, paths []string, delim string, recursive bool) (*beegfsMetaIndex, error) {
	meta := newBeeGFSMetadataIndex()
	for _, path := range paths {
		if recursive {
			args := []string{"-d", delim, "-E", "SELECT path() AS dir, * FROM beegfs_entryinfo", path}
			if err := runIndexCommand(backend, queryBinary, args, func(r io.Reader) error {
				return parseBeeGFSMetadataReader(r, delim, meta, "", true)
			}); err != nil {
				return nil, err
			}
			continue
		}
		args := []string{"-d", delim, "--max-level", "0", "-E", "SELECT * FROM beegfs_entryinfo", path}
		if err := runIndexCommand(backend, queryBinary, args, func(r io.Reader) error {
			return parseBeeGFSMetadataReader(r, delim, meta, path, false)
		}); err != nil {
			return nil, err
		}
	}
	return meta, nil
}

func parseBeeGFSMetadataReader(r io.Reader, delim string, meta *beegfsMetaIndex, dir string, hasDirColumn bool) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var parseErr error
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		cols := strings.Split(line, delim)
		base := 0
		currentDir := dir
		if hasDirColumn {
			if len(cols) < 11 {
				if parseErr == nil {
					parseErr = fmt.Errorf("unexpected BeeGFS metadata output: %q", line)
				}
				continue
			}
			currentDir = cols[0]
			base = 1
		} else if len(cols) < 10 {
			if parseErr == nil {
				parseErr = fmt.Errorf("unexpected BeeGFS metadata output: %q", line)
			}
			continue
		}
		info := beegfsEntryInfo{
			EntryID:           cols[base],
			Name:              cols[base+1],
			ParentID:          cols[base+2],
			OwnerID:           cols[base+3],
			EntryType:         cols[base+4],
			FeatureFlag:       cols[base+5],
			StripePatternType: cols[base+6],
			ChunkSize:         cols[base+7],
			NumTargets:        cols[base+8],
			PINode:            cols[base+9],
		}
		meta.add(currentDir, info)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return parseErr
}

func (m *beegfsMetaIndex) add(dir string, info beegfsEntryInfo) {
	if dir == "" {
		return
	}
	if info.Name == "" {
		m.byPath[dir] = info
		return
	}
	dirMap := m.byDir[dir]
	if dirMap == nil {
		dirMap = make(map[string]beegfsEntryInfo)
		m.byDir[dir] = dirMap
	}
	dirMap[info.Name] = info
	m.byPath[filepath.Join(dir, info.Name)] = info
}

func mergeBeeGFSLsOutput(r io.Reader, meta *beegfsMetaIndex, delim string, paths []string, tbl *cmdfmt.Printomatic) error {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	sanitize := shouldSanitizeIndexJSON()

	currentDir := ""
	if len(paths) == 1 {
		currentDir = paths[0]
	}

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			tbl.AddItem("")
			continue
		}
		if strings.HasSuffix(line, ":") {
			dir := strings.TrimSuffix(line, ":")
			currentDir = dir
			tbl.AddItem(sanitizeIndexLine(line, sanitize))
			continue
		}

		info, ok := resolveBeeGFSInfo(line, currentDir, delim, meta)
		if prefix := beegfsPrefix(line, delim, info, ok); prefix != "" {
			line = prefix + delim + line
		}
		tbl.AddItem(sanitizeIndexLine(line, sanitize))
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	tbl.PrintRemaining()
	return nil
}

func beegfsPrefix(line, delim string, info beegfsEntryInfo, hasInfo bool) string {
	if delim == "" {
		return ""
	}
	if !hasInfo {
		if looksLikeLongListing(line) {
			return zeroBeeGFSPrefix(delim)
		}
		return ""
	}
	if info.EntryType == "1" {
		return zeroBeeGFSPrefix(delim)
	}
	fields := []string{
		info.EntryID,
		info.OwnerID,
		info.StripePatternType,
		info.ChunkSize,
		info.NumTargets,
	}
	return strings.Join(fields, delim)
}

func looksLikeLongListing(line string) bool {
	if len(line) < 10 {
		return false
	}
	switch line[0] {
	case 'd', '-', 'l':
		return true
	default:
		return false
	}
}

func zeroBeeGFSPrefix(delim string) string {
	zeros := make([]string, beegfsPrefixFieldCount)
	for i := range zeros {
		zeros[i] = "0"
	}
	return strings.Join(zeros, delim)
}

func resolveBeeGFSInfo(line, currentDir, delim string, meta *beegfsMetaIndex) (beegfsEntryInfo, bool) {
	parts := splitWithDelim(line, delim)
	if currentDir != "" {
		if dirMap, ok := meta.byDir[currentDir]; ok {
			if info, ok := findMatchingEntry(parts, delim, dirMap, nil); ok {
				return info, true
			}
		}
	}
	return findMatchingEntry(parts, delim, nil, meta.byPath)
}

func findMatchingEntry(parts []string, delim string, byName map[string]beegfsEntryInfo, byPath map[string]beegfsEntryInfo) (beegfsEntryInfo, bool) {
	for i := 0; i < len(parts); i++ {
		candidate := strings.Join(parts[i:], delim)
		if byName != nil {
			if info, ok := byName[candidate]; ok {
				return info, true
			}
			if strings.HasSuffix(candidate, "/") {
				if info, ok := byName[strings.TrimSuffix(candidate, "/")]; ok {
					return info, true
				}
			}
		}
		if byPath != nil {
			if info, ok := byPath[candidate]; ok {
				return info, true
			}
			if strings.HasSuffix(candidate, "/") {
				if info, ok := byPath[strings.TrimSuffix(candidate, "/")]; ok {
					return info, true
				}
			}
		}
	}
	return beegfsEntryInfo{}, false
}

func splitWithDelim(line, delim string) []string {
	if delim == "" {
		return []string{line}
	}
	return strings.Split(line, delim)
}
