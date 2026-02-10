package index

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
)

const beegfsStatDelim = "\t"

type beegfsStripeInfo struct {
	Ordinal  int
	TargetID string
}

func runBeeGFSStatIndex(backend indexBackend, paths []string, verbose bool) error {
	tbl := newIndexLinePrintomatic("line")
	sanitize := shouldSanitizeIndexJSON()

	for i, path := range paths {
		if i > 0 {
			tbl.AddItem("")
		}
		indexPath, err := indexPathFromRelative(path)
		if err != nil {
			return err
		}
		info, stripes, err := fetchBeeGFSStatInfo(backend, indexPath, verbose)
		if err != nil {
			return err
		}
		for _, line := range formatBeeGFSStatOutput(path, info, stripes) {
			tbl.AddItem(sanitizeIndexLine(line, sanitize))
		}
	}
	tbl.PrintRemaining()
	return nil
}

func fetchBeeGFSStatInfo(backend indexBackend, path string, verbose bool) (beegfsEntryInfo, []beegfsStripeInfo, error) {
	dbPath, entryName := resolveBeeGFSStatQuery(path, backend)
	info, found, err := queryBeeGFSStatEntryInfo(backend, dbPath, entryName, verbose)
	if err != nil {
		return beegfsEntryInfo{}, nil, err
	}
	if !found && entryName != "" {
		altPath := filepath.Clean(path)
		altInfo, altFound, altErr := queryBeeGFSStatEntryInfo(backend, altPath, "", verbose)
		if altErr != nil {
			return beegfsEntryInfo{}, nil, altErr
		}
		if altFound {
			dbPath = altPath
			info = altInfo
			found = true
		}
	}
	if !found {
		return beegfsEntryInfo{}, nil, fmt.Errorf("no BeeGFS metadata found for %q", path)
	}
	stripes, err := queryBeeGFSStatStripes(backend, dbPath, info.EntryID, verbose)
	if err != nil {
		return beegfsEntryInfo{}, nil, err
	}
	return info, stripes, nil
}

func resolveBeeGFSStatQuery(path string, backend indexBackend) (string, string) {
	cleanPath := filepath.Clean(path)
	if cleanPath == "." || cleanPath == string(filepath.Separator) {
		return cleanPath, ""
	}
	if backend.isLocal() {
		if info, err := os.Stat(cleanPath); err == nil && info.IsDir() {
			return cleanPath, ""
		}
	}
	return filepath.Dir(cleanPath), filepath.Base(cleanPath)
}

func queryBeeGFSStatEntryInfo(backend indexBackend, dbPath, entryName string, verbose bool) (beegfsEntryInfo, bool, error) {
	whereClause := "name = ''"
	if entryName != "" {
		whereClause = fmt.Sprintf("name = '%s'", escapeSQLString(entryName))
	}
	query := fmt.Sprintf("SELECT * FROM beegfs_entryinfo WHERE %s", whereClause)
	args := []string{"-d", beegfsStatDelim, "--max-level", "0", "-E", query, dbPath}

	var info beegfsEntryInfo
	found := false
	err := runBeeGFSStatQuery(backend, args, verbose, func(r io.Reader) error {
		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
		var parseErr error
		for scanner.Scan() {
			line := scanner.Text()
			if line == "" {
				continue
			}
			cols := strings.Split(line, beegfsStatDelim)
			if len(cols) < 10 {
				if parseErr == nil {
					parseErr = fmt.Errorf("unexpected BeeGFS metadata output: %q", line)
				}
				continue
			}
			if !found {
				info = beegfsEntryInfo{
					EntryID:           cols[0],
					Name:              cols[1],
					ParentID:          cols[2],
					OwnerID:           cols[3],
					EntryType:         cols[4],
					FeatureFlag:       cols[5],
					StripePatternType: cols[6],
					ChunkSize:         cols[7],
					NumTargets:        cols[8],
					PINode:            cols[9],
				}
				found = true
			}
		}
		if err := scanner.Err(); err != nil {
			return err
		}
		return parseErr
	})
	if err != nil {
		return beegfsEntryInfo{}, false, err
	}
	return info, found, nil
}

func queryBeeGFSStatStripes(backend indexBackend, dbPath, entryID string, verbose bool) ([]beegfsStripeInfo, error) {
	if entryID == "" {
		return nil, nil
	}
	query := fmt.Sprintf("SELECT * FROM beegfs_stripes WHERE entryID = '%s' ORDER BY ordinal", escapeSQLString(entryID))
	args := []string{"-d", beegfsStatDelim, "--max-level", "0", "-E", query, dbPath}

	var stripes []beegfsStripeInfo
	err := runBeeGFSStatQuery(backend, args, verbose, func(r io.Reader) error {
		parsed, err := parseBeeGFSStripesReader(r, beegfsStatDelim)
		if err != nil {
			return err
		}
		stripes = parsed
		return nil
	})
	if err != nil {
		return nil, err
	}
	return stripes, nil
}

func runBeeGFSStatQuery(backend indexBackend, args []string, verbose bool, handle func(io.Reader) error) error {
	if verbose {
		cmd, err := buildIndexCommand(backend, queryBinary, args)
		if err != nil {
			cmdfmt.Printf("%s %s\n", queryBinary, strings.Join(args, " "))
		} else {
			cmdfmt.Printf("%s\n", strings.Join(cmd.Args, " "))
		}
	}
	return runIndexCommand(backend, queryBinary, args, handle)
}

func parseBeeGFSStripesReader(r io.Reader, delim string) ([]beegfsStripeInfo, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	var stripes []beegfsStripeInfo
	var parseErr error
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		cols := strings.Split(line, delim)
		if len(cols) < 5 {
			if parseErr == nil {
				parseErr = fmt.Errorf("unexpected BeeGFS stripe output: %q", line)
			}
			continue
		}
		ordinal, err := strconv.Atoi(cols[1])
		if err != nil {
			if parseErr == nil {
				parseErr = fmt.Errorf("unexpected BeeGFS stripe ordinal %q", cols[1])
			}
			continue
		}
		stripes = append(stripes, beegfsStripeInfo{
			Ordinal:  ordinal,
			TargetID: cols[2],
		})
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if parseErr != nil {
		return stripes, parseErr
	}
	sort.Slice(stripes, func(i, j int) bool {
		return stripes[i].Ordinal < stripes[j].Ordinal
	})
	return stripes, nil
}

func formatBeeGFSStatOutput(path string, info beegfsEntryInfo, stripes []beegfsStripeInfo) []string {
	entryType, entryTypeOK := parseBeeGFSStatEntryType(info.EntryType)
	entryTypeLabel := info.EntryType
	if entryTypeOK {
		entryTypeLabel = entryType.String()
	}
	patternLabel := formatBeeGFSStatStripePattern(info.StripePatternType)

	lines := []string{
		fmt.Sprintf("path: %s", path),
		fmt.Sprintf("Entry type: %s", entryTypeLabel),
		fmt.Sprintf("EntryID: %s", info.EntryID),
		fmt.Sprintf("ParentID: %s", info.ParentID),
		fmt.Sprintf("Metadata Owner ID: %s", info.OwnerID),
	}

	showStripeDetails := (entryTypeOK && entryType.IsFile()) || len(stripes) > 0
	if showStripeDetails {
		chunkSize := info.ChunkSize
		numTargets := info.NumTargets
		if numTargets == "" && len(stripes) > 0 {
			numTargets = strconv.Itoa(len(stripes))
		}
		lines = append(lines, "Stripe pattern details:")
		if patternLabel != "" {
			lines = append(lines, fmt.Sprintf("+ Type: %s", patternLabel))
		}
		if chunkSize != "" {
			lines = append(lines, fmt.Sprintf("+ Chunksize: %s", chunkSize))
		}
		if numTargets != "" {
			lines = append(lines, fmt.Sprintf("+ Number of storage targets: %s", numTargets))
		}
		for _, stripe := range stripes {
			lines = append(lines, fmt.Sprintf(" + Target ID %s", stripe.TargetID))
		}
	}
	return lines
}

func parseBeeGFSStatEntryType(value string) (beegfs.EntryType, bool) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return beegfs.EntryUnknown, false
	}
	return beegfs.EntryType(parsed), true
}

func formatBeeGFSStatStripePattern(value string) string {
	if value == "" {
		return ""
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return value
	}
	return beegfs.StripePatternType(parsed).String()
}

func escapeSQLString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
