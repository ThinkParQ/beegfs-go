package index

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

func newGenericStatCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.MinimumNArgs(0),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}

			paths := args
			if len(paths) == 0 {
				cwd, err := os.Getwd()
				if err != nil {
					return err
				}
				paths = []string{cwd}
			}

			useBeeGFS, _ := cmd.Flags().GetBool("beegfs")
			if useBeeGFS {
				return runBeegfsStat(paths)
			}

			return runPythonStatIndex(bflagSet, paths)
		},
	}
	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("format", "c", "Use the specified FORMAT instead of the default; output a newline after each use of FORMAT", "-c", ""),
		bflag.Flag("terse", "t", "Print the information in terse form", "-t", false),
		bflag.Flag("verbose", "V", "Show the query being executed", "--verbose", false),
		bflag.Flag("version", "", "Show program's version number and exit", "--version", false),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)
	cmd.Flags().BoolP("beegfs", "b", false, "Include BeeGFS entry information")

	return cmd
}

func newStatCmd() *cobra.Command {
	s := newGenericStatCmd()
	s.Use = "stat"
	s.Short = "Displays file or directory metadata information."

	s.Long = `Retrieve metadata information for files and directories.

This command displays detailed status information, similar to the standard "stat" command,
including metadata attributes for files and directories.

Example: Display the status of a file

$ beegfs index stat README
`
	return s
}

func runPythonStatIndex(bflagSet *bflag.FlagSet, paths []string) error {
	log, _ := config.GetLogger()
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+len(paths))
	allArgs = append(allArgs, wrappedArgs...)
	allArgs = append(allArgs, paths...)
	log.Debug("Running BeeGFS Hive Index stat command",
		zap.Any("wrappedArgs", wrappedArgs),
		zap.Any("paths", paths),
		zap.Any("allArgs", allArgs),
	)
	cmd := exec.Command(statBinary, allArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("unable to start index command: %w", err)
	}
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("error executing index command: %w", err)
	}
	return nil
}

type beegfsStatEntry struct {
	entryType         int
	entryID           string
	parentID          string
	ownerID           string
	stripePatternType int
	chunkSize         int64
	numTargets        int
	targetIDs         []string
}

func runBeegfsStat(paths []string) error {
	log, _ := config.GetLogger()
	if _, err := loadIndexConfig(); err != nil {
		log.Debug("unable to load BeeGFS index config", zap.Error(err))
	}

	if _, err := os.Stat(sqlite3Binary); err != nil {
		return fmt.Errorf("BeeGFS metadata stat requires gufi_sqlite3 at %s: %w", sqlite3Binary, err)
	}

	for i, p := range paths {
		if i > 0 {
			fmt.Println()
		}

		entry, err := beegfsStatForPath(p, log)
		if err != nil {
			return err
		}

		fmt.Print(formatBeegfsStat(entry))
	}

	return nil
}

func beegfsStatForPath(p string, log *logger.Logger) (beegfsStatEntry, error) {
	dbPath, entryName, err := resolveBeeStatTarget(p)
	if err != nil {
		return beegfsStatEntry{}, err
	}

	entry, err := fetchBeegfsStatEntry(dbPath, entryName, log)
	if err != nil && entryName != "" {
		// fallback to directory entry when file name not found
		entry, err = fetchBeegfsStatEntry(dbPath, "", log)
	}
	if err != nil {
		return beegfsStatEntry{}, fmt.Errorf("fetching BeeGFS metadata for %s: %w", p, err)
	}

	return entry, nil
}

func resolveBeeStatTarget(p string) (string, string, error) {
	clean := filepath.Clean(p)
	dbInPath := filepath.Join(clean, "db.db")
	if info, err := os.Stat(dbInPath); err == nil && !info.IsDir() {
		return dbInPath, "", nil
	}

	parent := filepath.Dir(clean)
	if parent == "" || parent == clean {
		return "", "", fmt.Errorf("BeeGFS index database not found for %s", p)
	}

	dbInParent := filepath.Join(parent, "db.db")
	if info, err := os.Stat(dbInParent); err == nil && !info.IsDir() {
		return dbInParent, filepath.Base(clean), nil
	}

	return "", "", fmt.Errorf("BeeGFS index database not found for %s", p)
}

func fetchBeegfsStatEntry(dbPath, entryName string, log *logger.Logger) (beegfsStatEntry, error) {
	escapedName := strings.ReplaceAll(entryName, "'", "''")
	query := fmt.Sprintf("SELECT entryID, parentID, ownerID, entryType, stripe_pattern_type, chunk_size, num_targets FROM beegfs_entryinfo WHERE name='%s';", escapedName)
	args := []string{"-d", "|", dbPath, query}

	log.Debug("querying BeeGFS entryinfo for stat",
		zap.String("dbPath", dbPath),
		zap.String("entryName", entryName),
		zap.String("query", query),
	)

	out, err := exec.Command(sqlite3Binary, args...).CombinedOutput()
	if err != nil {
		return beegfsStatEntry{}, fmt.Errorf("running beegfs stat query: %w (output: %s)", err, bytes.TrimSpace(out))
	}

	line := strings.TrimSpace(string(out))
	if line == "" {
		return beegfsStatEntry{}, fmt.Errorf("entry %q not found in %s", entryName, dbPath)
	}

	fields := strings.Split(line, "|")
	if len(fields) < 7 {
		return beegfsStatEntry{}, fmt.Errorf("unexpected entryinfo format for %q in %s", entryName, dbPath)
	}

	entryID := strings.TrimSpace(fields[0])
	parentID := strings.TrimSpace(fields[1])
	ownerID := strings.TrimSpace(fields[2])
	entryType := safeAtoi(strings.TrimSpace(fields[3]))
	stripeType := safeAtoi(strings.TrimSpace(fields[4]))
	chunkSize := safeAtoi64(strings.TrimSpace(fields[5]))
	numTargets := safeAtoi(strings.TrimSpace(fields[6]))

	targetIDs, err := fetchStripeTargets(dbPath, entryID, log)
	if err != nil {
		return beegfsStatEntry{}, err
	}

	return beegfsStatEntry{
		entryType:         entryType,
		entryID:           entryID,
		parentID:          parentID,
		ownerID:           ownerID,
		stripePatternType: stripeType,
		chunkSize:         chunkSize,
		numTargets:        numTargets,
		targetIDs:         targetIDs,
	}, nil
}

func fetchStripeTargets(dbPath, entryID string, log *logger.Logger) ([]string, error) {
	if entryID == "" {
		return nil, nil
	}

	query := fmt.Sprintf("SELECT target_id FROM beegfs_stripes WHERE entryID='%s' ORDER BY ordinal;", strings.ReplaceAll(entryID, "'", "''"))
	args := []string{"-d", "|", dbPath, query}

	log.Debug("querying BeeGFS stripe targets for stat",
		zap.String("dbPath", dbPath),
		zap.String("entryID", entryID),
		zap.String("query", query),
	)

	out, err := exec.Command(sqlite3Binary, args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("running beegfs stripe query: %w (output: %s)", err, bytes.TrimSpace(out))
	}

	text := strings.TrimSpace(string(out))
	if text == "" {
		return nil, nil
	}

	lines := strings.Split(text, "\n")
	targets := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		targets = append(targets, line)
	}
	return targets, nil
}

func formatBeegfsStat(entry beegfsStatEntry) string {
	var b strings.Builder

	fmt.Fprintf(&b, "Entry type: %s\n", entryTypeString(entry.entryType))
	fmt.Fprintf(&b, "EntryID: %s\n", valueOrDefault(entry.entryID, "0"))
	parent := valueOrDefault(entry.parentID, "root")
	fmt.Fprintf(&b, "ParentID: %s\n", parent)
	fmt.Fprintf(&b, "Metadata Owner ID: %s\n", valueOrDefault(entry.ownerID, "0"))
	fmt.Fprintf(&b, "Stripe pattern details:\n")
	fmt.Fprintf(&b, "+ Type: %s\n", stripePatternString(entry.stripePatternType))
	fmt.Fprintf(&b, "+ Chunksize: %d\n", entry.chunkSize)
	fmt.Fprintf(&b, "+ Number of storage targets: %d\n", entry.numTargets)
	for _, t := range entry.targetIDs {
		fmt.Fprintf(&b, "  + Target ID %s\n", t)
	}

	return b.String()
}

func entryTypeString(t int) string {
	switch t {
	case 1:
		return "directory"
	case 2:
		return "file"
	case 3:
		return "symlink"
	default:
		return fmt.Sprintf("unknown(%d)", t)
	}
}

func stripePatternString(t int) string {
	switch t {
	case 1:
		return "RAID0"
	default:
		return fmt.Sprintf("Unknown(%d)", t)
	}
}
