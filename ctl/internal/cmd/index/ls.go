package index

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

func newGenericLsCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
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
				return runBeegfsLs(cmd, bflagSet, paths)
			}
			return runPythonLsIndex(bflagSet, paths)
		},
	}

	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("all", "a", "Do not ignore entries starting with .", "-a", false),
		bflag.Flag("almost-all", "A", "Do not list implied . and ..", "-A", false),
		bflag.Flag("block-size", "", "With -l, scale sizes by SIZE when printing them", "--block-size", ""),
		bflag.Flag("ignore-backups", "B", "Do not list implied entries ending with ~", "--ignore-backups", false),
		bflag.Flag("full-time", "", "Like -l --time-style=full-iso", "--full-time", false),
		bflag.Flag("no-group", "G", "In a long listing, don't print group names", "--no-group", false),
		bflag.Flag("human-readable", "h", "With -l and -s, print sizes like 1K 234M 2G etc.", "-h", false),
		bflag.Flag("inode", "i", "Print the index number of each file", "-i", false),
		bflag.Flag("long-listing", "l", "Use a long listing format", "-l", false),
		bflag.Flag("reverse", "r", "Reverse order while sorting", "-r", false),
		bflag.Flag("recursive", "R", "List subdirectories recursively", "-R", false),
		bflag.Flag("size", "s", "Print the allocated size of each file, in blocks", "-s", false),
		bflag.Flag("sort-largest", "S", "Sort by file size, largest first", "-S", false),
		bflag.Flag("version", "v", "BeeGFS Hive Index Version", "--version", false),
		bflag.Flag("time-style", "", "Time/date format with -l", "--time-style", ""),
		bflag.Flag("mtime", "t", "Sort by modification time, newest first", "-t", false),
		bflag.Flag("delim", "", "Delimiter separating output columns", "--delim", " "),
		bflag.Flag("in-memory-name", "", "In-memory name", "--in-memory-name", "out"),
		bflag.Flag("aggregate-name", "", "Name of final database when aggregation is performed", "--aggregate-name", ""),
		bflag.Flag("skip-file", "", "Name of file containing directory basenames to skip", "--skip-file", ""),
		bflag.Flag("verbose", "V", "Show the gufi_query being executed", "--verbose", false),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)
	cmd.Flags().BoolP("beegfs", "b", false, "Include BeeGFS entry information")
	cmd.PersistentFlags().BoolP("help", "", false, "Help for ls")
	cmd.Flags().MarkHidden("in-memory-name")
	return cmd
}

func newLsCmd() *cobra.Command {
	s := newGenericLsCmd()
	s.Use = "ls"
	s.Short = "Lists the contents of the index directory."

	s.Long = `Displays the contents of the index directory.

This command works similarly to the standard "ls" command, supporting both absolute and relative paths. 
You can use it from within the index directory or from a filesystem directory when specifying relative paths.

Example: List the contents of the index directory at /mnt/index.

$ beegfs index ls /mnt/index
`
	return s
}

func runPythonLsIndex(bflagSet *bflag.FlagSet, paths []string) error {
	log, _ := config.GetLogger()
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+len(paths)+2)
	allArgs = append(allArgs, paths...)
	allArgs = append(allArgs, wrappedArgs...)
	outputFormat := viper.GetString(config.OutputKey)
	if outputFormat != "" && outputFormat != config.OutputTable.String() {
		allArgs = append(allArgs, "-Q", outputFormat)
	}
	log.Debug("Running BeeGFS Hive Index ls command",
		zap.Any("wrappedArgs", wrappedArgs),
		zap.Any("lsBinary", lsBinary),
		zap.Any("paths", paths),
		zap.Any("allArgs", allArgs),
	)
	cmd := exec.Command(lsBinary, allArgs...)

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

type beegfsEntryInfo struct {
	entryID           string
	owner             string
	stripePatternType int
	chunkSize         int64
	numberOfTargets   int
	hasBeegfsMetadata bool
}

func runBeegfsLs(cmd *cobra.Command, bflagSet *bflag.FlagSet, paths []string) error {
	log, _ := config.GetLogger()
	cfg, _ := loadIndexConfig()
	if cfg == nil {
		cfg = &gufiConfig{}
	}
	log.Debug("resolved BeeGFS index config", zap.String("indexRoot", cfg.IndexRoot))

	if _, err := os.Stat(sqlite3Binary); err != nil {
		return fmt.Errorf("BeeGFS metadata listing requires gufi_sqlite3 at %s: %w", sqlite3Binary, err)
	}

	longListing, _ := cmd.Flags().GetBool("long-listing")
	delim, _ := cmd.Flags().GetString("delim")
	if delim == "" {
		delim = " "
	}

	wrappedArgs := bflagSet.WrappedArgs()
	lsPaths := make([]string, 0, len(paths))
	for _, p := range paths {
		rel, _ := normalizeIndexPath(p, cfg.IndexRoot)
		lsPaths = append(lsPaths, rel)
	}

	allArgs := make([]string, 0, len(lsPaths)+len(wrappedArgs))
	allArgs = append(allArgs, lsPaths...)
	allArgs = append(allArgs, wrappedArgs...)

	log.Debug("Running BeeGFS Hive Index ls (beegfs mode)",
		zap.Any("paths", lsPaths),
		zap.Any("wrappedArgs", wrappedArgs),
		zap.String("lsBinary", lsBinary),
	)

	cmdExec := exec.Command(lsBinary, allArgs...)
	out, err := cmdExec.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing ls: %w (output: %s)", err, bytes.TrimSpace(out))
	}

	return augmentAndPrintLsOutput(out, longListing, delim, lsPaths, cfg.IndexRoot, log)
}

func fetchBeegfsInfo(dbPath string, log *logger.Logger) (map[string]beegfsEntryInfo, error) {
	if _, err := os.Stat(dbPath); err != nil {
		return nil, fmt.Errorf("BeeGFS index database not found at %s: %w", dbPath, err)
	}

	query := "SELECT name, entryID, ownerID, stripe_pattern_type, chunk_size, num_targets FROM beegfs_entryinfo;"
	args := []string{"-d", "|", dbPath, query}

	log.Debug("querying BeeGFS metadata for ls",
		zap.String("dbPath", dbPath),
		zap.String("sqlite3Binary", sqlite3Binary),
		zap.String("query", query),
	)

	out, err := exec.Command(sqlite3Binary, args...).CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("running beegfs query via %s: %w (output: %s)", sqlite3Binary, err, bytes.TrimSpace(out))
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	result := make(map[string]beegfsEntryInfo, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, "|")
		if len(fields) < 6 {
			continue
		}

		name := strings.TrimSpace(fields[0])
		if name == "" || name == "." || name == ".." {
			continue
		}

		info := beegfsEntryInfo{
			entryID:           strings.TrimSpace(fields[1]),
			owner:             ownerFromID(strings.TrimSpace(fields[2])),
			stripePatternType: safeAtoi(strings.TrimSpace(fields[3])),
			chunkSize:         safeAtoi64(strings.TrimSpace(fields[4])),
			numberOfTargets:   safeAtoi(strings.TrimSpace(fields[5])),
			hasBeegfsMetadata: true,
		}

		result[name] = info
	}

	return result, nil
}

func ownerFromID(id string) string {
	if id == "" {
		return "0"
	}

	if userInfo, err := user.LookupId(id); err == nil && userInfo != nil {
		return userInfo.Username
	}

	return id
}

func safeAtoi(val string) int {
	if val == "" {
		return 0
	}
	i, err := strconv.Atoi(val)
	if err != nil {
		return 0
	}
	return i
}

func safeAtoi64(val string) int64 {
	if val == "" {
		return 0
	}
	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0
	}
	return i
}

func buildBeegfsPrefix(metadata beegfsEntryInfo, delim string) string {
	if !metadata.hasBeegfsMetadata {
		metadata.entryID = "0"
		metadata.owner = "0"
	}

	return strings.Join([]string{
		valueOrDefault(metadata.entryID, "0"),
		valueOrDefault(metadata.owner, "0"),
		strconv.Itoa(metadata.stripePatternType),
		strconv.FormatInt(metadata.chunkSize, 10),
		strconv.Itoa(metadata.numberOfTargets),
	}, delim)
}

func valueOrDefault(value, def string) string {
	if value == "" {
		return def
	}
	return value
}

func augmentAndPrintLsOutput(out []byte, longListing bool, delim string, paths []string, indexRoot string, log *logger.Logger) error {
	lines := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
	if len(lines) == 1 && lines[0] == "" {
		return nil
	}

	metaCache := make(map[string]map[string]beegfsEntryInfo)
	currentDir := ""
	if len(paths) > 0 {
		currentDir = absIndexPath(strings.TrimSuffix(paths[0], string(filepath.Separator)), indexRoot)
	}

	for _, raw := range lines {
		line := strings.TrimSpace(raw)
		if line == "" {
			fmt.Println(raw)
			continue
		}

		if strings.HasSuffix(line, ":") {
			currentDir = absIndexPath(strings.TrimSuffix(line, ":"), indexRoot)
			fmt.Println(raw)
			continue
		}

		if strings.HasPrefix(line, "total") {
			fmt.Println(raw)
			continue
		}

		dirForEntry := currentDir
		if dirForEntry == "" && len(paths) > 0 {
			dirForEntry = absIndexPath(strings.TrimSuffix(paths[0], string(filepath.Separator)), indexRoot)
		}

		if dirForEntry == "" {
			fmt.Println(raw)
			continue
		}

		name, ok := extractEntryName(line, longListing, delim)
		if !ok {
			fmt.Println(raw)
			continue
		}

		metadata, err := metadataForDir(dirForEntry, indexRoot, metaCache, log)
		if err != nil {
			return err
		}

		prefix := buildBeegfsPrefix(metadata[name], delim)
		fmt.Println(prefix + delim + raw)
	}

	return nil
}

func extractEntryName(line string, longListing bool, delim string) (string, bool) {
	if !longListing {
		return strings.TrimSpace(line), true
	}

	fields := splitByDelimiter(line, delim)
	if len(fields) == 0 {
		return "", false
	}
	return fields[len(fields)-1], true
}

func splitByDelimiter(line string, delim string) []string {
	if delim == "" {
		return strings.Fields(line)
	}

	var parts []string
	for _, part := range strings.Split(line, delim) {
		if part == "" {
			continue
		}
		parts = append(parts, part)
	}
	if len(parts) == 0 && delim == " " {
		return strings.Fields(line)
	}
	return parts
}

func metadataForDir(dir string, indexRoot string, cache map[string]map[string]beegfsEntryInfo, log *logger.Logger) (map[string]beegfsEntryInfo, error) {
	if metadata, ok := cache[dir]; ok {
		return metadata, nil
	}

	cleanDir := absIndexPath(dir, indexRoot)
	dbPath := filepath.Join(cleanDir, "db.db")
	metadata, err := fetchBeegfsInfo(dbPath, log)
	if err != nil {
		// Attempt parent directory fallback (single file input)
		parent := filepath.Dir(cleanDir)
		if parent != "" && parent != cleanDir {
			parentDB := filepath.Join(parent, "db.db")
			if parentMeta, parentErr := fetchBeegfsInfo(parentDB, log); parentErr == nil {
				cache[cleanDir] = parentMeta
				return parentMeta, nil
			}
		}
		return nil, err
	}

	cache[cleanDir] = metadata
	return metadata, nil
}

func resolvePath(p string) string {
	if filepath.IsAbs(p) {
		return filepath.Clean(p)
	}
	cwd, err := os.Getwd()
	if err != nil {
		return filepath.Clean(p)
	}
	return filepath.Clean(filepath.Join(cwd, p))
}

func normalizeIndexPath(p, indexRoot string) (string, string) {
	abs := resolvePath(p)
	root := filepath.Clean(indexRoot)
	if root != "" && strings.HasPrefix(abs, root) {
		rel := strings.TrimPrefix(abs, root)
		rel = strings.TrimPrefix(rel, string(filepath.Separator))
		if rel == "" {
			rel = "."
		}
		return rel, filepath.Join(root, rel)
	}
	return p, abs
}

func absIndexPath(p, indexRoot string) string {
	if filepath.IsAbs(p) {
		return filepath.Clean(p)
	}
	root := filepath.Clean(indexRoot)
	if root != "" {
		return filepath.Join(root, p)
	}
	return resolvePath(p)
}
