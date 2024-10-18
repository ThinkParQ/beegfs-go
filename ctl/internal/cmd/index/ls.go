package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

type createLsConfig struct {
	beegfs        bool
	all           bool
	almostAll     bool
	blockSize     string
	ignoreBackups bool
	fullTime      bool
	noGroup       bool
	humanReadable bool
	inode         bool
	longListing   bool
	reverse       bool
	recursive     bool
	size          bool
	sortLargest   bool
	timeStyle     string
	mtime         bool
	noSort        bool
	path          string
	delim         string
	inMemoryName  string
	nlinkWidth    int
	sizeWidth     int
	userWidth     int
	groupWidth    int
	version       bool
}

func fullTimeAction(cfg *createLsConfig) {
	cfg.longListing = true
	cfg.beegfs = true
	cfg.timeStyle = "full-iso"
}

func newGenericLsCmd() *cobra.Command {
	cfg := createLsConfig{}

	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				cfg.path = args[0]
			} else {
				cwd, err := os.Getwd()
				if err != nil {
					return err
				}
				beegfsClient, err := config.BeeGFSClient(cwd)
				if err != nil {
					return err
				}
				cfg.path = beegfsClient.GetMountPath()
			}
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonLsIndex(&cfg)
		},
	}

	cmd.PersistentFlags().BoolP("help", "", false, "Help for ls")
	cmd.Flags().BoolVarP(&cfg.beegfs, "beegfs", "b", false, "Print BeeGFS metadata for the file(s)")
	cmd.Flags().BoolVarP(&cfg.all, "all", "a", false, "Do not ignore entries starting with .")
	cmd.Flags().BoolVarP(&cfg.almostAll, "almost-all", "A", false, "Do not list implied . and ..")
	cmd.Flags().StringVar(&cfg.blockSize, "block-size", "", "{K,KB,KiB,M,MB,"+
		"MiB,G,GB,GiB,T,TB,TiB,P,PB,PiB,E,EB,EiB,Z,ZB,ZiB,Y,YB,YiB}, With -l,"+
		" scale sizes by SIZE when printing them")
	cmd.Flags().BoolVarP(&cfg.ignoreBackups, "ignore-backups", "B", false, "Do not list implied entries ending with ~")
	cmd.Flags().BoolVar(&cfg.fullTime, "full-time", false, "Like -l --time-style=full-iso")
	cmd.Flags().BoolVarP(&cfg.noGroup, "no-group", "G", false, "In a long listing, don't print group names")
	cmd.Flags().BoolVarP(&cfg.humanReadable, "human-readable", "h", false, "With -l and -s, print sizes like 1K 234M 2G etc.")
	cmd.Flags().BoolVarP(&cfg.inode, "inode", "i", false, "Print the index number of each file")
	cmd.Flags().BoolVarP(&cfg.longListing, "long-listing", "l", false, "Use a long listing format")
	cmd.Flags().BoolVarP(&cfg.reverse, "reverse", "r", false, "Reverse order while sorting")
	cmd.Flags().BoolVarP(&cfg.recursive, "recursive", "R", false, "List subdirectories recursively")
	cmd.Flags().BoolVarP(&cfg.size, "size", "s", false, "Print the allocated size of each file, in blocks")
	cmd.Flags().BoolVarP(&cfg.sortLargest, "sort-largest", "S", false, "Sort by file size, largest first")
	cmd.Flags().BoolVarP(&cfg.version, "version", "v", false, "BeeGFS Hive Index Version")
	cmd.Flags().StringVar(&cfg.timeStyle, "time-style", "", "Time/date format with -l")
	cmd.Flags().BoolVarP(&cfg.mtime, "mtime", "t", false, "Sort by modification time, newest first")
	cmd.Flags().BoolVarP(&cfg.noSort, "no-sort", "U", false, "Do not sort; list entries in directory order")
	cmd.Flags().StringVar(&cfg.delim, "delim", " ", "Delimiter separating output columns")
	cmd.Flags().StringVar(&cfg.inMemoryName, "in-memory-name", "out", "In-memory name (hidden argument)")
	cmd.Flags().IntVar(&cfg.nlinkWidth, "nlink-width", 2, "Width of nlink column")
	cmd.Flags().IntVar(&cfg.sizeWidth, "size-width", 10, "Width of size column")
	cmd.Flags().IntVar(&cfg.userWidth, "user-width", 5, "Width of user column")
	cmd.Flags().IntVar(&cfg.groupWidth, "group-width", 5, "Width of group column")
	err := cmd.Flags().MarkHidden("in-memory-name")
	if err != nil {
		return nil
	}

	return cmd
}

func newLsCmd() *cobra.Command {
	s := newGenericLsCmd()
	s.Use = "ls"
	s.Short = "Lists the contents"
	s.Long = `Lists the contents of the index directory.

It functions similarly to standard ls commands and supports both absolute and relative paths. 
You can run this command from the index directory or a filesystem directory when using relative paths.

Example: List the contents of the index directory located at /mnt/index.
$ beegfs index ls /mnt/index
`
	return s

}

func validateLsInputs(cfg *createLsConfig) error {
	if cfg.fullTime {
		fullTimeAction(cfg)
	}
	if cfg.delim != "" {
		if err := getChar(cfg.delim); err != nil {
			return err
		}
	}
	if err := getNonNegative(cfg.nlinkWidth); err != nil {
		return err
	}
	if err := getNonNegative(cfg.sizeWidth); err != nil {
		return err
	}
	if err := getNonNegative(cfg.userWidth); err != nil {
		return err
	}
	if err := getNonNegative(cfg.groupWidth); err != nil {
		return err
	}
	if cfg.blockSize != "" {
		if err := validateBlockSize(cfg.blockSize); err != nil {
			return err
		}
	}
	return nil
}

func runPythonLsIndex(cfg *createLsConfig) error {
	if err := validateLsInputs(cfg); err != nil {
		return err
	}

	args := []string{
		"ls",
	}

	if cfg.beegfs {
		args = append(args, "--beegfs")
	}
	if cfg.all {
		args = append(args, "-a")
	}
	if cfg.almostAll {
		args = append(args, "-A")
	}
	if cfg.blockSize != "" {
		args = append(args, "--block-size", cfg.blockSize)
	}
	if cfg.ignoreBackups {
		args = append(args, "--ignore-backups")
	}
	if cfg.fullTime {
		args = append(args, "--full-time")
	}
	if cfg.noGroup {
		args = append(args, "--no-group")
	}
	if cfg.humanReadable {
		args = append(args, "-h")
	}
	if cfg.inode {
		args = append(args, "-i")
	}
	if cfg.longListing {
		args = append(args, "-l")
	}
	if cfg.reverse {
		args = append(args, "-r")
	}
	if cfg.recursive {
		args = append(args, "-R")
	}
	if cfg.size {
		args = append(args, "-s")
	}
	if cfg.sortLargest {
		args = append(args, "--sort=size")
	}
	if cfg.version {
		args = append(args, "--version")
	}
	if cfg.timeStyle != "" {
		args = append(args, "--time-style", cfg.timeStyle)
	}
	if cfg.mtime {
		args = append(args, "--mtime")
	}
	if cfg.noSort {
		args = append(args, "-U")
	}
	if cfg.path != "" {
		args = append(args, cfg.path)
	}
	if cfg.delim != "" {
		args = append(args, "--delim", cfg.delim)
	}
	if cfg.inMemoryName != "" {
		args = append(args, "--in-memory-name", cfg.inMemoryName)
	}
	if cfg.nlinkWidth > 0 {
		args = append(args, "--nlink-width", fmt.Sprint(cfg.nlinkWidth))
	}
	if cfg.sizeWidth > 0 {
		args = append(args, "--size-width", fmt.Sprint(cfg.sizeWidth))
	}
	if cfg.userWidth > 0 {
		args = append(args, "--user-width", fmt.Sprint(cfg.userWidth))
	}
	if cfg.groupWidth > 0 {
		args = append(args, "--group-width", fmt.Sprint(cfg.groupWidth))
	}

	cmd := exec.Command(beeBinary, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("error starting command: %v", err)
	}
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("error executing beeBinary: %v", err)
	}
	return nil
}
