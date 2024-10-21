package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

type findIndexConfig struct {
	maxdepth     uint
	mindepth     uint
	version      bool
	amin         float64
	atime        float64
	cmin         float64
	ctime        float64
	empty        bool
	executable   bool
	falseFile    bool
	gid          int
	group        string
	inum         int
	links        int
	mmin         float64
	mtime        float64
	name         string
	entryID      string
	ownerID      string
	targetID     string
	newer        string
	path         string
	paths        string
	readable     bool
	sameFile     string
	size         string
	trueFile     bool
	fileType     string
	uid          string
	user         string
	writable     bool
	delim        string
	numResults   uint
	smallest     bool
	largest      bool
	inMemoryName string
}

func newGenericFindCmd() *cobra.Command {
	cfg := findIndexConfig{}

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
			return runPythonFindIndex(&cfg)
		},
	}

	cmd.Flags().UintVar(&cfg.maxdepth, "maxdepth", 0, "Descend at most levels (a non-negative integer) levels of directories.")
	cmd.Flags().UintVar(&cfg.mindepth, "mindepth", 0, "Do not apply any tests or actions at levels less than levels.")
	cmd.Flags().BoolVarP(&cfg.version, "version", "v", false, "Version of the find command.")
	cmd.Flags().Float64Var(&cfg.amin, "amin", 0, "File was last accessed N minutes ago.")
	cmd.Flags().Float64Var(&cfg.atime, "atime", 0, "File was last accessed N*24 hours ago.")
	cmd.Flags().Float64Var(&cfg.cmin, "cmin", 0, "File's status was last changed N minutes ago.")
	cmd.Flags().Float64Var(&cfg.ctime, "ctime", 0, "File's status was last changed N*24 hours ago.")
	cmd.Flags().BoolVar(&cfg.empty, "empty", false, "File is empty and is either a regular file or a directory.")
	cmd.Flags().BoolVar(&cfg.executable, "executable", false, "Matches files which are executable and directories which are searchable.")
	cmd.Flags().BoolVar(&cfg.falseFile, "false", false, "File is false and is either a regular file or a directory.")
	cmd.Flags().IntVar(&cfg.gid, "gid", 0, "File's numeric group ID is N.")
	cmd.Flags().StringVar(&cfg.group, "group", "", "File belongs to group gname (numeric group ID allowed).")
	cmd.Flags().IntVar(&cfg.inum, "inum", 0, "File has inode number N.")
	cmd.Flags().IntVar(&cfg.links, "links", 0, "File has N links.")
	cmd.Flags().Float64Var(&cfg.mmin, "mmin", 0, "File's data was last modified N minutes ago.")
	cmd.Flags().Float64Var(&cfg.mtime, "mtime", 0, "File's data was last modified N*24 hours ago.")
	cmd.Flags().StringVar(&cfg.name, "name", "", "Base of file name matches shell pattern.")
	cmd.Flags().StringVar(&cfg.entryID, "entryID", "", "Get file path for the given BeeGFS entryID.")
	cmd.Flags().StringVar(&cfg.ownerID, "ownerID", "", "Get list of files whose metadata is owned by given BeeGFS Metadata node ID.")
	cmd.Flags().StringVar(&cfg.targetID, "targetID", "", "Get list of files whose data is present on given BeeGFS targetID.")
	cmd.Flags().StringVar(&cfg.newer, "newer", "", "File was modified more recently than file.")
	cmd.Flags().StringVar(&cfg.paths, "path", "", "File name matches shell pattern pattern.")
	cmd.Flags().BoolVar(&cfg.readable, "readable", false, "Matches files which are readable.")
	cmd.Flags().StringVar(&cfg.sameFile, "samefile", "", "File refers to the same inode as name.")
	cmd.Flags().StringVar(&cfg.size, "size", "", "File's size matches the specified criteria.")
	cmd.Flags().BoolVar(&cfg.trueFile, "true", false, "Always true.")
	cmd.Flags().StringVar(&cfg.fileType, "type", "", "File is of type c.")
	cmd.Flags().StringVar(&cfg.uid, "uid", "", "File's numeric user ID is N.")
	cmd.Flags().StringVar(&cfg.user, "user", "", "File is owned by user uname.")
	cmd.Flags().BoolVar(&cfg.writable, "writable", false, "Matches files which are writable.")
	cmd.Flags().StringVar(&cfg.delim, "delim", " ", "Delimiter separating output columns.")
	cmd.Flags().UintVar(&cfg.numResults, "num-results", 0, "First n results.")
	cmd.Flags().BoolVar(&cfg.smallest, "smallest", false, "Top n smallest files.")
	cmd.Flags().BoolVar(&cfg.largest, "largest", false, "Top n largest files.")
	cmd.Flags().StringVar(&cfg.inMemoryName, "in-memory-name", "out", "In-memory name for processing.")
	err := cmd.Flags().MarkHidden("in-memory-name")
	if err != nil {
		return nil
	}

	return cmd
}

func newFindCmd() *cobra.Command {
	s := newGenericFindCmd()
	s.Use = "find"
	s.Short = "Finds files in the index"
	s.Long = `Allows users to get results by running queries over the index directory.

find has very similar options to GNU find, 
Hive’s find is way faster than running actual find commands over the filesystem.

Example: Get the list of files which are greater than 1GB in size
$ beegfs index find -size=+1G
`
	return s
}

func validateFindInputs(cfg *findIndexConfig) error {
	if cfg.size != "" {
		if err := isValidSize(cfg.size); err != nil {
			return err
		}
	}
	return nil
}

func runPythonFindIndex(cfg *findIndexConfig) error {
	if err := validateFindInputs(cfg); err != nil {
		return err
	}

	args := []string{
		"find",
	}

	if cfg.path != "" {
		args = append(args, cfg.path)
	}
	if cfg.maxdepth > 0 {
		args = append(args, "-maxdepth", fmt.Sprint(cfg.maxdepth))
	}
	if cfg.mindepth > 0 {
		args = append(args, "-mindepth", fmt.Sprint(cfg.mindepth))
	}
	if cfg.version {
		args = append(args, "--version")
	}
	if cfg.amin != 0 {
		args = append(args, "-amin", fmt.Sprint(cfg.amin))
	}
	if cfg.atime != 0 {
		args = append(args, "-atime", fmt.Sprint(cfg.atime))
	}
	if cfg.cmin != 0 {
		args = append(args, "-cmin", fmt.Sprint(cfg.cmin))
	}
	if cfg.ctime != 0 {
		args = append(args, "-ctime", fmt.Sprint(cfg.ctime))
	}
	if cfg.empty {
		args = append(args, "-empty")
	}
	if cfg.executable {
		args = append(args, "-executable")
	}
	if cfg.falseFile {
		args = append(args, "-false")
	}
	if cfg.gid != 0 {
		args = append(args, "-gid", fmt.Sprint(cfg.gid))
	}
	if cfg.group != "" {
		args = append(args, "-group", cfg.group)
	}
	if cfg.inum != 0 {
		args = append(args, "-inum", fmt.Sprint(cfg.inum))
	}
	if cfg.links != 0 {
		args = append(args, "-links", fmt.Sprint(cfg.links))
	}
	if cfg.mmin != 0 {
		args = append(args, "-mmin", fmt.Sprint(cfg.mmin))
	}
	if cfg.mtime != 0 {
		args = append(args, "-mtime", fmt.Sprint(cfg.mtime))
	}
	if len(cfg.name) > 0 {
		args = append(args, "-name", fmt.Sprint(cfg.name))
	}
	if len(cfg.entryID) > 0 {
		args = append(args, "-entryID", fmt.Sprint(cfg.entryID))
	}
	if len(cfg.ownerID) > 0 {
		args = append(args, "-ownerID", fmt.Sprint(cfg.ownerID))
	}
	if len(cfg.targetID) > 0 {
		args = append(args, "-targetID", fmt.Sprint(cfg.targetID))
	}
	if cfg.newer != "" {
		args = append(args, "-newer", cfg.newer)
	}
	if len(cfg.paths) > 0 {
		args = append(args, "-path", fmt.Sprint(cfg.paths))
	}
	if cfg.readable {
		args = append(args, "-readable")
	}
	if cfg.sameFile != "" {
		args = append(args, "-samefile", cfg.sameFile)
	}
	if len(cfg.size) > 0 {
		args = append(args, fmt.Sprintf("-size=%s", cfg.size))
	}
	if cfg.trueFile {
		args = append(args, "-true")
	}
	if cfg.fileType != "" {
		args = append(args, "-type", cfg.fileType)
	}
	if len(cfg.uid) > 0 {
		args = append(args, "-uid", fmt.Sprint(cfg.uid))
	}
	if cfg.user != "" {
		args = append(args, "-user", cfg.user)
	}
	if cfg.writable {
		args = append(args, "-writable")
	}
	if cfg.delim != "" {
		args = append(args, "--delim", cfg.delim)
	}
	if cfg.numResults > 0 {
		args = append(args, "--num-results", fmt.Sprint(cfg.numResults))
	}
	if cfg.smallest {
		args = append(args, "--smallest")
	}
	if cfg.largest {
		args = append(args, "--largest")
	}
	if cfg.inMemoryName != "" {
		args = append(args, "--in-memory-name", cfg.inMemoryName)
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
		return fmt.Errorf("error executing command: %v", err)
	}
	return nil
}
