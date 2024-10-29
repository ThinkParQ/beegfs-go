package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/v8/ctl/pkg/config"
)

type createIndexConfig struct {
	maxMemory string
	fsPath    string
	indexPath string
	summary   bool
	xattrs    bool
	maxLevel  uint
	scanDirs  bool
	port      uint
	mntPath   string
	runUpdate bool
}

func newGenericCreateCmd() *cobra.Command {
	cfg := createIndexConfig{}

	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonCreateIndex(&cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.fsPath, "fs-path", "", "File system path for which index will be created [default: IndexEnv.conf]")
	cmd.Flags().StringVar(&cfg.indexPath, "index-path", "", "Index directory path [default: IndexEnv.conf]")
	cmd.Flags().StringVar(&cfg.maxMemory, "max-memory", "", "Max memory usage (e.g. 8GB, 1G)")
	cmd.Flags().BoolVar(&cfg.summary, "summary", false, "Create tree summary table along with other tables")
	cmd.Flags().BoolVar(&cfg.xattrs, "xattrs", false, "Pull xattrs from source")
	cmd.Flags().UintVar(&cfg.maxLevel, "max-level", 0, "Max level to go down")
	cmd.Flags().BoolVar(&cfg.scanDirs, "scan-dirs", false, "Print the number of scanned directories")
	cmd.Flags().UintVar(&cfg.port, "port", 0, "Port number to connect with client")
	cmd.Flags().BoolVar(&cfg.runUpdate, "update", false, "Run the update index")

	return cmd
}

func newCreateCmd() *cobra.Command {
	s := newGenericCreateCmd()
	s.Use = "create"
	s.Short = "Generates the index"
	s.Long = `Generate the index by walking the source directory.

The index can be created inside the source directory or separate index directory.
Breadth first readdirplus walk of input tree to list the tree, or create an output
db and/or output files of encountered directories and or files. This program has
two primary uses: to find suspect directories that have changed in some way that
need to be used to incrementally  update a Hive index from source file system changes
and to create a full dump of all directories and or files/links either in walk order
(directory then all files in that dir, etc.) or striding inodes into multiple files
to merge against attribute list files that are also inode strided.

Example: Create an index for the file system located at /mnt/fs, limiting memory usage to 8GB.
$ beegfs index create --fs-path /mnt/fs --index-path /mnt/index --max-memory 8GB
`
	return s

}

func validateCreateInputs(cfg *createIndexConfig) error {
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	beegfsClient, err := config.BeeGFSClient(cwd)
	if err != nil {
		return err
	}
	cfg.mntPath = beegfsClient.GetMountPath()

	return nil
}

func runPythonCreateIndex(cfg *createIndexConfig) error {
	if err := validateCreateInputs(cfg); err != nil {
		return err
	}

	args := []string{
		"index",
	}

	if cfg.fsPath != "" {
		args = append(args, "-F", cfg.fsPath)
	}
	if cfg.indexPath != "" {
		args = append(args, "-I", cfg.indexPath)
	}
	if cfg.maxMemory != "" {
		args = append(args, "-X", cfg.maxMemory)
	}
	args = append(args, "-n", fmt.Sprint(viper.GetInt(config.NumWorkersKey)))
	if cfg.summary {
		args = append(args, "-S")
	}
	if cfg.xattrs {
		args = append(args, "-x")
	}
	if cfg.maxLevel > 0 {
		args = append(args, "-z", fmt.Sprint(cfg.maxLevel))
	}
	if cfg.scanDirs {
		args = append(args, "-C")
	}
	if cfg.port > 0 {
		args = append(args, "-p", fmt.Sprint(cfg.port))
	}
	if cfg.mntPath != "" {
		args = append(args, "-M", cfg.mntPath)
	}
	if viper.GetBool(config.DebugKey) {
		args = append(args, "-V", "1")
	}
	if cfg.runUpdate {
		args = append(args, "-U")
	}
	args = append(args, "-k")

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
