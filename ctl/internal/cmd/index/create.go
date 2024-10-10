package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
)

type createIndex_Config struct {
	maxMemory  string
	fsPath     string
	indexPath  string
	numThreads uint
	summary    bool
	xattrs     bool
	maxLevel   uint
	scanDirs   bool
	port       uint
	debug      bool
	mntPath    string
	runUpdate  bool
}

func newGenericCreateCmd() *cobra.Command {
	cfg := createIndex_Config{}

	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeegfsConfig(); err != nil {
				return err
			}
			return runPythonCreateIndex(&cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.fsPath, "fs-path", "", "File system path for which index will be created")
	cmd.Flags().StringVar(&cfg.indexPath, "index-path", "", "Index directory path")
	cmd.Flags().StringVar(&cfg.maxMemory, "max-memory", "", "Max memory usage (e.g. 8GB, 1G)")
	cmd.Flags().UintVar(&cfg.numThreads, "num-threads", 0, "Number of threads to create index")
	cmd.Flags().BoolVar(&cfg.summary, "summary", false, "Create tree summary table along with other tables")
	cmd.Flags().BoolVar(&cfg.xattrs, "xattrs", false, "Pull xattrs from source")
	cmd.Flags().UintVar(&cfg.maxLevel, "max-level", 0, "Max level to go down")
	cmd.Flags().BoolVar(&cfg.scanDirs, "scan-dirs", false, "Print the number of scanned directories")
	cmd.Flags().UintVar(&cfg.port, "port", 0, "Port number to connect with client")
	cmd.Flags().StringVar(&cfg.mntPath, "mnt-path", "", "Specify the mount point of the source directory.")
	cmd.Flags().BoolVar(&cfg.debug, "debug", false, "Enable debugging mode")
	cmd.Flags().BoolVar(&cfg.runUpdate, "update", false, "Run the update index")

	return cmd
}

func newCreateCmd() *cobra.Command {
	s := newGenericCreateCmd()
	s.Use = "create"
	s.Short = "Generates a BeeGFS Hive index"
	s.Long = `Generate  a  Hive-index by walking the source directory.
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

// It ensures the necessary Beegfs configurations and binaries are present before attempting to create an index, preventing runtime errors.
func checkBeegfsConfig() error {
	if _, err := os.Stat("/usr/bin/bee"); os.IsNotExist(err) {
		return fmt.Errorf("BeeGFS Hive binary not found at /usr/bin/bee.")
	}

	if _, err := os.Stat("/etc/beegfs/index/config"); os.IsNotExist(err) {
		return fmt.Errorf("Beegfs Hive is not configured: /etc/beegfs/index/config not found")
	}

	if _, err := os.Stat("/etc/beegfs/index/indexEnv.conf"); os.IsNotExist(err) {
		return fmt.Errorf("Beegfs Hive is not configured: /etc/beegfs/index/indexEnv.conf not found")
	}

	return nil
}

func runPythonCreateIndex(cfg *createIndex_Config) error {
	args := []string{
		"/usr/bin/bee", "index",
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
	if cfg.numThreads > 0 {
		args = append(args, "-n", fmt.Sprint(cfg.numThreads))
	}
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
	if cfg.debug {
		args = append(args, "-V", "1")
	}
	if cfg.runUpdate {
		args = append(args, "-U")
	}
	args = append(args, "-k")

	cmd := exec.Command("python3", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing command: %v\nOutput: %s", err, string(output))
	}
	return nil
}
