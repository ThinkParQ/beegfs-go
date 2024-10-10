package index

import (
	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
)

func newGenericCreateCmd() *cobra.Command {
	cfg := index.CreateIndex_Config{}

	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			return index.RunPythonCreateIndex(&cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.FsPath, "fs-path", "", "File system path for which index will be created (required)")
	cmd.Flags().StringVar(&cfg.IndexPath, "index-path", "", "Index directory path (required)")
	cmd.Flags().StringVar(&cfg.MaxMemory, "max-memory", "", "Max memory usage (e.g. 8GB, 1G)")
	cmd.Flags().UintVar(&cfg.NumThreads, "num-threads", 0, "Number of threads to create index")
	cmd.Flags().BoolVar(&cfg.Summary, "summary", false, "Create tree summary table along with other tables")
	cmd.Flags().BoolVar(&cfg.Xattrs, "xattrs", false, "Pull xattrs from source")
	cmd.Flags().UintVar(&cfg.MaxLevel, "max-level", 0, "Max level to go down")
	cmd.Flags().BoolVar(&cfg.ScanDirs, "scan-dirs", false, "Print the number of scanned directories")
	cmd.Flags().UintVar(&cfg.Port, "port", 0, "Port number to connect with client")
	cmd.Flags().StringVar(&cfg.MntPath, "mnt-path", "", "File system mount point path")
	cmd.Flags().BoolVar(&cfg.Debug, "debug", false, "Enable debugging mode")
	cmd.Flags().BoolVar(&cfg.RunUpdate, "update", false, "Run the update index")

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
