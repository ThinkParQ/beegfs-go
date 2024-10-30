package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

const createCmd = "index"

func newGenericCreateCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonCreateIndex(bflagSet)
		},
	}

	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("fs-path", "F", "File system path for which index will be created [default"+": IndexEnv.conf]", "-F", ""),
		bflag.Flag("index-path", "I",
			"File system path for which index will be created [default: IndexEnv.conf]", "-I", ""),
		bflag.GlobalFlag(config.BeeGFSMountPointKey, "-M"),
		bflag.Flag("max-memory", "X", "Max memory usage (e.g. 8GB, 1G)", "-X", ""),
		bflag.GlobalFlag(config.NumWorkersKey, "-n"),
		bflag.Flag("summary", "s", "Create tree summary table along with other tables", "-s", false),
		bflag.Flag("only-summary", "S", "Create only tree summary table", "-S", false),
		bflag.Flag("xattrs", "x", "Pull xattrs from source", "-x", false),
		bflag.Flag("max-level", "z", "Max level to go down", "-z", 0),
		bflag.Flag("scan-dirs", "C", "Print the number of scanned directories", "-C", false),
		bflag.Flag("port", "p", "Port number to connect with client", "-p", 0),
		bflag.Flag("update", "U", "Run the update index", "-U", false),
		bflag.Flag("create", "k", "Run the create-index", "-k", true),
		bflag.Flag("version", "v", "BeeGFS Hive Index Version", "-v", false),
		bflag.GlobalFlag(config.DebugKey, "-V=1"),
		bflag.Flag("no-metadata", "B", "Do not extract BeeGFS specific metadata", "-B", false),
	}

	bflagSet = bflag.NewFlagSet(copyFlags, cmd)
	cmd.Flags().MarkHidden("create")
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

func runPythonCreateIndex(bflagSet *bflag.FlagSet) error {
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+1)
	allArgs = append(allArgs, createCmd)
	allArgs = append(allArgs, wrappedArgs...)

	cmd := exec.Command(beeBinary, allArgs...)
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
