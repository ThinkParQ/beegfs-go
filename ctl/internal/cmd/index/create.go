package index

import (
	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"go.uber.org/zap"
)

func newGenericCreateCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, err := parseIndexAddr(indexAddr)
			if err != nil {
				return err
			}
			if err := checkIndexConfig(backend, beeBinary); err != nil {
				return err
			}
			return runPythonCreateIndex(bflagSet, backend)
		},
	}

	bflagSet = bflag.NewFlagSet(commonIndexFlags, cmd)
	return cmd
}

func newCreateCmd() *cobra.Command {
	s := newGenericCreateCmd()
	s.Use = "create"
	s.Short = "Generates or updates the index for the specified file system."
	s.Long = `Generate or updates the index by traversing the source directory.

The index can exist within the source directory or in a separate index directory.
The program performs a breadth-first readdirplus traversal to list the contents, or it creates
an output database and/or files listing directories and files it encounters. This program serves two main purposes:

1. To identify directories with changes, allowing incremental updates to a Hive index from changes in the source file system.
2. To create a comprehensive dump of directories, files, and links. You can choose to output in traversal order
   (each directory followed by its files) or to stride inodes across multiple files for merging with inode-strided attribute lists.

Example: Create or update the index for the file system at /mnt/fs, limiting memory usage to 8GB:

$ beegfs index create --fs-path /mnt/fs --index-path /mnt/index --max-memory 8GB
`
	return s
}

func runPythonCreateIndex(bflagSet *bflag.FlagSet, backend indexBackend) error {
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs))
	allArgs = append(allArgs, wrappedArgs...)
	return runIndexCommandWithPrint(backend, beeBinary, allArgs, "Running GUFI dir2index command",
		zap.String("indexAddr", indexAddr),
		zap.Any("wrappedArgs", wrappedArgs),
		zap.Any("allArgs", allArgs),
	)
}
