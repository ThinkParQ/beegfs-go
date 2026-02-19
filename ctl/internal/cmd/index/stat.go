package index

import (
	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"go.uber.org/zap"
)

func newGenericStatCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, err := parseIndexAddr(indexAddr)
			if err != nil {
				return err
			}
			beegfsEnabled, err := cmd.Flags().GetBool("beegfs")
			if err != nil {
				return err
			}
			if beegfsEnabled {
				if err := checkIndexConfig(backend, queryBinary); err != nil {
					return err
				}
				verbose, err := cmd.Flags().GetBool("verbose")
				if err != nil {
					return err
				}
				paths, err := defaultBeeGFSPaths(backend, args)
				if err != nil {
					return err
				}
				return runBeeGFSStatIndex(backend, paths, verbose)
			}
			paths, err := defaultBeeGFSPaths(backend, args)
			if err != nil {
				return err
			}
			if err := checkIndexConfig(backend, statBinary); err != nil {
				return err
			}
			return runPythonStatIndex(bflagSet, backend, paths)
		},
	}
	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("version", "v", "Show program's version number and exit.", "--version", false),
		bflag.Flag("verbose", "V", "Show the actual command being run.", "--verbose", false),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)
	cmd.Flags().BoolP("beegfs", "b", false, "Print BeeGFS metadata for the file(s)")
	cmd.MarkFlagsMutuallyExclusive("beegfs", "version")

	return cmd
}

func newStatCmd() *cobra.Command {
	s := newGenericStatCmd()
	s.Use = "stat"
	s.Short = "Displays file or directory metadata information."

	s.Long = `Retrieve metadata information for files and directories.

This command displays detailed status information, similar to the standard "stat" command, 
including metadata attributes for files and directories. Additional options allow retrieval 
of BeeGFS-specific metadata if available.

Example: Display the status of a file

$ beegfs index stat README

Example: Display BeeGFS-specific metadata for a file

$ beegfs index stat --beegfs README
`
	return s
}

func runPythonStatIndex(bflagSet *bflag.FlagSet, backend indexBackend, paths []string) error {
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+len(paths))
	allArgs = append(allArgs, wrappedArgs...)
	allArgs = append(allArgs, paths...)
	return runIndexCommandWithPrint(backend, statBinary, allArgs, "Running GUFI stat command",
		zap.String("indexAddr", indexAddr),
		zap.Any("wrappedArgs", wrappedArgs),
		zap.Any("paths", paths),
		zap.Any("allArgs", allArgs),
	)
}
