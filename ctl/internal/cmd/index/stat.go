package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

const statCmd = "stat"

func newGenericStatCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				path = args[0]
			} else {
				cwd, err := os.Getwd()
				if err != nil {
					return err
				}
				beegfsClient, err := config.BeeGFSClient(cwd)
				if err != nil {
					return err
				}
				path = beegfsClient.GetMountPath()
			}
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonStatIndex(bflagSet, path)
		},
	}
	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("beegfs", "b", "Print BeeGFS Metadata for the File", "--beegfs", false),
		bflag.Flag("debug-values", "H", "Show assigned input values (for debugging)", "-H", false),
		bflag.Flag("terse", "j", "Print the information in terse form", "-j", false),
		bflag.Flag("format", "f", "Use the specified FORMAT instead of the default; output a newline after each use of FORMAT", "-f", ""),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)

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

func runPythonStatIndex(bflagSet *bflag.FlagSet, path string) error {
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+2)
	allArgs = append(allArgs, statCmd)
	allArgs = append(allArgs, wrappedArgs...)
	allArgs = append(allArgs, path)
	cmd := exec.Command(beeBinary, allArgs...)
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
