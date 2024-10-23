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

var path string

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
		bflag.Flag("beegfs", "", "Print BeeGFS Metadata for the File",
			"--beegfs", false),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)

	return cmd
}

func newStatCmd() *cobra.Command {
	s := newGenericStatCmd()
	s.Use = "stat"
	s.Short = "Display file status"
	s.Long = `Displays file and directory metadata information.

Example: View the Stat of a File
$ beegfs index stat README

View the BeeGFS Stat of a File
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
		return fmt.Errorf("error starting command: %v", err)
	}
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("error executing beeBinary: %v", err)
	}
	return nil
}
