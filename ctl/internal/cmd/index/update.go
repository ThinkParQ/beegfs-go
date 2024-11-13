package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
)

const updateCmd = "index"

func newGenericUpdateCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonUpdateIndex(bflagSet)
		},
	}

	bflagSet = bflag.NewFlagSet(commonIndexFlags, cmd)
	return cmd
}

func newUpdateCmd() *cobra.Command {
	s := newGenericUpdateCmd()
	s.Use = "update"
	s.Short = "Manually update an existing index."

	s.Long = `Manually update an existing index.

Only one instance of the update command should be ran at a time. 
This also means if you are using file system modification events to keep the
index up-to-date you should first stop the auto-update service before
manually updating the index.
`
	return s
}

func runPythonUpdateIndex(bflagSet *bflag.FlagSet) error {
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+2)
	allArgs = append(allArgs, updateCmd)
	allArgs = append(allArgs, wrappedArgs...)
	allArgs = append(allArgs, "-U")
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
