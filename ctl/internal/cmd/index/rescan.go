package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
)

func newGenericRescanCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet
	var recurse bool
	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonRescanIndex(bflagSet, recurse)
		},
	}

	rescanFlags := []bflag.FlagWrapper{
		bflag.Flag("file-path", "F", "Sub Directory Path to Rescan", "-F", ""),
	}
	cmd.Flags().BoolVar(&recurse, "recurse", false, "Rescan Directory with Recursion")
	bflagSet = bflag.NewFlagSet(rescanFlags, cmd)
	cmd.MarkFlagRequired("file-path")

	return cmd
}

func newRescanCmd() *cobra.Command {
	s := newGenericRescanCmd()
	s.Use = "rescan"
	s.Short = "Rescan Bee"
	s.Long = "Rescan Bee"

	return s
}

func runPythonRescanIndex(bflagSet *bflag.FlagSet, recurse bool) error {
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+2)
	allArgs = append(allArgs, createCmd)
	if recurse {
		allArgs = append(allArgs, "-U")
	} else {
		allArgs = append(allArgs, "-k")
	}
	allArgs = append(allArgs, wrappedArgs...)
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
