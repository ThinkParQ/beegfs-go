package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
)

func newGenericRescanCmd() *cobra.Command {
	var recurse bool
	var cmd = &cobra.Command{
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonRescanIndex(args, recurse)
		},
	}

	cmd.Flags().BoolVar(&recurse, "recurse", false, "Rescan Directory with Recursion")

	return cmd
}

func newRescanCmd() *cobra.Command {
	s := newGenericRescanCmd()
	s.Use = "rescan"
	s.Short = "Rescan Bee"
	s.Long = "Rescan Bee"

	return s
}

func runPythonRescanIndex(paths []string, recurse bool) error {
	for _, path := range paths {
		allArgs := make([]string, 0, 4)
		allArgs = append(allArgs, createCmd, "-F", path)
		if recurse {
			allArgs = append(allArgs, "-U")
		} else {
			allArgs = append(allArgs, "-k")
		}
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
	}
	cmd := exec.Command(beeBinary, createCmd, "-S")
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
