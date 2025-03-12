package index

import (
	"fmt"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

func newGenericRescanCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet
	var recurse bool
	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) != 0 {
				cwd, err := os.Getwd()
				if err != nil {
					return err
				}
				args = append([]string{cwd}, args...)
			}
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			return runPythonRescanIndex(args, bflagSet, recurse)
		},
	}
	rescanFlags := []bflag.FlagWrapper{
		bflag.Flag("max-memory", "X", "Max memory usage (e.g. 8GB, 1G)", "-X", ""),
		bflag.GlobalFlag(config.NumWorkersKey, "-n"),
		bflag.GlobalFlag(config.DebugKey, "-V=1"),
		bflag.Flag("no-metadata", "B", "Do not extract BeeGFS specific metadata", "-B", false),
	}
	bflagSet = bflag.NewFlagSet(rescanFlags, cmd)
	cmd.Flags().BoolVar(&recurse, "recurse", false, "Recursively rescan all directories beneath the specified path.")

	return cmd
}

func newRescanCmd() *cobra.Command {
	s := newGenericRescanCmd()
	s.Use = "rescan"
	s.Short = "Rescan Bee"
	s.Long = "Rescan Bee"

	return s
}

func runPythonRescanIndex(paths []string, bflagSet *bflag.FlagSet, recurse bool) error {
	log, _ := config.GetLogger()
	wrappedArgs := bflagSet.WrappedArgs()
	for _, path := range paths {
		allArgs := make([]string, 0, len(wrappedArgs)+4)
		allArgs = append(allArgs, createCmd, "-F", path)
		allArgs = append(allArgs, wrappedArgs...)
		if recurse {
			allArgs = append(allArgs, "-U")
		} else {
			allArgs = append(allArgs, "-k")
		}
		log.Debug("Running BeeGFS Hive Index rescan command",
			zap.String("path", path),
			zap.Bool("recurse", recurse),
			zap.Any("wrappedArgs", wrappedArgs),
			zap.Any("allArgs", allArgs),
		)
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
	treeArgs := []string{createCmd, "-S"}
	log.Debug("Running BeeGFS Hive Index Tree-Summary command",
		zap.Any("Args", treeArgs),
	)
	cmd := exec.Command(beeBinary, treeArgs...)
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
