package copy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

const (
	beegfsCopyPath = "/opt/beegfs/sbin/beegfs-copy/bin/beegfs-copy"
	featureString  = "io.beegfs.copy"
)

type frontendCfg struct {
	stdinDelimiter string
	batchSize      int
}

func NewCopyCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet
	var frontendCfg frontendCfg
	cmd := &cobra.Command{
		Use:     "copy <source> [<source>] <destination>",
		Args:    cobra.MinimumNArgs(2),
		Aliases: []string{"cp"},
		Short:   "Copy files and directories in parallel.",
		Long: fmt.Sprintf(`Copy files and directories in parallel.

Specifying Paths:
One or more <source> paths can be copied under a <destination> directory.
When supported by the current shell, standard wildcards (globbing patterns) can be used in each path.
Alternatively paths can be provided using stdin by using '-' as the <source> (example: 'cat file_list.txt | beegfs copy - <destination>').

Parallelism:
This mode supports two levels of parallelism:

* Thread parallelism using multiple threads per node based on the --%s flag (by default the number of cores on this machine).
* Node level parallelism using one or more machines specified in a machine file.

Passwordless SSH must be setup to all the nodes in the machine file, including the localhost if specified.
A machine file is always required and the simplest file could contain a single entry for the local machine:

machinefile.txt
  localhost

If the same BeeGFS instance or network share is mounted to multiple nodes, multiple machines can be used to copy data in parallel.
A path to a machine file containing a list of nodes to use to execute the copy operation must be provided:

machinefile.txt
  node01
  node02
  node03

IMPORTANT: The primary use case of the copy mode is staging data for compute jobs.
As such it does not validate after the copy completes that the source was not modified in the meantime.
For performance it also does not perform any checksum verification when comparing the source/destination.
If used as part of a backup or other workload where strict consistency guarantees are required, users
should take measures to perform additional verification the source and destination match bit-for-bit.
`, config.NumWorkersKey),
		RunE: func(cmd *cobra.Command, args []string) error {
			_, err := os.Stat(beegfsCopyPath)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("to use this mode first install the beegfs-copy package")
				} else {
					return fmt.Errorf("error checking for required component at %s: %w (verify the beegfs-copy package is properly installed)", beegfsCopyPath, err)
				}
			}
			if mgmtdClient, err := config.ManagementClient(); err != nil {
				return err
			} else {
				if _, err := mgmtdClient.VerifyLicense(cmd.Context(), featureString); err != nil {
					return err
				}
			}
			if args[0] == "-" {
				if len(args) != 2 {
					return fmt.Errorf("when reading source paths from stdin only '-' and the destination path should be specified")
				}
				return copyUsingStdin(cmd.Context(), frontendCfg, bflagSet, args[1])
			}
			return copyRunner(bflagSet, args[:len(args)-1], args[len(args)-1])
		},
	}
	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("machine-file", "m", "Path to a file containing a list of nodes that should be used to execute this copy.", "-m", ""),
		bflag.GlobalFlag(config.NumWorkersKey, "-t"),
		bflag.Flag("nodes", "n", "Start this many from machine file. If set to zero all nodes will be used.", "-n", 0),
		bflag.Flag("keep-atime", "a", "Do not modify the access time of the source file(s).", "-a", false),
		bflag.Flag("chunksize", "c", "Chunk size for copy operations (in MB).", "-c", ""),
		bflag.Flag("keep-mtime", "k", "Keep the original modification time from the source in the destination.", "-k", false),
		bflag.Flag("partition-threshold", "p", "Partition copy threshold (in MB).", "-p", ""),
		bflag.Flag("list-diff", "l", `Do not make any changes and just list differences between the source and destination paths. 
This mode will list missing files and directories, and files with different sizes or older
modification times in the destination. Only one source path is accepted, and can be either 
a file or directory, however if the destination is a file the source must also be a file.`,
			"-l", false),
		bflag.Flag("update", "u", `The update mode compares source and destination paths, updating the destination by copying 
only missing files and directories and files with different sizes or older modification 
times in the destination. Only one source path accepted and can be either a file or
directory, however if the destination is a file the source must be a file as well.`,
			"-u", false),
		bflag.Flag("always-update", "d", `By default when using the list-diff and update modes, only files where the modification
time on the source is newer than the destination will be marked as needing an update.
Optionally this flag can be set to always require an update if files in the source and
destination have different modification times.`,
			"-d", false),
		bflag.Flag("statistics", "s", "Print thread statistics.", "-s", false),
		// Note I have yet to find a good way to detect if the debug flag is set and automatically set "-v 3".
		bflag.Flag("verbose", "v", "Increase output verbosity. Levels 1-3 are supported.", "-v", ""),
	}

	bflagSet = bflag.NewFlagSet(copyFlags, cmd)
	cmd.Flags().StringVar(&frontendCfg.stdinDelimiter, "stdin-delimiter", "\\n", `Change the string delimiter used to determine individual paths when read from stdin.
		For example use --stdin-delimiter=\"\\x00\" for NULL.`)
	cmd.Flags().IntVar(&frontendCfg.batchSize, "stdin-batch", 1024, "At most this many paths will be read from stdin before triggering the parallel copy. Setting to higher values will consume more memory.")
	cmd.MarkFlagRequired("machine-file")
	cmd.Flags().MarkHidden("copy-help")
	return cmd
}

func copyRunner(bflagSet *bflag.FlagSet, paths []string, dest string) error {
	logger, _ := config.GetLogger()
	log := logger.With(zap.String("component", "copyRunner"))

	wrappedArgs := bflagSet.WrappedArgs()
	log.Debug("starting copy runner", zap.Any("wrappedArgs", wrappedArgs), zap.Any("paths", paths), zap.Any("destination", dest))

	allArgs := make([]string, 0, len(wrappedArgs)+len(paths)+1)
	allArgs = append(allArgs, wrappedArgs...)
	allArgs = append(allArgs, paths...)
	allArgs = append(allArgs, dest)
	c := exec.Command(beegfsCopyPath, allArgs...)
	c.Stdout = os.Stdout
	c.Stderr = os.Stderr
	if err := c.Start(); err != nil {
		return fmt.Errorf("unable to start copy: %w", err)
	}
	if err := c.Wait(); err != nil {
		return fmt.Errorf("error waiting for copy to complete: %w", err)
	}
	return nil
}

func copyUsingStdin(ctx context.Context, frontendCfg frontendCfg, bflagSet *bflag.FlagSet, destination string) error {
	logger, _ := config.GetLogger()
	log := logger.With(zap.String("component", "copyUsingStdin"))

	pathsChan := make(chan string, frontendCfg.batchSize)
	stdinErrChan := make(chan error, 1)
	d, err := util.GetStdinDelimiterFromString(frontendCfg.stdinDelimiter)
	if err != nil {
		return err
	}

	go util.ReadFromStdin(ctx, d, pathsChan, stdinErrChan)
	reachedEOF := false
	for {
		log.Debug("reading paths from stdin")
		paths, err := readBatchFromStdin(ctx, pathsChan, stdinErrChan, frontendCfg.batchSize)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			log.Debug("stdin returned EOF")
			reachedEOF = true
		}
		if len(paths) > 0 {
			copyRunner(bflagSet, paths, destination)
		} else if reachedEOF {
			return nil
		}
	}
}

func readBatchFromStdin(ctx context.Context, pathsChan <-chan string, errChan <-chan error, batchSize int) ([]string, error) {
	paths := make([]string, 0)
	for i := 0; i < batchSize; i++ {
		select {
		case <-ctx.Done():
			return nil, context.Canceled
		case path, ok := <-pathsChan:
			// When the channel is closed check for an error otherwise we're at EOF.
			if !ok {
				select {
				case err, ok := <-errChan:
					if ok {
						return paths, err
					}
				default:
					return paths, io.EOF
				}
			}
			paths = append(paths, path)
		}
	}
	return paths, nil
}
