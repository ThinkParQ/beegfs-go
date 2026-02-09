package copy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"go.uber.org/zap"
)

const (
	beegfsCopyPath = "/opt/beegfs/sbin/beegfs-copy"
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
		Use:     "copy -m <machine-file> <source> [<source>] <destination>",
		Args:    cobra.MinimumNArgs(2),
		Aliases: []string{"cp"},
		Short:   "Copy files and directories in parallel",
		Long: `Copy files and directories in parallel.

SPECIFYING PATHS:
One or more <source> paths can be copied under a <destination> directory. The <destination> must be a directory (not a file).
When supported by the current shell, standard wildcards (globbing patterns) can be used in each path.
Alternatively paths can be provided using stdin by using '-' as the <source> (example: 'cat file_list.txt | beegfs copy - <destination>').
When copying using multiple nodes, ensure the source path(s) and destination are mounted to the same location on all nodes.

PARALLELISM:
This mode supports two levels of parallelism:

* Thread parallelism using multiple threads per node based on the --threads flag (by default the number of cores on this machine).
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

EXAMPLES:
Copy file1 and file2 to /path2/file1 and /path2/file2 using a single thread per node (-t 1):

  $ beegfs copy -m /tmp/machinefile -t 1 /path1/file1 /path1/file1 /path2

Copy folder /many_files/ and its contents to /path2/many_files/ using 8 threads per node (-t 8):

  $ beegfs copy -m /tmp/machinefile -t 8 /path1/many_files/ /path2

Copy two files, one 100MB and one 200GB with a partition threshold of 5GB (-p 5120):

  $ beegfs copy -m /tmp/machinefile -t 8 -p 5120 /path1/file100MB.file /path1/file200GB.file /path2

Copy one file specifying the chunksize for each thread as 512MB (-c 512):

  $ beegfs copy m /tmp/machinefile -t 8 -c 512 /path1/bigData.file /path2

Copy a file with all possible options enabled: 

  $ beegfs copy -m /tmp/machinefile -n 4 -t 8 -p 1024 -c 512 -v 3 -s /path1/file_256GB.file /path2

This would copy using first four nodes in the machinefile with 8 threads per node, a partition threshold
of 1024MB and chunksize of 512MB while printing out statistics and detailed logs about the copy process.
The nodes, threads, and partition configuration would result in 32 threads each copying 8GB of data.

Compare the contents of one folder with another:

  $ beegfs copy -m /tmp/machinefile -t 8 /path1/many_files_and_dir /path2/many_files_and_dir -l

This would list the missing subdirectories and files in the destination path, along with any files
with different sizes or older modification timestamps in the destination. No changes will be made.
This mode only accepts once source path which can be a file or directory, but the destination type
must match the source type.

Update a destination path and its contents based on the source path:

  $ beegfs-copy -m /tmp/machinefile -t 8 /path1/many_files_and_dir /path2/many_files_and_dir -u

This will compare the source and destination path, only copying contents that are missing based on the
same rules defined above for comparing source and destination paths.

LIMITATIONS:
* Modifying the source/destination directory tree structure while copy is running is discouraged, and
  alterations could lead to incomplete copies of the directory. The update mode when running on a 
  single node does not have this limitations; only multi-node updates that must first prepare a list of
  changed files before distributing them across multiple nodes is affected. The subsequent copy process
  is agnostic to modifications of the directory structure.
* When copying large directory structures, the copy mode could reach the open files limit (ulimit -n)
  and exit return a message like: "Failed to open directory". If necessary, increase this limit.
* Avoid copying multiple source paths with the same base name, since the final copy may be different
  from what is expected. For example avoid a copy like:
  * $ beegfs copy -m machinefile dir1/dir dir2/dir dir3/dir destination

IMPORTANT: The primary use case of the copy mode is staging data for compute jobs.
As such it does not validate after the copy completes that the source was not modified in the meantime.
For performance it also does not perform any checksum verification when comparing the source/destination.
If used as part of a backup or other workload where strict consistency guarantees are required, users
should take measures to perform additional verification the source and destination match bit-for-bit.`,
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
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

	defaultThreads := runtime.GOMAXPROCS(0)
	if defaultThreads > 1 {
		// If there is more than one CPU, reserve one for CTL itself.
		defaultThreads = defaultThreads - 1
	}

	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("machine-file", "m", "Path to a file containing a list of nodes that should be used to execute this copy.", "-m", ""),
		bflag.Flag("threads", "t", "Number of threads per node. The default is the number of CPUs on this machine minus one, or one if there is a single CPU.", "-t", defaultThreads),
		bflag.Flag("nodes", "n", "Start the copy using this many nodes from the machine file (selected top to bottom). The default (0) will use all nodes.", "-n", 0),
		bflag.Flag("keep-atime", "a", `Do not modify the access time of the source file(s). 
This only works if supported and enabled on the source file system.`, "-a", false),
		bflag.Flag("chunksize", "c", `Chunk size for copy operations (in MB).
Default chunk size: 128 MB.`, "-c", 128),
		bflag.Flag("keep-mtime", "k", `Keep the original modification time from the source in the destination. 
This only works if supported and enabled on the destination file system.`, "-k", false),
		bflag.Flag("preserve-ownership", "O", "Preserve ownership (user/group) from source (requires root permissions). By default ownership of files in the destination is determined by the effective user/group ID of the Copy process (8.3+).", "-O", false),
		bflag.Flag("preserve-permissions", "P", "Preserve permission bits when updating existing entries. By default only new entries will inherit their permission bits from the source (8.3+).", "-P", false),
		bflag.Flag("partition-threshold", "p", `Partition copy threshold (in MB).
To accelerate the copy operation, large files are partitioned and copied using multiple threads.
Files with a size below the partition-threshold are processed by a single thread.
Default partition threshold: 1024 MB.`, "-p", 1024),
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
		bflag.Flag("verbose", "v", `Increase output verbosity. Levels 1-3 are supported.
1 - Print general information about the copy process such as the specified path, start, and finish. 
2 - Also print the files and folders that were copied.
3 - Debug logging including details about the reading and writing process.`, "-v", ""),
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
	log, _ := config.GetLogger()

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
	err := c.Wait()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			log.Debug("beegfs-copy exited with non-zero status, propagating exit code", zap.Error(err))
			os.Exit(exitErr.ExitCode())
		}
		return fmt.Errorf("error waiting for copy to complete: %w", err)
	}
	return nil
}

func copyUsingStdin(ctx context.Context, frontendCfg frontendCfg, bflagSet *bflag.FlagSet, destination string) error {
	log, _ := config.GetLogger()

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
