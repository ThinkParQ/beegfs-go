package index

import (
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

const dir2IndexPluginPath = "/usr/local/lib/libbeegfs_plugin.so"

func newGenericCreateCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet
	var fsPath string
	var indexPath string
	var summary bool
	var onlySummary bool

	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, err := parseIndexAddr(indexAddr)
			if err != nil {
				return err
			}
			if !onlySummary {
				if err := checkIndexConfig(backend, beeBinary); err != nil {
					return err
				}
			}
			if summary || onlySummary {
				if err := checkIndexConfig(backend, treeSummaryBinary); err != nil {
					return err
				}
			}
			runOpts := createRunOptions{
				fsPath:      fsPath,
				indexPath:   indexPath,
				summary:     summary,
				onlySummary: onlySummary,
				summaryOpts: treeSummaryOptions{
					threads: viper.GetInt(config.NumWorkersKey),
					debug:   viper.GetBool(config.DebugKey),
				},
			}
			return runPythonCreateIndex(bflagSet, backend, runOpts)
		},
	}

	bflagSet = bflag.NewFlagSet(commonIndexFlags, cmd)
	cmd.Flags().StringVarP(&fsPath, "fs-path", "F",
		"", "File system path for which index will be created.")
	cmd.Flags().StringVarP(&indexPath, "index-path", "I",
		"", "GUFI tree path at which the index will be stored.")
	cmd.Flags().BoolVarP(&summary, "summary", "s", false,
		"Create tree summary table along with other tables.")
	cmd.Flags().BoolVarP(&onlySummary, "only-summary", "S", false,
		"Create only tree summary table.")
	cmd.MarkFlagRequired("fs-path")
	cmd.MarkFlagRequired("index-path")
	cmd.MarkFlagsMutuallyExclusive("summary", "only-summary")
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

Example: Create the index and then generate a full tree summary:

$ beegfs index create --fs-path /mnt/fs --index-path /mnt/index --summary
`
	return s
}

type treeSummaryOptions struct {
	threads int
	debug   bool
}

type createRunOptions struct {
	fsPath      string
	indexPath   string
	summary     bool
	onlySummary bool
	summaryOpts treeSummaryOptions
}

func runPythonCreateIndex(bflagSet *bflag.FlagSet, backend indexBackend, opts createRunOptions) error {
	if !opts.onlySummary {
		wrappedArgs := bflagSet.WrappedArgs()
		dirArgs, err := buildDir2IndexArgs(opts.fsPath, opts.indexPath, wrappedArgs)
		if err != nil {
			return err
		}
		if err := runIndexCommandWithPrint(backend, beeBinary, dirArgs, "Running GUFI dir2index command",
			zap.String("indexAddr", indexAddr),
			zap.String("fsPath", opts.fsPath),
			zap.String("indexPath", opts.indexPath),
			zap.Any("wrappedArgs", wrappedArgs),
			zap.Any("dirArgs", dirArgs),
		); err != nil {
			return err
		}
	}
	if opts.summary || opts.onlySummary {
		treeArgs, err := buildTreeSummaryArgs(opts.indexPath, opts.summaryOpts)
		if err != nil {
			return err
		}
		return runIndexCommandWithPrint(backend, treeSummaryBinary, treeArgs, "Running GUFI treesummary command",
			zap.String("indexAddr", indexAddr),
			zap.String("indexPath", opts.indexPath),
			zap.Any("treeArgs", treeArgs),
		)
	}
	return nil
}

func buildDir2IndexArgs(fsPath, indexPath string, wrappedArgs []string) ([]string, error) {
	args := buildDir2IndexBaseArgs(wrappedArgs)
	args = append(args, fsPath, indexPath)
	return args, nil
}

func buildDir2IndexBaseArgs(wrappedArgs []string) []string {
	args := make([]string, 0, len(wrappedArgs)+2)
	noMetadata := false
	for i := 0; i < len(wrappedArgs); i++ {
		arg := wrappedArgs[i]
		if arg == "-B" {
			noMetadata = true
		}
		if arg == "--plugin" {
			if i+1 < len(wrappedArgs) {
				i++
			}
			continue
		}
		if strings.HasPrefix(arg, "--plugin=") {
			continue
		}
		args = append(args, arg)
	}
	if !noMetadata {
		args = append(args, "--plugin", dir2IndexPluginPath)
	}
	return args
}

func buildTreeSummaryArgs(indexPath string, opts treeSummaryOptions) ([]string, error) {
	args := make([]string, 0, 12)
	if opts.debug {
		args = append(args, "-H")
	}
	if opts.threads > 0 {
		args = append(args, "-n", strconv.Itoa(opts.threads))
	}
	args = append(args, indexPath)
	return args, nil
}
