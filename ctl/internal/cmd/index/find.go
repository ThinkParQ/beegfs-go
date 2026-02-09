package index

import (
	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/bflag"
	"go.uber.org/zap"
)

func newGenericFindCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		RunE: func(cmd *cobra.Command, args []string) error {
			backend, err := parseIndexAddr(indexAddr)
			if err != nil {
				return err
			}
			if err := checkIndexConfig(backend, findBinary); err != nil {
				return err
			}
			paths, err := defaultIndexPaths(backend, args)
			if err != nil {
				return err
			}
			return runPythonFindIndex(bflagSet, backend, paths)
		},
	}

	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("maxdepth", "", "Descend at most levels (a non-negative integer) levels of directories.", "-maxdepth", ""),
		bflag.Flag("mindepth", "", "Do not apply any tests or actions at levels less than levels.", "-mindepth", ""),
		bflag.Flag("version", "v", "Version of the find command.", "--version", false),
		bflag.Flag("amin", "", "File was last accessed N minutes ago.", "-amin", ""),
		bflag.Flag("atime", "", "File was last accessed N*24 hours ago.", "-atime", ""),
		bflag.Flag("cmin", "", "File's status was last changed N minutes ago.", "-cmin", ""),
		bflag.Flag("ctime", "", "File's status was last changed N*24 hours ago.", "-ctime", ""),
		bflag.Flag("empty", "", "File is empty and is either a regular file or a directory.", "-empty", false),
		bflag.Flag("executable", "", "Matches files which are executable and directories which are searchable.", "-executable", false),
		bflag.Flag("false", "", "File is false and is either a regular file or a directory.", "-false", false),
		bflag.Flag("gid", "", "File's numeric group ID is N.", "-gid", ""),
		bflag.Flag("group", "", "File belongs to group gname (numeric group ID allowed).", "-group", ""),
		bflag.Flag("inum", "", "File has inode number N.", "-inum", ""),
		bflag.Flag("links", "", "File has N links.", "-links", ""),
		bflag.Flag("mmin", "", "File's data was last modified N minutes ago.", "-mmin", ""),
		bflag.Flag("mtime", "", "File's data was last modified N*24 hours ago.", "-mtime", ""),
		bflag.Flag("name", "", "Base of file name matches shell pattern.", "-name", ""),
		bflag.Flag("entryID", "", "Get file path for the given BeeGFS entryID.", "-entryID", ""),
		bflag.Flag("ownerID", "", "Get list of files whose metadata is owned by given BeeGFS Metadata node ID.", "-ownerID", ""),
		bflag.Flag("targetID", "", "Get list of files whose data is present on given BeeGFS targetID.", "-targetID", ""),
		bflag.Flag("newer", "", "File was modified more recently than file.", "-newer", ""),
		bflag.Flag("path", "", "File name matches shell pattern pattern.", "-path", ""),
		bflag.Flag("readable", "", "Matches files which are readable.", "-readable", false),
		bflag.Flag("samefile", "", "File refers to the same inode as name.", "-samefile", ""),
		bflag.Flag("size", "", "File's size matches the specified criteria.", "-size", "", bflag.WithEquals()),
		bflag.Flag("fprint", "", "Output file prefix (Creates file <output>.tid)", "-fprint", false),
		bflag.Flag("printf", "", "print format on the standard output, "+
			"similar to GNU find", "-printf", ""),
		bflag.Flag("true", "", "Always true.", "-true", false),
		bflag.Flag("type", "", "File is of type c.", "-type", ""),
		bflag.Flag("uid", "", "File's numeric user ID is N.", "-uid", ""),
		bflag.Flag("user", "", "File is owned by user uname.", "-user", ""),
		bflag.Flag("writable", "", "Matches files which are writable.", "-writable", false),
		bflag.Flag("num-results", "", "First n results.", "--num-results", 0),
		bflag.Flag("smallest", "", "Top n smallest files.", "--smallest", false),
		bflag.Flag("largest", "", "Top n largest files.", "--largest", false),
		bflag.Flag("in-memory-name", "", "In-memory name for processing.", "--in-memory-name", "out"),
		bflag.Flag("delim", "", "Delimiter separating output columns", "--delim", " "),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)
	err := cmd.Flags().MarkHidden("in-memory-name")
	if err != nil {
		return nil
	}

	return cmd
}

func newFindCmd() *cobra.Command {
	s := newGenericFindCmd()
	s.Use = "find"
	s.Short = "Searches for files in the index."

	s.Long = `Search for files in the index directory using query options.

This command provides similar options to GNU find, but Hive's find is significantly faster 
than running traditional find commands on the filesystem.

Example: List files in the index directory that are larger than 1GB.

$ beegfs index find --size +1G
`
	return s
}

func runPythonFindIndex(bflagSet *bflag.FlagSet, backend indexBackend, paths []string) error {
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+len(paths))
	allArgs = append(allArgs, paths...)
	allArgs = append(allArgs, wrappedArgs...)
	return runIndexCommandWithPrint(backend, findBinary, allArgs, "Running GUFI find command",
		zap.String("indexAddr", indexAddr),
		zap.Any("wrappedArgs", wrappedArgs),
		zap.Any("paths", paths),
		zap.Any("allArgs", allArgs),
	)
}
