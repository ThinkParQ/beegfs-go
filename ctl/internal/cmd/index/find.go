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
		bflag.Flag("version", "v", "Show program's version number and exit.", "--version", false),
		bflag.Flag("maxdepth", "", "Descend at most levels (a non-negative integer) levels of directories below the command line arguments. -maxdepth 0 means only apply the tests and actions to the command line arguments.", "-maxdepth", ""),
		bflag.Flag("mindepth", "", "Do not apply any tests or actions at levels less than levels (a non-negative integer). -mindepth 1 means process all files except the command line arguments.", "-mindepth", ""),
		bflag.Flag("amin", "", "File was last accessed n minutes ago.", "-amin", ""),
		bflag.Flag("anewer", "", "File was last accessed more recently than file was modified.", "-anewer", ""),
		bflag.Flag("atime", "", "File was last accessed n*24 hours ago.", "-atime", ""),
		bflag.Flag("cmin", "", "File's status was last changed n minutes ago.", "-cmin", ""),
		bflag.Flag("cnewer", "", "File's status was last changed more recently than file was modified.", "-cnewer", ""),
		bflag.Flag("ctime", "", "File's status was last changed n*24 hours ago.", "-ctime", ""),
		bflag.Flag("empty", "", "File is empty and is either a regular file or a directory.", "-empty", false),
		bflag.Flag("executable", "", "Matches files which are executable and directories which are searchable (in a file name resolution sense).", "-executable", false),
		bflag.Flag("false", "", "File is false and is either a regular file or a directory.", "-false", false),
		bflag.Flag("gid", "", "File's numeric group ID is n.", "-gid", ""),
		bflag.Flag("group", "", "File belongs to group gname (numeric group ID allowed).", "-group", ""),
		bflag.Flag("iname", "", "Like -name, but the match is case insensitive (uses regex, not glob).", "-iname", ""),
		bflag.Flag("inum", "", "File has inode number n. It is normally easier to use the -samefile test instead.", "-inum", ""),
		bflag.Flag("iregex", "", "Like -regex, but the match is case insensitive.", "-iregex", ""),
		bflag.Flag("links", "", "File has n links.", "-links", ""),
		bflag.Flag("lname", "", "File is a symbolic link whose contents match shell pattern.", "-lname", ""),
		bflag.Flag("mmin", "", "File's data was last modified n minutes ago.", "-mmin", ""),
		bflag.Flag("mtime", "", "File's data was last modified n*24 hours ago.", "-mtime", ""),
		bflag.Flag("name", "", "Base of file name (the path with the leading directories removed) matches shell pattern.", "-name", ""),
		bflag.Flag("newer", "", "File was modified more recently than file.", "-newer", ""),
		bflag.Flag("path", "", "File name matches shell pattern.", "-path", ""),
		bflag.Flag("readable", "", "Matches files which are readable.", "-readable", false),
		bflag.Flag("regex", "", "File name matches regular expression pattern.", "-regex", ""),
		bflag.Flag("samefile", "", "File refers to the same inode as name.", "-samefile", ""),
		bflag.Flag("size", "", "File's size matches the specified criteria. Note: for sizes less than some value (e.g. -2k), use --size=-2k so the value isn't treated as a flag.", "-size", "", bflag.WithEquals()),
		bflag.Flag("true", "", "Always true.", "-true", false),
		bflag.Flag("type", "", "File is of type c.", "-type", ""),
		bflag.Flag("uid", "", "File's numeric user ID is n.", "-uid", ""),
		bflag.Flag("user", "", "File is owned by user uname.", "-user", ""),
		bflag.Flag("writable", "", "Matches files which are writable.", "-writable", false),
		bflag.Flag("num-results", "", "First n results.", "--num-results", 0),
		bflag.Flag("smallest", "", "Sort output by size, ascending.", "--smallest", false),
		bflag.Flag("largest", "", "Sort output by size, descending.", "--largest", false),
		bflag.Flag("compress", "", "Try to reduce memory usage by compressing with zlib (if available).", "--compress", false),
		bflag.Flag("delim", "", "Delimiter separating output columns", "--delim", " "),
		bflag.Flag("in-memory-name", "", "Name of in-memory database when aggregation is performed.", "--in-memory-name", "out"),
		bflag.Flag("aggregate-name", "", "Name of final database when aggregation is performed.", "--aggregate-name", ""),
		bflag.Flag("skip-file", "", "Name of file containing directory basenames to skip.", "--skip-file", ""),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)
	if err := cmd.Flags().MarkHidden("in-memory-name"); err != nil {
		return nil
	}
	if err := cmd.Flags().MarkHidden("aggregate-name"); err != nil {
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
