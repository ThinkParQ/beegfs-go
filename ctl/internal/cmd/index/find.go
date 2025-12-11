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

func newGenericFindCmd() *cobra.Command {
	var bflagSet *bflag.FlagSet

	var cmd = &cobra.Command{
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkBeeGFSConfig(); err != nil {
				return err
			}
			var paths []string
			if len(args) > 0 {
				paths = args
			} else {
				cwd, err := os.Getwd()
				if err != nil {
					return err
				}
				paths = []string{cwd}
			}
			return runPythonFindIndex(bflagSet, paths)
		},
	}

	copyFlags := []bflag.FlagWrapper{
		bflag.Flag("maxdepth", "", "Descend at most levels (a non-negative integer) levels of directories.", "-maxdepth", ""),
		bflag.Flag("mindepth", "", "Do not apply any tests or actions at levels less than levels.", "-mindepth", ""),
		bflag.Flag("version", "v", "Version of the find command.", "--version", false),
		bflag.Flag("amin", "", "File was last accessed N minutes ago.", "-amin", ""),
		bflag.Flag("anewer", "", "File was last accessed more recently than file was modified.", "-anewer", ""),
		bflag.Flag("atime", "", "File was last accessed N*24 hours ago.", "-atime", ""),
		bflag.Flag("cmin", "", "File's status was last changed N minutes ago.", "-cmin", ""),
		bflag.Flag("cnewer", "", "File's status was last changed more recently than file was modified.", "-cnewer", ""),
		bflag.Flag("ctime", "", "File's status was last changed N*24 hours ago.", "-ctime", ""),
		bflag.Flag("empty", "", "File is empty and is either a regular file or a directory.", "-empty", false),
		bflag.Flag("executable", "", "Matches files which are executable and directories which are searchable.", "-executable", false),
		bflag.Flag("false", "", "File is false and is either a regular file or a directory.", "-false", false),
		bflag.Flag("gid", "", "File's numeric group ID is N.", "-gid", ""),
		bflag.Flag("group", "", "File belongs to group gname (numeric group ID allowed).", "-group", ""),
		bflag.Flag("iname", "", "Like -name, but the match is case insensitive (uses regex, not glob).", "-iname", ""),
		bflag.Flag("inum", "", "File has inode number N.", "-inum", ""),
		bflag.Flag("iregex", "", "Like -regex, but the match is case insensitive.", "-iregex", ""),
		bflag.Flag("lname", "", "File is a symbolic link whose contents match shell pattern.", "-lname", ""),
		bflag.Flag("links", "", "File has N links.", "-links", ""),
		bflag.Flag("mmin", "", "File's data was last modified N minutes ago.", "-mmin", ""),
		bflag.Flag("mtime", "", "File's data was last modified N*24 hours ago.", "-mtime", ""),
		bflag.Flag("name", "", "Base of file name matches shell pattern.", "-name", ""),
		bflag.Flag("newer", "", "File was modified more recently than file.", "-newer", ""),
		bflag.Flag("path", "", "File name matches shell pattern pattern.", "-path", ""),
		bflag.Flag("readable", "", "Matches files which are readable.", "-readable", false),
		bflag.Flag("regex", "", "File name matches regular expression pattern.", "-regex", ""),
		bflag.Flag("samefile", "", "File refers to the same inode as name.", "-samefile", ""),
		bflag.Flag("size", "", "File's size matches the specified criteria.", "-size", ""),
		bflag.Flag("fprint", "", "Output result to file", "-fprint", ""),
		bflag.Flag("ls", "", "List current file in ls -dils format", "-ls", false),
		bflag.Flag("print", "", "Print the full name on stdout", "-print", false),
		bflag.Flag("print0", "", "Print the full name followed by a null character", "-print0", false),
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
		bflag.Flag("aggregate-name", "", "Name of final database when aggregation is performed", "--aggregate-name", "aggregate"),
		bflag.Flag("skip-file", "", "Name of file containing directory basenames to skip", "--skip-file", ""),
		bflag.Flag("compress", "", "Try to reduce memory usage by compressing with zlib (if available)", "--compress", false),
		bflag.Flag("verbose", "V", "Show the gufi_query being executed", "--verbose", false),
		bflag.Flag("delim", "", "Delimiter separating output columns", "--delim", " "),
	}
	bflagSet = bflag.NewFlagSet(copyFlags, cmd)

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

func runPythonFindIndex(bflagSet *bflag.FlagSet, paths []string) error {
	log, _ := config.GetLogger()
	wrappedArgs := bflagSet.WrappedArgs()
	allArgs := make([]string, 0, len(wrappedArgs)+len(paths)+2)
	allArgs = append(allArgs, paths...)
	allArgs = append(allArgs, wrappedArgs...)
	log.Debug("Running BeeGFS Hive Index find command",
		zap.Any("wrappedArgs", wrappedArgs),
		zap.String("findBinary", findBinary),
		zap.Any("paths", paths),
		zap.Any("allArgs", allArgs),
	)
	cmd := exec.Command(findBinary, allArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("unable to start index command: %w", err)
	}
	err = cmd.Wait()
	if err != nil {
		return fmt.Errorf("error executing command: %v", err)
	}
	return nil
}
