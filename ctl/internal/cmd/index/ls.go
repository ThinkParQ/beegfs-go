package index

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func newLsCmd() *cobra.Command {
	backendCfg := indexPkg.LsCfg{}
	var human bool

	cmd := &cobra.Command{
		Use:         "ls [path]",
		Short:       "Lists the contents of the index directory.",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Long: `Display the contents of an indexed directory, similar to ls(1).

By default dotfiles (names beginning with '.') are hidden; use -a to show them.
Sort order is by name ascending unless overridden by -S or -t.

Example: long listing of /mnt/beegfs/data

  beegfs index ls -l /mnt/beegfs/data

Example: show all files sorted by size, human-readable sizes

  beegfs index ls -lhS /mnt/beegfs/data

Example: recursive listing

  beegfs index ls -R /mnt/beegfs/data

Example: include BeeGFS metadata

  beegfs index ls -l --beegfs /mnt/beegfs/data
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()

			exec, err := newExecutor()
			if err != nil {
				return err
			}
			indexPath, err := resolveIndexPath(args)
			if err != nil {
				return err
			}

			log.Debug("running beegfs index ls",
				zap.String("indexPath", indexPath),
				zap.Any("cfg", backendCfg),
			)

			rows, errWait, err := indexPkg.Ls(cmd.Context(), exec, backendCfg, indexPath)
			if err != nil {
				return err
			}

			// allColumns defines every possible column in ls-style display order.
			// The last positional name/path column name varies by --recursive.
			displayNameCol := "name"
			if backendCfg.Recursive {
				displayNameCol = "path"
			}
			allColumns := []string{
				"permissions", "nlink", "user", "group", "size",
				"mtime", "atime", "ctime", "inode", "type", displayNameCol,
			}
			if backendCfg.BeeGFS {
				allColumns = append(allColumns, "owner_id", "parent_entry_id", "entry_id", "stripe_targets")
			}

			// defaultColumns: short = name/path only; long = ls -l style.
			var defaultColumns []string
			if backendCfg.Long {
				defaultColumns = []string{"permissions", "nlink", "user", "group", "size", "mtime", displayNameCol}
			} else {
				defaultColumns = []string{displayNameCol}
			}
			if backendCfg.ShowInode {
				defaultColumns = append([]string{"inode"}, defaultColumns...)
			}
			if backendCfg.BeeGFS {
				if backendCfg.Long {
					defaultColumns = append(defaultColumns, "entry_id", "owner_id", "stripe_targets")
				} else {
					defaultColumns = append(defaultColumns, "entry_id")
				}
			}
			if viper.GetBool(config.DebugKey) {
				defaultColumns = allColumns
			}

			tbl := cmdfmt.NewPrintomatic(allColumns, defaultColumns)
			defer tbl.PrintRemaining()

		run:
			for {
				select {
				case <-cmd.Context().Done():
					return cmd.Context().Err()
				case row, ok := <-rows:
					if !ok {
						break run
					}
					tbl.AddItem(toAny(formatLsRow(row, backendCfg.Recursive, backendCfg.BeeGFS, human))...)
				}
			}

			return errWait()
		},
	}

	// cobra reserves -h for --help by default. Pre-register --help without a
	// shorthand so cobra skips claiming -h, leaving it free for --human-readable
	// (matching real ls(1) behavior). --help still works via the long flag.
	cmd.Flags().Bool("help", false, "help for ls")

	// Filter flags
	cmd.Flags().StringVar(&backendCfg.Name, "name", "", "Only list entries whose name matches shell pattern.")
	cmd.Flags().StringVar(&backendCfg.Type, "type", "", "Only list entries of type: f, d, l.")
	cmd.Flags().BoolVarP(&backendCfg.All, "all", "a", false, "Show all files including hidden (names starting with '.').")
	cmd.Flags().BoolVarP(&backendCfg.AlmostAll, "almost-all", "A", false, "Like -a (. and .. are not indexed in GUFI).")
	cmd.Flags().BoolVarP(&backendCfg.IgnoreBackups, "ignore-backups", "B", false, "Ignore entries whose names end with ~.")

	// Display flags
	cmd.Flags().BoolVarP(&backendCfg.Long, "long", "l", false, "Long listing format: permissions, links, user, group, size, time, name.")
	cmd.Flags().BoolVarP(&backendCfg.ShowInode, "inode", "i", false, "Print the index node number of each file.")
	cmd.Flags().BoolVarP(&human, "human-readable", "h", false, "With -l, print sizes like 1.5K, 2.3M, 4.0G.")
	cmd.Flags().BoolVar(&backendCfg.BeeGFS, "beegfs", false, "Include BeeGFS metadata columns.")

	// Traversal flags
	cmd.Flags().BoolVarP(&backendCfg.Recursive, "recursive", "R", false, "List subdirectories recursively.")

	// Sort flags
	cmd.Flags().BoolVarP(&backendCfg.Reverse, "reverse", "r", false, "Reverse the sort order.")
	cmd.Flags().BoolVarP(&backendCfg.SortBySize, "sort-by-size", "S", false, "Sort by file size, largest first.")
	cmd.Flags().BoolVarP(&backendCfg.SortByMtime, "sort-by-mtime", "t", false, "Sort by modification time, newest first.")

	// Output control
	cmd.Flags().IntVarP(&backendCfg.NumResults, "num-results", "n", 0, "Return at most N entries per directory (0 = unlimited).")

	return cmd
}

// formatLsRow maps a raw GUFI ls result row to formatted values in allColumns order.
//
// GUFI raw column positions (from SQL templates):
//
//	Non-recursive: name(0) type(1) inode(2) size(3) mtime(4) atime(5) ctime(6) mode(7) uid(8) gid(9) nlink(10)
//	Recursive:     path(0) name(1) type(2) inode(3) size(4) mtime(5) atime(6) ctime(7) mode(8) uid(9) gid(10) nlink(11)
//	+BeeGFS:       ...nlink(N) owner_id(N+1) parent_entry_id(N+2) entry_id(N+3) stripe_num_targets(N+4)
//
// Output positions match allColumns:
//
//	[permissions, nlink, user, group, size, mtime, atime, ctime, inode, type, name/path, ...]
func formatLsRow(raw []string, recursive, beegfs, human bool) []string {
	off := 0
	var pathStr string
	if recursive {
		pathStr = safeGet(raw, off)
		off++
	}
	name := safeGet(raw, off)
	off++
	typeChar := safeGet(raw, off)
	off++
	inode := safeGet(raw, off)
	off++
	size := safeGet(raw, off)
	off++
	mtime := safeGet(raw, off)
	off++
	atime := safeGet(raw, off)
	off++
	ctime := safeGet(raw, off)
	off++
	modeStr := safeGet(raw, off)
	off++
	uid := safeGet(raw, off)
	off++
	gid := safeGet(raw, off)
	off++
	nlink := safeGet(raw, off)
	off++

	displaySize := size
	if human {
		displaySize = fmtSizeHuman(size)
	}

	// For recursive, the last column is the full rpath; otherwise it's just the name.
	displayEntry := name
	if recursive {
		displayEntry = pathStr
	}

	result := []string{
		modeString(modeStr, typeChar), // permissions
		nlink,
		lookupUID(uid),
		lookupGID(gid),
		displaySize,
		fmtTimestamp(mtime),
		fmtTimestamp(atime),
		fmtTimestamp(ctime),
		inode,
		typeChar,
		displayEntry,
	}

	if beegfs {
		result = append(result,
			safeGet(raw, off+0), // owner_id
			safeGet(raw, off+1), // parent_entry_id
			safeGet(raw, off+2), // entry_id
			safeGet(raw, off+3), // stripe_num_targets
		)
	}

	return result
}
