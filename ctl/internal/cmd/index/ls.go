package index

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func newLsCmd(globalCfg *indexPkg.GlobalCfg, parentFlags *pflag.FlagSet) *cobra.Command {
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

			if backendCfg.Type != "" && backendCfg.Type != "f" && backendCfg.Type != "d" && backendCfg.Type != "l" {
				return fmt.Errorf("--type must be f, d, or l (got %q)", backendCfg.Type)
			}

			cfg, args := resolveCfgAndPaths(cmd.Context(), globalCfg, args)
			backendCfg.GlobalCfg = cfg
			paths, err := resolveIndexPaths(cmd.Context(), cfg, args)
			if err != nil {
				return err
			}
			exec, err := indexPkg.NewExecutor(cfg)
			if err != nil {
				return err
			}

			displayNameCol := "name"
			if backendCfg.Recursive {
				displayNameCol = "path"
			}
			allColumns := []string{
				"permissions", "nlink", "user", "group", "size", "blocks",
				"mtime", "atime", "ctime", "inode", "type", displayNameCol,
			}
			if backendCfg.BeeGFS {
				allColumns = append(allColumns,
					"owner_id", "parent_entry_id", "entry_id",
					"stripe_pattern", "stripe_chunk", "stripe_targets",
				)
			}

			isLong := backendCfg.Long || backendCfg.FullTime
			var defaultColumns []string
			if isLong {
				defaultColumns = []string{"permissions", "nlink", "user"}
				if !backendCfg.NoGroup {
					defaultColumns = append(defaultColumns, "group")
				}
				defaultColumns = append(defaultColumns, "size")
				if backendCfg.ShowBlockSize {
					defaultColumns = append(defaultColumns, "blocks")
				}
				defaultColumns = append(defaultColumns, "mtime", displayNameCol)
			} else {
				if backendCfg.ShowBlockSize {
					defaultColumns = append(defaultColumns, "blocks")
				}
				defaultColumns = append(defaultColumns, displayNameCol)
			}
			if backendCfg.ShowInode {
				defaultColumns = append([]string{"inode"}, defaultColumns...)
			}
			if backendCfg.BeeGFS {
				defaultColumns = append(defaultColumns,
					"owner_id", "parent_entry_id", "entry_id",
					"stripe_pattern", "stripe_chunk", "stripe_targets",
				)
			}
			if viper.GetBool(config.DebugKey) {
				defaultColumns = allColumns
			}

			multiPath := len(paths) > 1
			printed := false

			for _, rp := range paths {
				if err := checkIndexExists(cfg, rp.indexPath); err != nil {
					return err
				}

				log.Debug("running beegfs index ls",
					zap.String("indexPath", rp.indexPath),
					zap.Any("cfg", backendCfg),
				)

				rows, errWait, err := indexPkg.Ls(cmd.Context(), exec, backendCfg, rp.walkRoot, rp.glob)
				if err != nil {
					return err
				}

				if multiPath {
					if printed {
						fmt.Println()
					}
					fmt.Printf("%s:\n", rp.fsPath)
					printed = true
				}

				tbl := cmdfmt.NewPrintomatic(allColumns, defaultColumns)

				err = drain(cmd.Context(), rows, errWait, func(row []string) {
					tbl.AddItem(toAny(formatLsRow(row, backendCfg.Recursive, backendCfg.BeeGFS, human, backendCfg.BlockSize, backendCfg.FullTime, rp.indexPath, rp.fsPath, backendCfg.AbsolutePaths))...)
				})
				// Flush buffered rows before returning, so partial output is shown
				// even when the query ends in an error.
				tbl.PrintRemaining()
				if err != nil {
					return err
				}
			}
			return nil
		},
	}

	cmd.Flags().AddFlagSet(parentFlags)
	cmd.Flags().Bool("help", false, "help for ls")

	cmd.Flags().StringVar(&backendCfg.Name, "name", "", "Only list entries whose name matches shell pattern.")
	cmd.Flags().StringVar(&backendCfg.Type, "type", "", "Only list entries of type: f, d, l.")
	cmd.Flags().BoolVarP(&backendCfg.All, "all", "a", false, "Show all files including hidden (names starting with '.').")
	cmd.Flags().BoolVarP(&backendCfg.AlmostAll, "almost-all", "A", false, "Like -a (. and .. are not indexed).")
	cmd.Flags().BoolVarP(&backendCfg.IgnoreBackups, "ignore-backups", "B", false, "Ignore entries whose names end with ~.")

	cmd.Flags().BoolVarP(&backendCfg.Long, "long", "l", false, "Long listing format: permissions, links, user, group, size, time, name.")
	cmd.Flags().BoolVarP(&backendCfg.ShowInode, "inode", "i", false, "Print the index node number of each file.")
	cmd.Flags().BoolVarP(&human, "human-readable", "h", false, "With -l, print sizes like 1.5K, 2.3M, 4.0G.")
	cmd.Flags().BoolVar(&backendCfg.BeeGFS, "beegfs", false, "Include BeeGFS metadata columns.")

	cmd.Flags().BoolVarP(&backendCfg.Recursive, "recursive", "R", false, "List subdirectories recursively.")
	cmd.Flags().BoolVar(&backendCfg.AbsolutePaths, "absolute-paths", false, "With -R, emit absolute BeeGFS paths instead of paths relative to the search root.")

	cmd.Flags().BoolVarP(&backendCfg.Reverse, "reverse", "r", false, "Reverse the sort order.")
	cmd.Flags().BoolVarP(&backendCfg.SortBySize, "sort-by-size", "S", false, "Sort by file size, largest first.")
	cmd.Flags().BoolVarP(&backendCfg.SortByMtime, "sort-by-mtime", "t", false, "Sort by modification time, newest first.")

	cmd.Flags().IntVarP(&backendCfg.NumResults, "num-results", "n", 0, "Return at most N entries (0 = unlimited).")
	cmd.Flags().BoolVarP(&backendCfg.NoGroup, "no-group", "G", false, "Suppress the group column in long listing.")
	cmd.Flags().BoolVarP(&backendCfg.ShowBlockSize, "size", "s", false, "Print the allocated size of each file in blocks.")
	cmd.Flags().StringVar(&backendCfg.BlockSize, "block-size", "", "Scale block sizes when using -s: K, M, or G.")
	cmd.Flags().StringVar(&backendCfg.SkipFile, "skip", "", "Skip directories listed in FILE.")
	cmd.Flags().BoolVar(&backendCfg.FullTime, "full-time", false, "Show full ISO-8601 timestamps (implies -l).")

	return cmd
}

func formatLsRow(raw []string, recursive, beegfs, human bool, blockSize string, fullTime bool, indexPath, fsPath string, absolute bool) []string {
	r := indexPkg.ParseLsRow(raw, recursive, beegfs)

	displaySize := r.Size
	switch {
	case human:
		displaySize = fmtSizeHuman(r.Size)
	case blockSize != "":
		// GNU ls --block-size scales the -l size column too, not just -s blocks.
		displaySize = fmtSizeWithBlockSize(r.Size, blockSize)
	}

	displayBlocks := r.Blocks
	if blockSize != "" {
		displayBlocks = fmtBlockSize(r.Blocks, blockSize)
	}

	timeFmt := fmtTimestamp
	if fullTime {
		timeFmt = fmtStatTime
	}

	displayEntry := r.Name
	if recursive {
		displayEntry = displayPath(r.Path, indexPath, fsPath, absolute)
	}

	result := []string{
		modeString(r.Mode, r.Type),
		r.Nlink,
		lookupUID(r.Uid),
		lookupGID(r.Gid),
		displaySize,
		displayBlocks,
		timeFmt(r.Mtime),
		timeFmt(r.Atime),
		timeFmt(r.Ctime),
		r.Inode,
		r.Type,
		displayEntry,
	}

	if beegfs {
		result = append(result,
			r.OwnerID, r.ParentEntryID, r.EntryID,
			r.StripePatternType, r.StripeChunkSize, r.StripeNumTargets,
		)
	}

	return result
}
