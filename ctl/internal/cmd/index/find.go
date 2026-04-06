package index

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func newFindCmd() *cobra.Command {
	backendCfg := indexPkg.FindCfg{Uid: -1, Gid: -1, MaxDepth: -1}

	var long bool
	var human bool

	// Newer-than reference files; resolved to timestamps in RunE.
	var newerFile, anewFile, cnewFile string

	cmd := &cobra.Command{
		Use:         "find [path]",
		Short:       "Searches for files in the index.",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Long: `Search the GUFI index for entries matching the given criteria, similar to find(1).

By default only paths are printed, one per line.
Use -l to show a long listing with metadata.

Time predicates follow GNU find conventions:
  -N  less than N units ago   (mtime > threshold)
  +N  more than N units ago   (mtime < threshold)
   N  exactly N units ago

Example: find all .c files

  beegfs index find --name "*.c" /data

Example: find files larger than 1 GB

  beegfs index find --size +1G /data

Example: find files modified in the last 7 days

  beegfs index find --mtime -7 /data

Example: long listing of the 10 largest files

  beegfs index find -l --largest --num-results 10 /data

Example: find BeeGFS files by metadata owner

  beegfs index find --ownerID 3 --beegfs /data
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()

			// Resolve newer-than reference files to timestamps.
			if newerFile != "" {
				st, err := os.Lstat(newerFile)
				if err != nil {
					return fmt.Errorf("--newer: %w", err)
				}
				backendCfg.NewerThan = st.ModTime().Unix()
			}
			if anewFile != "" {
				st, err := os.Lstat(anewFile)
				if err != nil {
					return fmt.Errorf("--anewer: %w", err)
				}
				backendCfg.AnewThan = st.ModTime().Unix()
			}
			if cnewFile != "" {
				st, err := os.Lstat(cnewFile)
				if err != nil {
					return fmt.Errorf("--cnewer: %w", err)
				}
				backendCfg.CnewThan = st.ModTime().Unix()
			}

			exec, err := newExecutor()
			if err != nil {
				return err
			}
			indexPath, err := resolveIndexPath(args)
			if err != nil {
				return err
			}
			if err := checkIndexExists(indexPath); err != nil {
				return err
			}

			log.Debug("running beegfs index find",
				zap.String("indexPath", indexPath),
				zap.Any("cfg", backendCfg),
			)

			rows, errWait, err := indexPkg.Find(cmd.Context(), exec, backendCfg, indexPath)
			if err != nil {
				return err
			}

			useTargets := backendCfg.TargetID != ""

			// allColumns in display order.
			allColumns := []string{
				"path",
				"permissions", "nlink", "user", "group", "size",
				"mtime", "atime", "ctime", "inode", "type", "name",
			}
			if backendCfg.BeeGFS {
				allColumns = append(allColumns, "owner_id", "parent_entry_id", "entry_id", "stripe_targets")
			} else if useTargets {
				allColumns = append(allColumns, "target_id")
			}

			// Default: only path (like real find). With -l: long listing.
			var defaultColumns []string
			if long {
				defaultColumns = []string{"path", "permissions", "nlink", "user", "group", "size", "mtime", "type"}
				if backendCfg.BeeGFS {
					defaultColumns = append(defaultColumns, "entry_id", "owner_id")
				} else if useTargets {
					defaultColumns = append(defaultColumns, "target_id")
				}
			} else {
				defaultColumns = []string{"path"}
				if backendCfg.BeeGFS {
					defaultColumns = append(defaultColumns, "entry_id")
				} else if useTargets {
					defaultColumns = append(defaultColumns, "target_id")
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
					tbl.AddItem(toAny(formatFindRow(row, backendCfg.BeeGFS, useTargets, human))...)
				}
			}

			return errWait()
		},
	}

	// cobra reserves -h for --help by default. Pre-register --help without a
	// shorthand so cobra skips claiming -h, leaving it free for --human-readable
	// (matching real find(1) behavior). --help still works via the long flag.
	cmd.Flags().Bool("help", false, "help for find")

	// Core predicates
	cmd.Flags().StringVar(&backendCfg.Name, "name", "", "Base of file name matches shell pattern (GLOB, case-sensitive).")
	cmd.Flags().StringVar(&backendCfg.Iname, "iname", "", "Like --name but case-insensitive.")
	cmd.Flags().StringVar(&backendCfg.Size, "size", "", "File size: +1G (>1G), -100k (<100k), 5M (=5M). Units: c b k M G T.")
	cmd.Flags().StringVar(&backendCfg.Type, "type", "", "Entry type: f (file), d (directory), l (symlink).")
	cmd.Flags().IntVar(&backendCfg.Uid, "uid", -1, "Numeric user ID (exact match; -1 = any).")
	cmd.Flags().IntVar(&backendCfg.Gid, "gid", -1, "Numeric group ID (exact match; -1 = any).")

	// Time predicates — day-based
	cmd.Flags().StringVar(&backendCfg.Mtime, "mtime", "", "mtime N*24 hours ago: -7 (<7 days), +7 (>7 days), 7 (=7 days).")
	cmd.Flags().StringVar(&backendCfg.Atime, "atime", "", "atime N*24 hours ago (same format).")
	cmd.Flags().StringVar(&backendCfg.Ctime, "ctime", "", "ctime N*24 hours ago (same format).")

	// Time predicates — minute-based
	cmd.Flags().StringVar(&backendCfg.Mmin, "mmin", "", "mtime N minutes ago: -10 (<10 min), +10 (>10 min).")
	cmd.Flags().StringVar(&backendCfg.Amin, "amin", "", "atime N minutes ago (same format).")
	cmd.Flags().StringVar(&backendCfg.Cmin, "cmin", "", "ctime N minutes ago (same format).")

	// Newer-than (reference file)
	cmd.Flags().StringVar(&newerFile, "newer", "", "Entry mtime is more recent than FILE's mtime.")
	cmd.Flags().StringVar(&anewFile, "anewer", "", "Entry atime is more recent than FILE's mtime.")
	cmd.Flags().StringVar(&cnewFile, "cnewer", "", "Entry ctime is more recent than FILE's mtime.")

	// Path predicates
	cmd.Flags().StringVar(&backendCfg.Path, "path", "", "Full path matches shell pattern (GLOB).")
	cmd.Flags().StringVar(&backendCfg.Regex, "regex", "", "Full path matches regular expression.")
	cmd.Flags().StringVar(&backendCfg.Iregex, "iregex", "", "Like --regex but case-insensitive.")
	cmd.Flags().StringVar(&backendCfg.Lname, "lname", "", "Symlink target matches shell pattern (implies type=l).")

	// Identity predicates
	cmd.Flags().StringVar(&backendCfg.Inum, "inum", "", "Inode number: +5 (>5), -5 (<5), 5 (=5).")
	cmd.Flags().StringVar(&backendCfg.Links, "links", "", "Hard link count: +2 (>2), -2 (<2), 2 (=2).")

	// Permission predicates
	cmd.Flags().BoolVar(&backendCfg.Executable, "executable", false, "Entry has execute bit set.")
	cmd.Flags().BoolVar(&backendCfg.Readable, "readable", false, "Entry has read bit set.")
	cmd.Flags().BoolVar(&backendCfg.Writable, "writable", false, "Entry has write bit set.")

	// State predicates
	cmd.Flags().BoolVar(&backendCfg.Empty, "empty", false, "File is empty (type=f AND size=0).")

	// BeeGFS-specific predicates
	cmd.Flags().StringVar(&backendCfg.EntryID, "entryID", "", "BeeGFS entry ID matches pattern.")
	cmd.Flags().StringVar(&backendCfg.OwnerID, "ownerID", "", "BeeGFS metadata owner (server) ID.")
	cmd.Flags().StringVar(&backendCfg.TargetID, "targetID", "", "BeeGFS storage target or buddy group ID.")
	cmd.Flags().BoolVar(&backendCfg.BeeGFS, "beegfs", false, "Include BeeGFS metadata columns in output.")

	// Depth control
	cmd.Flags().IntVar(&backendCfg.MaxDepth, "maxdepth", -1, "Descend at most N directory levels (-1 = unlimited).")
	cmd.Flags().IntVar(&backendCfg.MinDepth, "mindepth", 0, "Do not match entries at levels less than N.")

	// Output control
	cmd.Flags().BoolVarP(&long, "long", "l", false, "Long listing: show permissions, size, time, type.")
	cmd.Flags().BoolVarP(&human, "human-readable", "h", false, "With -l, print sizes like 1.5K 2.3M.")
	cmd.Flags().IntVar(&backendCfg.NumResults, "num-results", 0, "Return at most N results per directory (0 = unlimited).")
	cmd.Flags().BoolVar(&backendCfg.Smallest, "smallest", false, "Sort by size ascending (smallest first) per directory.")
	cmd.Flags().BoolVar(&backendCfg.Largest, "largest", false, "Sort by size descending (largest first) per directory.")

	return cmd
}

// formatFindRow maps a raw GUFI find result row to formatted values in allColumns order.
//
// GUFI raw column positions (from FindCoreE / FindBeeGFSE / FindTargetsE):
//
//	path(0) name(1) type(2) inode(3) size(4) mtime(5) atime(6) ctime(7) mode(8) uid(9) gid(10) nlink(11)
//	+BeeGFS:  owner_id(12) parent_entry_id(13) entry_id(14) stripe_num_targets(15)
//	+Targets: target_or_group(12)
//
// Output positions match allColumns:
//
//	[path, permissions, nlink, user, group, size, mtime, atime, ctime, inode, type, name, ...]
func formatFindRow(raw []string, beegfs, targets, human bool) []string {
	path := safeGet(raw, 0)
	name := safeGet(raw, 1)
	typeChar := safeGet(raw, 2)
	inode := safeGet(raw, 3)
	size := safeGet(raw, 4)
	mtime := safeGet(raw, 5)
	atime := safeGet(raw, 6)
	ctime := safeGet(raw, 7)
	modeStr := safeGet(raw, 8)
	uid := safeGet(raw, 9)
	gid := safeGet(raw, 10)
	nlink := safeGet(raw, 11)

	displaySize := size
	if human {
		displaySize = fmtSizeHuman(size)
	}

	result := []string{
		path,
		modeString(modeStr, typeChar),
		nlink,
		lookupUID(uid),
		lookupGID(gid),
		displaySize,
		fmtTimestamp(mtime),
		fmtTimestamp(atime),
		fmtTimestamp(ctime),
		inode,
		typeChar,
		name,
	}

	if beegfs {
		result = append(result,
			safeGet(raw, 12), // owner_id
			safeGet(raw, 13), // parent_entry_id
			safeGet(raw, 14), // entry_id
			safeGet(raw, 15), // stripe_num_targets
		)
	} else if targets {
		result = append(result, safeGet(raw, 12)) // target_or_group
	}

	return result
}

// toAny converts []string to []any for cmdfmt.Printomatic.AddItem.
func toAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}
