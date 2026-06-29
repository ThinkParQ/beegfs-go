package index

import (
	"fmt"
	"os/user"
	"strconv"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"go.uber.org/zap"
)

func newFindCmd(globalCfg *indexPkg.GlobalCfg, parentFlags *pflag.FlagSet) *cobra.Command {
	backendCfg := indexPkg.FindCfg{}

	var long bool
	var human bool
	var absolutePaths bool

	var uidVal, gidVal, maxDepthVal, minDepthVal int
	var newerFile, anewFile, cnewFile string
	var sameFile, userName, groupName string

	cmd := &cobra.Command{
		Use:         "find [path]",
		Short:       "Searches for files in the index.",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Long: `Search the index for entries matching the given criteria, similar to find(1).

By default only paths are printed, one per line.
Use -l to show a long listing with metadata.

Time predicates follow GNU find conventions:
  -N  less than N units ago   (mtime > threshold)
  +N  more than N units ago   (mtime < threshold)
   N  exactly N units ago

Example: find all .c files

  beegfs index find --name "*.c" /mnt/beegfs

Example: find files larger than 1 GB

  beegfs index find --size +1G /mnt/beegfs

Example: find files modified in the last 7 days

  beegfs index find --mtime -7 /mnt/beegfs

Example: long listing of the 10 largest files

  beegfs index find -l --largest --num-results 10 /mnt/beegfs

Example: find BeeGFS files by metadata owner

  beegfs index find --ownerID 3 --beegfs /mnt/beegfs
`,
		RunE: func(cmd *cobra.Command, args []string) error {
			log, _ := config.GetLogger()

			switch backendCfg.Type {
			case "", "f", "d", "l":
			default:
				return fmt.Errorf("invalid --type %q: must be f (file), d (directory), or l (symlink)", backendCfg.Type)
			}

			if cmd.Flags().Changed("uid") {
				backendCfg.Uid = &uidVal
			}
			if cmd.Flags().Changed("gid") {
				backendCfg.Gid = &gidVal
			}
			if cmd.Flags().Changed("maxdepth") {
				if maxDepthVal < 0 {
					return fmt.Errorf("--maxdepth must be >= 0, got %d", maxDepthVal)
				}
				backendCfg.MaxDepth = &maxDepthVal
			}
			if cmd.Flags().Changed("mindepth") {
				if minDepthVal < 0 {
					return fmt.Errorf("--mindepth must be >= 0, got %d", minDepthVal)
				}
				backendCfg.MinDepth = &minDepthVal
			}

			if newerFile != "" {
				fi, err := statRefFileInMount(newerFile)
				if err != nil {
					return fmt.Errorf("--newer: %w", err)
				}
				backendCfg.NewerThan = indexPkg.Ptr(fi.ModTime().Unix())
			}
			if anewFile != "" {
				fi, err := statRefFileInMount(anewFile)
				if err != nil {
					return fmt.Errorf("--anewer: %w", err)
				}
				backendCfg.AnewThan = indexPkg.Ptr(fi.ModTime().Unix())
			}
			if cnewFile != "" {
				fi, err := statRefFileInMount(cnewFile)
				if err != nil {
					return fmt.Errorf("--cnewer: %w", err)
				}
				backendCfg.CnewThan = indexPkg.Ptr(fi.ModTime().Unix())
			}

			if sameFile != "" {
				fi, err := statRefFileInMount(sameFile)
				if err != nil {
					return fmt.Errorf("--samefile: %w", err)
				}
				st, ok := fi.Sys().(*syscall.Stat_t)
				if !ok {
					return fmt.Errorf("--samefile: cannot read inode of %q", sameFile)
				}
				backendCfg.InumStr = strconv.FormatUint(st.Ino, 10)
			}

			if userName != "" {
				u, err := user.Lookup(userName)
				if err != nil {
					return fmt.Errorf("--user: %w", err)
				}
				uid, err := strconv.Atoi(u.Uid)
				if err != nil {
					return fmt.Errorf("--user: parsing uid %q: %w", u.Uid, err)
				}
				backendCfg.Uid = &uid
			}

			if groupName != "" {
				g, err := user.LookupGroup(groupName)
				if err != nil {
					return fmt.Errorf("--group: %w", err)
				}
				gid, err := strconv.Atoi(g.Gid)
				if err != nil {
					return fmt.Errorf("--group: parsing gid %q: %w", g.Gid, err)
				}
				backendCfg.Gid = &gid
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

			useTargets := backendCfg.TargetID != ""

			allColumns := []string{
				"path",
				"permissions", "nlink", "user", "group", "size",
				"mtime", "atime", "ctime", "inode", "type", "name",
			}
			if backendCfg.BeeGFS {
				allColumns = append(allColumns, "owner_id", "parent_entry_id", "entry_id", "stripe_targets")
			}
			if useTargets {
				allColumns = append(allColumns, "target_id")
			}

			var defaultColumns []string
			if long {
				defaultColumns = []string{"path", "permissions", "nlink", "user", "group", "size", "mtime", "type"}
				if backendCfg.BeeGFS {
					defaultColumns = append(defaultColumns, "entry_id", "owner_id")
				}
			} else {
				defaultColumns = []string{"path"}
				if backendCfg.BeeGFS {
					defaultColumns = append(defaultColumns, "entry_id")
				}
			}
			if useTargets {
				defaultColumns = append(defaultColumns, "target_id")
			}
			if viper.GetBool(config.DebugKey) {
				defaultColumns = allColumns
			}

			tbl := cmdfmt.NewPrintomatic(allColumns, defaultColumns)
			defer tbl.PrintRemaining()

			for _, rp := range paths {
				if err := checkIndexExists(cfg, rp.indexPath); err != nil {
					return err
				}

				log.Debug("running beegfs index find",
					zap.String("indexPath", rp.indexPath),
					zap.Any("cfg", backendCfg),
				)

				rows, errWait, err := indexPkg.Find(cmd.Context(), exec, backendCfg, rp.walkRoot, rp.indexPath, rp.fsPath, rp.glob)
				if err != nil {
					return err
				}

				if err := drain(cmd.Context(), rows, errWait, func(row []string) {
					tbl.AddItem(toAny(formatFindRow(row, backendCfg.BeeGFS, useTargets, human, rp.indexPath, rp.fsPath, absolutePaths))...)
				}); err != nil {
					return err
				}
			}
			return nil
		},
	}

	cmd.Flags().AddFlagSet(parentFlags)
	cmd.Flags().Bool("help", false, "help for find")

	cmd.Flags().StringVar(&backendCfg.Name, "name", "", "Base of file name matches shell pattern (GLOB, case-sensitive).")
	cmd.Flags().StringVar(&backendCfg.Iname, "iname", "", "Base of file name matches a case-insensitive regular expression (unlike --name, which uses GLOB syntax).")
	cmd.Flags().StringArrayVar(&backendCfg.Size, "size", nil, "File size: +1G (>1G), -100k (<100k), 5M (=5M). Units: c b k M G T. Repeatable.")
	cmd.Flags().StringVar(&backendCfg.Type, "type", "", "Entry type: f (file), d (directory), l (symlink).")
	cmd.Flags().IntVar(&uidVal, "uid", 0, "Numeric user ID (exact match).")
	cmd.Flags().IntVar(&gidVal, "gid", 0, "Numeric group ID (exact match).")

	cmd.Flags().StringVar(&backendCfg.Mtime, "mtime", "", "mtime N*24 hours ago: -7 (<7 days), +7 (>7 days), 7 (=7 days).")
	cmd.Flags().StringVar(&backendCfg.Atime, "atime", "", "atime N*24 hours ago (same format).")
	cmd.Flags().StringVar(&backendCfg.Ctime, "ctime", "", "ctime N*24 hours ago (same format).")

	cmd.Flags().StringVar(&backendCfg.Mmin, "mmin", "", "mtime N minutes ago: -10 (<10 min), +10 (>10 min).")
	cmd.Flags().StringVar(&backendCfg.Amin, "amin", "", "atime N minutes ago (same format).")
	cmd.Flags().StringVar(&backendCfg.Cmin, "cmin", "", "ctime N minutes ago (same format).")

	cmd.Flags().StringVar(&newerFile, "newer", "", "Entry mtime is more recent than FILE's mtime.")
	cmd.Flags().StringVar(&anewFile, "anewer", "", "Entry atime is more recent than FILE's mtime.")
	cmd.Flags().StringVar(&cnewFile, "cnewer", "", "Entry ctime is more recent than FILE's mtime.")

	cmd.Flags().StringVar(&backendCfg.Path, "path", "", "Full path matches shell pattern (GLOB).")
	cmd.Flags().StringVar(&backendCfg.Regex, "regex", "", "Full path matches regular expression.")
	cmd.Flags().StringVar(&backendCfg.Iregex, "iregex", "", "Like --regex but case-insensitive.")
	cmd.Flags().StringVar(&backendCfg.Lname, "lname", "", "Symlink target matches shell pattern (implies type=l).")

	cmd.Flags().StringVar(&backendCfg.Inum, "inum", "", "Inode number: +5 (>5), -5 (<5), 5 (=5).")
	cmd.Flags().StringVar(&backendCfg.Links, "links", "", "Hard link count: +2 (>2), -2 (<2), 2 (=2).")

	cmd.Flags().BoolVar(&backendCfg.Executable, "executable", false, "Entry has execute bit set.")
	cmd.Flags().BoolVar(&backendCfg.Readable, "readable", false, "Entry has read bit set.")
	cmd.Flags().BoolVar(&backendCfg.Writable, "writable", false, "Entry has write bit set.")

	cmd.Flags().BoolVar(&backendCfg.Empty, "empty", false, "File is empty (type=f AND size=0).")

	cmd.Flags().StringVar(&backendCfg.EntryID, "entryID", "", "BeeGFS entry ID matches pattern.")
	cmd.Flags().StringVar(&backendCfg.OwnerID, "ownerID", "", "BeeGFS metadata owner (server) ID.")
	cmd.Flags().StringVar(&backendCfg.TargetID, "targetID", "", "BeeGFS storage target or buddy group ID.")
	cmd.Flags().BoolVar(&backendCfg.BeeGFS, "beegfs", false, "Include BeeGFS metadata columns in output.")

	cmd.Flags().IntVar(&maxDepthVal, "maxdepth", 0, "Descend at most N directory levels.")
	cmd.Flags().IntVar(&minDepthVal, "mindepth", 0, "Do not match entries at levels less than N (0 includes the search root).")

	cmd.Flags().BoolVarP(&long, "long", "l", false, "Long listing: show permissions, size, time, type.")
	cmd.Flags().BoolVarP(&human, "human-readable", "h", false, "With -l, print sizes like 1.5K 2.3M.")
	cmd.Flags().IntVar(&backendCfg.NumResults, "num-results", 0, "Return at most N results per directory (0 = unlimited).")
	cmd.Flags().BoolVar(&backendCfg.Smallest, "smallest", false, "Sort by size ascending (smallest first) per directory.")
	cmd.Flags().BoolVar(&backendCfg.Largest, "largest", false, "Sort by size descending (largest first) per directory.")

	cmd.Flags().StringVar(&sameFile, "samefile", "", "Find entries with the same inode number as FILE.")
	cmd.Flags().StringVar(&userName, "user", "", "Find entries owned by user NAME (resolved to UID).")
	cmd.Flags().StringVar(&groupName, "group", "", "Find entries owned by group NAME (resolved to GID).")

	cmd.Flags().BoolVar(&absolutePaths, "absolute-paths", false, "Emit absolute BeeGFS paths instead of paths relative to the search root.")

	cmd.Flags().BoolVar(&backendCfg.True, "true", false, "Always true — matches every entry.")
	cmd.Flags().BoolVar(&backendCfg.False, "false", false, "Always false — matches no entry.")

	cmd.MarkFlagsMutuallyExclusive("smallest", "largest")
	cmd.MarkFlagsMutuallyExclusive("empty", "smallest")
	cmd.MarkFlagsMutuallyExclusive("empty", "largest")
	cmd.MarkFlagsMutuallyExclusive("true", "false")
	cmd.MarkFlagsMutuallyExclusive("uid", "user")
	cmd.MarkFlagsMutuallyExclusive("gid", "group")
	cmd.MarkFlagsMutuallyExclusive("inum", "samefile")

	return cmd
}

func formatFindRow(raw []string, beegfs, targets, human bool, indexPath, fsPath string, absolute bool) []string {
	r := indexPkg.ParseFindRow(raw, beegfs, targets)

	displaySize := r.Size
	if human {
		displaySize = fmtSizeHuman(r.Size)
	}

	result := []string{
		displayPath(r.Path, indexPath, fsPath, absolute),
		modeString(r.Mode, r.Type),
		r.Nlink,
		r.Uid,
		r.Gid,
		displaySize,
		fmtTimestamp(r.Mtime),
		fmtTimestamp(r.Atime),
		fmtTimestamp(r.Ctime),
		r.Inode,
		r.Type,
		r.Name,
	}

	if beegfs {
		result = append(result, r.OwnerID, r.ParentEntryID, r.EntryID, r.StripeNumTargets)
	}
	if targets {
		result = append(result, r.TargetOrGroup)
	}

	return result
}

func toAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}
