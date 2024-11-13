package pool

import (
	"fmt"
	"strings"

	"github.com/dsnet/golib/unitconv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/pool"
)

func newListCmd() *cobra.Command {
	var withLimits bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List storage pools",
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunListCmd(cmd, pool.GetStoragePools_Config{WithLimits: withLimits})
		},
	}

	cmd.Flags().BoolVar(&withLimits, "with-limits", false, "Request and print pool default quota limits")

	return cmd
}

func RunListCmd(cmd *cobra.Command, cfg pool.GetStoragePools_Config) error {
	// Since this table contains cells with multiple rows, pageSize=0 would mess up the output.
	if viper.GetUint(config.PageSizeKey) == 0 {
		return fmt.Errorf("--%s=0 is not supported for this view", config.PageSizeKey)
	}

	pools, err := pool.GetStoragePools(cmd.Context(), cfg)
	if err != nil {
		return err
	}

	defaultColumns := []string{"alias", "id", "members"}
	if cfg.WithLimits {
		defaultColumns = append(defaultColumns, "user_space_limit", "user_inode_limit", "group_space_limit", "group_inode_limit")
	}

	tbl := cmdfmt.NewTableWrapper(
		[]string{"uid", "alias", "id", "members", "user_space_limit", "user_inode_limit", "group_space_limit", "group_inode_limit"},
		defaultColumns,
	)

	for _, p := range pools {
		userSpaceLimit := ""
		if p.UserSpaceLimit != nil {
			if *p.UserSpaceLimit == -1 {
				userSpaceLimit = util.UnlimitedText
			} else {
				if viper.GetBool(config.RawKey) {
					userSpaceLimit = fmt.Sprint(*p.UserSpaceLimit)
				} else {
					userSpaceLimit = util.I64FormatPrefixWithUnlimited(*p.UserSpaceLimit, unitconv.IEC, 0, true)
				}
			}
		}

		userInodeLimit := ""
		if p.UserInodeLimit != nil {
			if *p.UserInodeLimit == -1 {
				userInodeLimit = util.UnlimitedText
			} else {
				if viper.GetBool(config.RawKey) {
					userInodeLimit = fmt.Sprint(*p.UserInodeLimit)
				} else {
					userInodeLimit = util.I64FormatPrefixWithUnlimited(*p.UserInodeLimit, unitconv.SI, 0, false)
				}
			}
		}

		groupSpaceLimit := ""
		if p.GroupSpaceLimit != nil {
			if *p.GroupSpaceLimit == -1 {
				groupSpaceLimit = util.UnlimitedText
			} else {
				if viper.GetBool(config.RawKey) {
					groupSpaceLimit = fmt.Sprint(*p.GroupSpaceLimit)
				} else {
					groupSpaceLimit = util.I64FormatPrefixWithUnlimited(*p.GroupSpaceLimit, unitconv.IEC, 0, true)
				}
			}
		}

		groupInodeLimit := ""
		if p.GroupInodeLimit != nil {
			if *p.GroupInodeLimit == -1 {
				groupInodeLimit = util.UnlimitedText
			} else {
				if viper.GetBool(config.RawKey) {
					groupInodeLimit = fmt.Sprint(*p.GroupInodeLimit)
				} else {
					groupInodeLimit = util.I64FormatPrefixWithUnlimited(*p.GroupInodeLimit, unitconv.SI, 0, false)
				}
			}
		}

		members := ""
		for _, t := range p.Targets {
			if viper.GetBool(config.DebugKey) {
				members += t.String() + "\n"
			} else {
				members += t.Alias.String() + "\n"
			}
		}
		for _, t := range p.BuddyGroups {
			if viper.GetBool(config.DebugKey) {
				members += t.String() + "\n"
			} else {
				members += t.Alias.String() + "\n"
			}
		}

		members = strings.Trim(members, "\n")

		tbl.Row(p.Pool.Uid, p.Pool.Alias, p.Pool.LegacyId, members, userSpaceLimit, userInodeLimit, groupSpaceLimit, groupInodeLimit)
	}

	tbl.PrintRemaining()
	fmt.Println()
	return nil
}
