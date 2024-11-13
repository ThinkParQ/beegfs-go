package quota

import (
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/dsnet/golib/unitconv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/pool"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	poolBackend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/pool"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/quota"
	pb "github.com/thinkparq/protobuf/go/beegfs"
	pm "github.com/thinkparq/protobuf/go/management"
)

const (
	// quotaPrecision defines the number of decimal places to include in the output, controlling the
	// precision of quota-related values.
	quotaPrecision = 2
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "quota",
		Short: "Manage user, group, and default quotas for each storage pool",
		Long:  "Manage user, group, and default quotas for each storage pool",
	}

	cmd.AddCommand(newListCmd())
	cmd.AddCommand(newSetDefaultCmd())
	cmd.AddCommand(newSetLimitsCmd())
	cmd.AddCommand(newListLimitsCmd())
	cmd.AddCommand(newListUsageCmd())

	return cmd
}

func newListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-defaults",
		Short: "List the default quota limits on pools",
		Long:  "List the default quota limits on each pool. This is equivalent to `pool list --with-limits`.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return pool.RunListCmd(cmd, poolBackend.GetStoragePools_Config{WithLimits: true})
		},
	}

	return cmd
}

func newSetDefaultCmd() *cobra.Command {
	var userSpaceStr string
	var userInodeStr string
	var groupSpaceStr string
	var groupInodeStr string

	cmd := &cobra.Command{
		Use:   "set-defaults",
		Short: "Sets the default quota limits for a pool",
		Long:  "Sets the default quota limits for a pool. These are the limits that apply to all users and/or groups if they are not subject to more specific user/group quotas.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			pool, err := beegfs.NewEntityIdParser(16, beegfs.Storage).Parse(args[0])
			if err != nil {
				return err
			}

			userSpaceLimit, err := parseLimit(userSpaceStr)
			if err != nil {
				return err
			}
			userInodeLimit, err := parseLimit(userInodeStr)
			if err != nil {
				return err
			}
			groupSpaceLimit, err := parseLimit(groupSpaceStr)
			if err != nil {
				return err
			}
			groupInodeLimit, err := parseLimit(groupInodeStr)
			if err != nil {
				return err
			}

			return quota.SetDefault(cmd.Context(), &pm.SetDefaultQuotaLimitsRequest{
				Pool:            pool.ToProto(),
				UserSpaceLimit:  userSpaceLimit,
				UserInodeLimit:  userInodeLimit,
				GroupSpaceLimit: groupSpaceLimit,
				GroupInodeLimit: groupInodeLimit,
			})
		},
	}

	cmd.Flags().StringVar(&userSpaceStr, "user-space", "", "User space limit")
	cmd.Flags().StringVar(&userInodeStr, "user-inode", "", "User inode limit")
	cmd.Flags().StringVar(&groupSpaceStr, "group-space", "", "Group space limit")
	cmd.Flags().StringVar(&groupInodeStr, "group-inode", "", "Group inode limit")

	return cmd
}

type setLimitsCmdConfig struct {
	spaceStr string
	inodeStr string
	uidStrs  []string
	gidStrs  []string
}

func newSetLimitsCmd() *cobra.Command {
	cfg := setLimitsCmdConfig{}

	cmd := &cobra.Command{
		Use:   "set-limits",
		Short: "Set explicit quota limits for users and groups",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSetLimitsCmd(cmd, args, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.spaceStr, "space", "", "Space limit")
	cmd.Flags().StringVar(&cfg.inodeStr, "inode", "", "Inode limit")
	cmd.Flags().StringSliceVar(&cfg.uidStrs, "uid", []string{}, "Comma separated list of user ids to apply the limits to. Values can be single ids or ranges in the form `<min>-<max>`.")
	cmd.Flags().StringSliceVar(&cfg.gidStrs, "gid", []string{}, "Comma separated list of group ids to apply the limits to. Values can be single ids or ranges in the form `<min>-<max>`.")

	cmd.MarkFlagsOneRequired("space", "inode")
	cmd.MarkFlagsOneRequired("uid", "gid")

	return cmd
}

func runSetLimitsCmd(cmd *cobra.Command, args []string, cfg setLimitsCmdConfig) error {
	spaceLimit, err := parseLimit(cfg.spaceStr)
	if err != nil {
		return err
	}

	inodeLimit, err := parseLimit(cfg.inodeStr)
	if err != nil {
		return err
	}

	poolId, err := beegfs.NewEntityIdParser(16, beegfs.Storage).Parse(args[0])
	if err != nil {
		return err
	}
	protoPoolId := poolId.ToProto()

	limits := []*pm.QuotaInfo{}
	addEntry := func(poolId *pb.EntityIdSet, idType pb.QuotaIdType, id *uint32, spaceLimit *int64, inodeLimit *int64) {
		if spaceLimit != nil || inodeLimit != nil {
			limits = append(limits, &pm.QuotaInfo{
				Pool:       poolId,
				IdType:     idType,
				QuotaId:    id,
				SpaceLimit: spaceLimit,
				InodeLimit: inodeLimit,
			})
		}
	}

	for _, uidStr := range cfg.uidStrs {
		min64, max64, err := util.ParseUint64RangeFromStr(uidStr, 0, uint64(^uint32(0)))
		if err != nil {
			return err
		}
		min := uint32(min64)
		max := uint32(max64)

		for c := min; c <= max; c += 1 {
			addEntry(protoPoolId, pb.QuotaIdType_QUOTA_ID_TYPE_USER, &c, spaceLimit, inodeLimit)
		}
	}

	for _, gidStr := range cfg.gidStrs {
		min64, max64, err := util.ParseUint64RangeFromStr(gidStr, 0, uint64(^uint32(0)))
		if err != nil {
			return err
		}
		min := uint32(min64)
		max := uint32(max64)

		for c := min; c <= max; c += 1 {
			addEntry(protoPoolId, pb.QuotaIdType_QUOTA_ID_TYPE_GROUP, &c, spaceLimit, inodeLimit)
		}
	}

	return quota.SetLimits(cmd.Context(), &pm.SetQuotaLimitsRequest{
		Limits: limits,
	})

}

type listLimitsConfig struct {
	ids  string
	typ  string
	pool beegfs.EntityId
}

func newListLimitsCmd() *cobra.Command {
	cfg := listLimitsConfig{pool: beegfs.InvalidEntityId{}}

	cmd := &cobra.Command{
		Use:   "list-limits",
		Short: "List the explicitly set quota limits for users and groups",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runListLimitsCmd(cmd, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.ids, "ids", "", "Quota ids to query. Can be either a single id or a range in the form `<min>-<max>`.")
	cmd.Flags().StringVar(&cfg.typ, "type", "", "Quota type to query ('user' or 'group')")
	cmd.Flags().Var(beegfs.NewEntityIdPFlag(&cfg.pool, 16, beegfs.Storage), "pool", "Storage pool to query")

	return cmd
}

func runListLimitsCmd(cmd *cobra.Command, cfg listLimitsConfig) error {
	req := pm.GetQuotaLimitsRequest{}

	if cfg.ids != "" {
		min, max, err := util.ParseUint64RangeFromStr(cfg.ids, 0, uint64(^uint32(0)))
		if err != nil {
			return err
		}

		req.QuotaIdMin = new(uint32)
		*req.QuotaIdMin = uint32(min)
		req.QuotaIdMax = new(uint32)
		*req.QuotaIdMax = uint32(max)
	}

	if cfg.typ != "" {
		switch cfg.typ {
		case "user":
			req.IdType = pb.QuotaIdType_QUOTA_ID_TYPE_USER
		case "group":
			req.IdType = pb.QuotaIdType_QUOTA_ID_TYPE_GROUP
		default:
			return fmt.Errorf("invalid id type: must be 'user' or 'group'")
		}
	}

	req.Pool = cfg.pool.ToProto()

	stream, err := quota.GetLimits(cmd.Context(), &req)
	if err != nil {
		return err
	}

	tbl := cmdfmt.NewTableWrapper(
		[]string{"id", "type", "pool", "space", "inode"},
		[]string{"id", "type", "pool", "space", "inode"},
	)

	for {
		resp, err := stream.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			break
		}

		limits := resp.GetLimits()

		idTypeStr := ""
		switch limits.IdType {
		case pb.QuotaIdType_QUOTA_ID_TYPE_USER:
			idTypeStr = "user"
		case pb.QuotaIdType_QUOTA_ID_TYPE_GROUP:
			idTypeStr = "group"
		}

		space := "-"
		if limits.SpaceLimit != nil {
			if viper.GetBool(config.RawKey) {
				space = fmt.Sprintf("%d", limits.GetSpaceLimit())
			} else {
				space = util.I64FormatPrefixWithUnlimited(limits.GetSpaceLimit(), unitconv.IEC, quotaPrecision, true)
			}
		}

		inode := "-"
		if limits.InodeLimit != nil {
			if viper.GetBool(config.RawKey) {
				inode = fmt.Sprintf("%d", limits.GetInodeLimit())
			} else {
				inode = util.I64FormatPrefixWithUnlimited(limits.GetInodeLimit(), unitconv.SI, quotaPrecision, false)
			}
		}

		pool, err := beegfs.EntityIdSetFromProto(limits.Pool)
		if err != nil {
			return err
		}

		if viper.GetBool(config.DebugKey) {
			tbl.Row(*limits.QuotaId, idTypeStr, pool.String(), space, inode)
		} else {
			tbl.Row(*limits.QuotaId, idTypeStr, pool.Alias.String(), space, inode)
		}

	}

	tbl.PrintRemaining()
	fmt.Println()

	return nil
}

const (
	listUsageExceededKey = "exceeded"
)

type listUsageConfig struct {
	ids      string
	typ      string
	pool     beegfs.EntityId
	exceeded bool
}

func newListUsageCmd() *cobra.Command {
	cfg := listUsageConfig{pool: beegfs.InvalidEntityId{}}

	cmd := &cobra.Command{
		Use:   "list-usage",
		Short: "List quota usage per user or group together with their effective limit",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runListUsageCmd(cmd, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.ids, "ids", "", "Quota ids to query. Can be either a single id or a range in the form `<min>-<max>`.")
	cmd.Flags().StringVar(&cfg.typ, "type", "", "Quota type to query ('user' or 'group')")
	var pool beegfs.EntityId = beegfs.InvalidEntityId{}
	cmd.Flags().Var(beegfs.NewEntityIdPFlag(&pool, 16, beegfs.Storage), "pool", "Storage pool to query")
	cmd.Flags().BoolVar(&cfg.exceeded, listUsageExceededKey, false, "List only entries that exceed their limit")

	return cmd
}

func runListUsageCmd(cmd *cobra.Command, cfg listUsageConfig) error {
	req := pm.GetQuotaUsageRequest{}

	if cfg.ids != "" {
		min, max, err := util.ParseUint64RangeFromStr(cfg.ids, 0, uint64(^uint32(0)))
		if err != nil {
			return err
		}

		req.QuotaIdMin = new(uint32)
		*req.QuotaIdMin = uint32(min)
		req.QuotaIdMax = new(uint32)
		*req.QuotaIdMax = uint32(max)
	}

	if cfg.typ != "" {
		switch cfg.typ {
		case "user":
			req.IdType = pb.QuotaIdType_QUOTA_ID_TYPE_USER
		case "group":
			req.IdType = pb.QuotaIdType_QUOTA_ID_TYPE_GROUP
		default:
			return fmt.Errorf("invalid id type: must be 'user' or 'group'")
		}
	}

	req.Pool = cfg.pool.ToProto()

	if cmd.Flags().Changed(listUsageExceededKey) {
		req.Exceeded = new(bool)
		*req.Exceeded = cfg.exceeded
	}

	stream, err := quota.GetUsage(cmd.Context(), &req)
	if err != nil {
		return err
	}

	refreshPeriod := ""

	tbl := cmdfmt.NewTableWrapper(
		[]string{"id", "type", "pool", "space", "inode"},
		[]string{"id", "type", "pool", "space", "inode"},
	)

	for {
		resp, err := stream.Recv()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			break
		}

		// The first entry comes with the refresh period field
		if refreshPeriod == "" {
			p := resp.GetRefreshPeriodS()
			if p == 0 {
				refreshPeriod = "?"
			} else {
				refreshPeriod = fmt.Sprintf("%ds", p)
			}
		}
		entry := resp.GetEntry()

		idTypeStr := ""
		switch entry.IdType {
		case pb.QuotaIdType_QUOTA_ID_TYPE_USER:
			idTypeStr = "user"
		case pb.QuotaIdType_QUOTA_ID_TYPE_GROUP:
			idTypeStr = "group"
		}

		space := "?/"
		if entry.SpaceUsed != nil {
			if entry.GetSpaceUsed() != -1 {
				if viper.GetBool(config.RawKey) {
					space = fmt.Sprintf("%d/", entry.GetSpaceUsed())
				} else {
					space = fmt.Sprintf("%s/", util.I64FormatPrefixWithUnlimited(entry.GetSpaceUsed(),
						unitconv.IEC, quotaPrecision, true))
				}
			}
		}
		if entry.SpaceLimit != nil {
			if entry.GetSpaceLimit() != -1 {
				if viper.GetBool(config.RawKey) {
					space += fmt.Sprintf("%d", entry.GetSpaceLimit())
				} else {
					space += util.I64FormatPrefixWithUnlimited(entry.GetSpaceLimit(), unitconv.IEC, quotaPrecision, true)
				}
			} else {
				space += util.UnlimitedText
			}
		} else {
			space += "?"
		}

		inode := "?/"
		if entry.InodeUsed != nil {
			if entry.GetInodeUsed() != -1 {
				if viper.GetBool(config.RawKey) {
					inode = fmt.Sprintf("%d/", entry.GetInodeUsed())
				} else {
					inode = fmt.Sprintf("%s/", util.I64FormatPrefixWithUnlimited(entry.GetInodeUsed(),
						unitconv.SI, quotaPrecision, false))
				}
			}
		}
		if entry.InodeLimit != nil {
			if entry.GetInodeLimit() != -1 {
				if viper.GetBool(config.RawKey) {
					inode += fmt.Sprintf("%d", entry.GetInodeLimit())
				} else {
					inode += util.I64FormatPrefixWithUnlimited(entry.GetInodeLimit(), unitconv.SI, quotaPrecision, false)
				}
			} else {
				inode += util.UnlimitedText
			}
		} else {
			inode += "?"
		}

		pool, err := beegfs.EntityIdSetFromProto(entry.Pool)
		if err != nil {
			return err
		}

		if viper.GetBool(config.DebugKey) {
			tbl.Row(*entry.QuotaId, idTypeStr, pool.String(), space, inode)
		} else {
			tbl.Row(*entry.QuotaId, idTypeStr, pool.Alias.String(), space, inode)
		}
	}

	tbl.PrintRemaining()
	fmt.Println()
	fmt.Printf("Quota usage information is fetched every %s from the server nodes, thus the displayed values might be slightly out of date.\n", refreshPeriod)

	return nil
}

func parseLimit(s string) (*int64, error) {
	var res = new(int64)
	if s == "unlimited" {
		*res = math.MaxInt64
	} else if s == "reset" {
		*res = -1
	} else if s == "" {
		res = nil
	} else {
		parsed, err := util.ParseIntFromStr(s)
		if err != nil {
			return nil, err
		}
		if parsed > math.MaxInt64 {
			return nil, fmt.Errorf("the provided limit (%d) is larger than the maximum allowed (%d)", parsed, math.MaxInt64)
		}

		*res = int64(parsed)
	}

	return res, nil
}
