package quota

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"sync"
	"syscall"

	"github.com/dsnet/golib/unitconv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/build"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmd/pool"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	poolBackend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/pool"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/quota"
	pb "github.com/thinkparq/protobuf/go/beegfs"
	pm "github.com/thinkparq/protobuf/go/management"
	"go.uber.org/zap"
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
		Args:  cobra.NoArgs,
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
		Short: "Set the default quota limits for a pool",
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

	cmd.Flags().StringVar(&userSpaceStr, "user-space", "", "User space limit.")
	cmd.Flags().StringVar(&userInodeStr, "user-inode", "", "User inode limit.")
	cmd.Flags().StringVar(&groupSpaceStr, "group-space", "", "Group space limit.")
	cmd.Flags().StringVar(&groupInodeStr, "group-inode", "", "Group inode limit.")

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
		Use:   "set-limits <pool>",
		Short: "Set explicit quota limits for users and groups",
		Long: `Set explicit quota limits for users and groups.

Example: Set a 1TiB space limit and 1,000,000 inode limit for user 1001 in pool 1

  beegfs quota set-limits --uid 1001 --space 1TiB --inode 1000000 1

Example: Set a 10TiB space limit for groups 2000-2005 in pool 1

  beegfs quota set-limits --gid 2000-2005 --space 10TiB 1`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return runSetLimitsCmd(cmd, args, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.spaceStr, "space", "", "Space limit.")
	cmd.Flags().StringVar(&cfg.inodeStr, "inode", "", "Inode limit.")
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
	userIds  []string
	groupIds []string
	pool     beegfs.EntityId
	users    []string
	groups   []string
	nss      bool
}

func newListLimitsCmd() *cobra.Command {
	cfg := listLimitsConfig{pool: beegfs.InvalidEntityId{}}

	cmd := &cobra.Command{
		Use:         "list-limits",
		Short:       "List the explicitly set quota limits for users and groups",
		Long:        "List the explicitly set quota limits for users and groups. By default only entries for the current user and their groups are listed. Only the root user can query limits for other user and group IDs.",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runListLimitsCmd(cmd, cfg)
		},
	}

	cmd.Flags().StringSliceVar(&cfg.userIds, "uids", []string{}, "User ids to query. Can be either a single id, a range in the form `<min>-<max>`, a comma separated list of ids, 'current' or 'all'.")
	cmd.Flags().StringSliceVar(&cfg.groupIds, "gids", []string{}, "Group ids to query. Can be either a single id, a range in the form `<min>-<max>`, a comma separated list of ids, 'current' or 'all'.")
	cmd.Flags().Var(beegfs.NewEntityIdPFlag(&cfg.pool, 16, beegfs.Storage), "pool", "Storage pool to query")
	cmd.Flags().StringSliceVar(&cfg.users, "users", []string{}, "User names to query. Resolved to IDs and added to --uids.")
	cmd.Flags().StringSliceVar(&cfg.groups, "groups", []string{}, "Group names to query. Resolved to IDs and added to --gids.")
	addNSSFlag(cmd, &cfg.nss)

	return cmd
}

func runListLimitsCmd(cmd *cobra.Command, cfg listLimitsConfig) error {
	req := pm.GetQuotaLimitsRequest_builder{}.Build()

	if err := appendNames(&cfg.userIds, cfg.users, &cfg.groupIds, cfg.groups, cfg.nss); err != nil {
		return err
	}

	if len(cfg.userIds) == 0 && len(cfg.groupIds) == 0 {
		cfg.userIds = append(cfg.userIds, "current")
		cfg.groupIds = append(cfg.groupIds, "current")
	}

	err := parseUserIdsInto(cfg.userIds, req.SetUserIdMin, req.SetUserIdMax, req.SetUserIdList)
	if err != nil {
		return err
	}

	err = parseGroupIdsInto(cfg.groupIds, req.SetGroupIdMin, req.SetGroupIdMax, req.SetGroupIdList)
	if err != nil {
		return err
	}

	req.SetPool(cfg.pool.ToProto())

	stream, err := quota.GetLimits(cmd.Context(), req)
	if err != nil {
		return err
	}

	tbl := cmdfmt.NewPrintomatic(
		[]string{"name", "id", "type", "pool", "space", "inode"},
		[]string{"name", "id", "type", "pool", "space", "inode"},
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

		name, err := idToName(*limits.QuotaId, idTypeStr, cfg.nss)
		if err != nil {
			return err
		}

		if viper.GetBool(config.DebugKey) {
			tbl.AddItem(name, *limits.QuotaId, idTypeStr, pool.String(), space, inode)
		} else {
			tbl.AddItem(name, *limits.QuotaId, idTypeStr, pool.Alias.String(), space, inode)
		}
	}

	tbl.PrintRemaining()

	return nil
}

const (
	listUsageExceededKey = "exceeded"
)

type listUsageConfig struct {
	userIds  []string
	groupIds []string
	pool     beegfs.EntityId
	exceeded bool
	users    []string
	groups   []string
	nss      bool
}

func newListUsageCmd() *cobra.Command {
	cfg := listUsageConfig{pool: beegfs.InvalidEntityId{}}

	cmd := &cobra.Command{
		Use:         "list-usage",
		Short:       "List quota usage per user or group together with their effective limit",
		Long:        "List the quota usage info for users and groups. By default only entries for the current user and their groups are listed. Only the root user can query limits for other user and group IDs.",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runListUsageCmd(cmd, cfg)
		},
	}

	cmd.Flags().StringSliceVar(&cfg.userIds, "uids", []string{}, "User ids to query. Can be either a single id, a range in the form `<min>-<max>`, a comma separated list of ids, 'current' or 'all'.")
	cmd.Flags().StringSliceVar(&cfg.groupIds, "gids", []string{}, "Group ids to query. Can be either a single id, a range in the form `<min>-<max>`, a comma separated list of ids, 'current' or 'all'.")
	cmd.Flags().Var(beegfs.NewEntityIdPFlag(&cfg.pool, 16, beegfs.Storage), "pool", "Storage pool to query")
	cmd.Flags().BoolVar(&cfg.exceeded, listUsageExceededKey, false, "List only entries that exceed their limit.")
	cmd.Flags().StringSliceVar(&cfg.users, "users", []string{}, "User names to query. Resolved to IDs and added to --uids.")
	cmd.Flags().StringSliceVar(&cfg.groups, "groups", []string{}, "Group names to query. Resolved to IDs and added to --gids.")
	addNSSFlag(cmd, &cfg.nss)

	return cmd
}

func runListUsageCmd(cmd *cobra.Command, cfg listUsageConfig) error {
	req := pm.GetQuotaUsageRequest_builder{}.Build()

	if err := appendNames(&cfg.userIds, cfg.users, &cfg.groupIds, cfg.groups, cfg.nss); err != nil {
		return err
	}

	if len(cfg.userIds) == 0 && len(cfg.groupIds) == 0 {
		cfg.userIds = append(cfg.userIds, "current")
		cfg.groupIds = append(cfg.groupIds, "current")
	}

	err := parseUserIdsInto(cfg.userIds, req.SetUserIdMin, req.SetUserIdMax, req.SetUserIdList)
	if err != nil {
		return err
	}

	err = parseGroupIdsInto(cfg.groupIds, req.SetGroupIdMin, req.SetGroupIdMax, req.SetGroupIdList)
	if err != nil {
		return err
	}

	req.SetPool(cfg.pool.ToProto())

	if cmd.Flags().Changed(listUsageExceededKey) {
		req.SetExceeded(cfg.exceeded)
	}

	stream, err := quota.GetUsage(cmd.Context(), req)
	if err != nil {
		return err
	}

	// If no quotas were returned, this will never be set.
	refreshPeriod := "?"
	entriesFound := false

	tbl := cmdfmt.NewPrintomatic(
		[]string{"name", "id", "type", "pool", "space", "inode"},
		[]string{"name", "id", "type", "pool", "space", "inode"},
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
		if refreshPeriod == "?" {
			entriesFound = true
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

		name, err := idToName(*entry.QuotaId, idTypeStr, cfg.nss)
		if err != nil {
			return err
		}

		if viper.GetBool(config.DebugKey) {
			tbl.AddItem(name, *entry.QuotaId, idTypeStr, pool.String(), space, inode)
		} else {
			tbl.AddItem(name, *entry.QuotaId, idTypeStr, pool.Alias.String(), space, inode)
		}
	}

	tbl.PrintRemaining()
	if entriesFound {
		cmdfmt.Printf("Note: Quota usage information is fetched every %s from the nodes, thus the displayed values might be slightly out of date.\n", refreshPeriod)
	} else {
		cmdfmt.Printf("WARNING: No information found for the specified ID(s). Either these ID(s) do not exist, or the management is not configured to query/enforce their quotas.\n")
	}

	return nil
}

func parseLimit(s string) (*int64, error) {
	res := new(int64)
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

// parses a user id string slice, extracts the ids or fetches them from os and calls the appropriate
// provided set functions
func parseUserIdsInto(
	idStr []string,
	setMin func(uint32),
	setMax func(uint32),
	setList func([]uint32),
) error {
	if len(idStr) == 1 && idStr[0] == "current" {
		uid := uint32(os.Getuid())
		setMin(uid)
		setMax(uid)
	} else if len(idStr) > 0 {
		if os.Geteuid() != 0 {
			return fmt.Errorf("only root can query arbitrary user ids")
		}

		if len(idStr) > 1 {
			ids := []uint32{}
			for _, idStr := range idStr {
				id, err := strconv.ParseUint(idStr, 10, 32)
				if err != nil {
					return fmt.Errorf("invalid user id in list: %w", err)
				}
				ids = append(ids, uint32(id))
			}
			setList(ids)
		} else {
			if idStr[0] == "all" {
				setMin(0)
			} else {
				min, max, err := util.ParseUint64RangeFromStr(idStr[0], 0, uint64(^uint32(0)))
				if err != nil {
					return err
				}
				setMin(uint32(min))
				setMax(uint32(max))
			}
		}
	}

	return nil
}

// parses a group id string slice, extracts the ids or fetches them from os and calls the appropriate
// provided set functions
func parseGroupIdsInto(
	idStr []string,
	setMin func(uint32),
	setMax func(uint32),
	setList func([]uint32),
) error {
	if len(idStr) == 1 && idStr[0] == "current" {
		gids, err := getCurrentGroupIds()
		if err != nil {
			return err
		}
		setList(gids)
	} else if len(idStr) > 0 {
		if os.Geteuid() != 0 {
			return fmt.Errorf("only root can query arbitrary group ids")
		}

		if len(idStr) > 1 {
			ids := []uint32{}
			for _, idStr := range idStr {
				id, err := strconv.ParseUint(idStr, 10, 32)
				if err != nil {
					return fmt.Errorf("invalid group id in list: %w", err)
				}
				ids = append(ids, uint32(id))
			}
			setList(ids)
		} else {
			if idStr[0] == "all" {
				setMin(0)
			} else {
				min, max, err := util.ParseUint64RangeFromStr(idStr[0], 0, uint64(^uint32(0)))
				if err != nil {
					return err
				}
				setMin(uint32(min))
				setMax(uint32(max))
			}
		}
	}

	return nil
}

func getCurrentGroupIds() ([]uint32, error) {
	user, err := user.Current()
	if err != nil {
		return nil, err
	}

	gidStrs, err := user.GroupIds()
	if err != nil {
		return nil, err
	}

	gids := []uint32{}
	for _, gid := range gidStrs {
		gid, err := strconv.ParseUint(gid, 10, 32)
		if err != nil {
			return nil, err
		}
		gids = append(gids, uint32(gid))
	}

	return gids, nil
}

const nssFlag = "nss"

// addNSSFlag defines nssFlag for the commands that resolve IDs to names.
func addNSSFlag(cmd *cobra.Command, target *bool) {
	cmd.Flags().BoolVar(target, nssFlag, false, `Resolve UIDs and GIDs using the system's name service (NSS), which includes LDAP, SSSD and AD.
	By default only local /etc/passwd and /etc/group entries are resolved and other IDs are printed numerically.`)
	if build.CGO {
		// A CGO enabled build already resolves through NSS in os/user, so the flag does nothing.
		// It stays defined rather than omitted so command lines that pass it keep working against
		// both builds.
		cmd.Flags().MarkHidden(nssFlag)
	}
}

// beegfs-nss-resolver is built with CGO enabled, so it resolves IDs through NSS. The path is fixed
// and deliberately never looked up in $PATH: the beegfs binary is installed setgid, so letting a
// caller choose the program executed here would run their code with the beegfs group's privileges.
const nssResolverPath = "/opt/beegfs/lib/beegfs-nss-resolver"

const nssResolverProtocolVersion = 1

// idRequest and idResponse cover the parts of the beegfs-nss-resolver protocol the CLI uses. An ID
// or name missing from the response maps was not found, one present in an error map didn't resolve.
type idRequest struct {
	Seq    uint64   `json:"seq"`
	UIDs   []uint32 `json:"uids,omitempty"`
	GIDs   []uint32 `json:"gids,omitempty"`
	Users  []string `json:"users,omitempty"`
	Groups []string `json:"groups,omitempty"`
}

type idResponse struct {
	Seq        uint64            `json:"seq"`
	Users      map[uint32]string `json:"users"`
	Groups     map[uint32]string `json:"groups"`
	UIDErrors  map[uint32]string `json:"uid_errors"`
	GIDErrors  map[uint32]string `json:"gid_errors"`
	UserIDs    map[string]uint32 `json:"user_ids"`
	GroupIDs   map[string]uint32 `json:"group_ids"`
	NameErrors map[string]string `json:"name_errors"`
}

// nssResolver resolves IDs by way of beegfs-nss-resolver, which is built with CGO enabled and so
// resolves through NSS. It is started on the first lookup and reused for the rest of the process.
// Nothing shuts it down: the helper reads EOF and exits on its own once the CLI terminates and its
// stdin pipe closes.
type nssResolver struct {
	mu      sync.Mutex
	started bool
	// Sticky, so a helper that cannot be started is not re-execed once per row of output.
	err error
	enc *json.Encoder
	dec *json.Decoder
	seq uint64
}

var resolver nssResolver

func (r *nssResolver) start() error {
	proc := exec.Command(nssResolverPath)
	// Name resolution needs no BeeGFS privilege. The CLI is installed setgid beegfs so it can read
	// the group-beegfs auth secret, which would otherwise leave this child running with
	// egid=beegfs. Mirrors index.CallerSysProcAttr, which does the same for the GUFI subprocesses.
	proc.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid:         uint32(os.Getuid()),
			Gid:         uint32(os.Getgid()),
			NoSetGroups: true,
		},
	}
	stdin, err := proc.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := proc.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := proc.StderrPipe()
	if err != nil {
		return err
	}
	if err := proc.Start(); err != nil {
		return fmt.Errorf("unable to start %s: %w", nssResolverPath, err)
	}

	// Route the helper's diagnostics through the logger so they don't interleave with table output.
	go func() {
		log, _ := config.GetLogger()
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			log.Warn("NSS resolver", zap.String("path", nssResolverPath),
				zap.String("message", scanner.Text()))
		}
	}()
	r.enc = json.NewEncoder(stdin)
	r.dec = json.NewDecoder(stdout)

	var greeting struct {
		Hello   string `json:"hello"`
		Version int    `json:"version"`
	}
	if err := r.dec.Decode(&greeting); err != nil || greeting.Hello != "beegfs-nss-resolver" {
		stdin.Close()
		return fmt.Errorf(
			"unexpected greeting from NSS resolver (%s), increase log level for more detail",
			nssResolverPath)
	}
	if greeting.Version != nssResolverProtocolVersion {
		stdin.Close()
		return fmt.Errorf("%s speaks protocol version %d, expected %d", nssResolverPath,
			greeting.Version, nssResolverProtocolVersion)
	}
	return nil
}

// resolve sends one request to the helper and returns its response, starting the helper on first
// use. Callers build the request and read whichever maps they asked to be filled.
func (r *nssResolver) resolve(req idRequest) (idResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		r.started = true
		r.err = r.start()
	}
	if r.err != nil {
		return idResponse{}, r.err
	}

	r.seq++
	req.Seq = r.seq
	if err := r.enc.Encode(&req); err != nil {
		return idResponse{}, fmt.Errorf("unable to send request to %s: %w", nssResolverPath, err)
	}

	var resp idResponse
	if err := r.dec.Decode(&resp); err != nil {
		return idResponse{}, fmt.Errorf("unable to read response from %s: %w", nssResolverPath, err)
	}
	if resp.Seq != r.seq {
		return idResponse{}, fmt.Errorf("%s responded out of sequence (got %d, want %d)",
			nssResolverPath, resp.Seq, r.seq)
	}
	return resp, nil
}

// converts a user or group ID to its corresponding username or groupname
// Fetched from the operating system's user and group database, or from beegfs-nss-resolver when
// nssFlag is given. If not found returns the ID as string.
func idToName(id uint32, idType string, nss bool) (string, error) {
	if idType != "user" && idType != "group" {
		return "", fmt.Errorf("invalid idType: %s", idType)
	}

	// A CGO enabled build already resolves through NSS in os/user below, so the helper would only
	// add a process without changing the result.
	if nss && !build.CGO {
		req := idRequest{}
		if idType == "user" {
			req.UIDs = []uint32{id}
		} else {
			req.GIDs = []uint32{id}
		}
		resp, err := resolver.resolve(req)
		if err != nil {
			return "", err
		}
		names, failed := resp.Users, resp.UIDErrors
		if idType == "group" {
			names, failed = resp.Groups, resp.GIDErrors
		}
		if name, ok := names[id]; ok {
			return name, nil
		}
		// Absent means the ID definitively does not exist, which falls through to printing it
		// numerically below. An entry in the error map means the lookup itself failed, which must
		// not be reported as if it had succeeded.
		if msg := failed[id]; msg != "" {
			return "", fmt.Errorf("unable to look up %s %d: %s", idType, id, msg)
		}
		return fmt.Sprintf("%d", id), nil
	}

	switch idType {
	case "user":
		userName, err := user.LookupId(strconv.Itoa(int(id)))
		if err == nil {
			return userName.Username, nil
		}
	case "group":
		groupName, err := user.LookupGroupId(strconv.Itoa(int(id)))
		if err == nil {
			return groupName.Name, nil
		}
	}

	return fmt.Sprintf("%d", id), nil
}

// appendNames resolves the --users and --groups values and appends them to the numeric ID lists,
// so that parseUserIdsInto and parseGroupIdsInto need no knowledge of names. It must run before
// the callers default an empty selection to "current".
func appendNames(userIds *[]string, users []string, groupIds *[]string, groups []string, nss bool) error {
	if err := rejectUnmergeableIds("uids", *userIds, users); err != nil {
		return err
	}
	if err := rejectUnmergeableIds("gids", *groupIds, groups); err != nil {
		return err
	}

	ids, err := namesToIds(users, "user", nss)
	if err != nil {
		return err
	}
	*userIds = append(*userIds, ids...)

	ids, err = namesToIds(groups, "group", nss)
	if err != nil {
		return err
	}
	*groupIds = append(*groupIds, ids...)

	return nil
}

// rejectUnmergeableIds errors when names are combined with anything but plain numeric IDs.
// Appending resolved names makes parseUserIdsInto take its list branch, which accepts only plain
// IDs, so keywords like "all" or "current" and ranges like 1000-2000 would otherwise fail with a
// confusing message further down.
func rejectUnmergeableIds(idFlag string, ids []string, names []string) error {
	if len(names) == 0 {
		return nil
	}
	for _, id := range ids {
		if _, err := strconv.ParseUint(id, 10, 32); err != nil {
			return fmt.Errorf("--%s %q cannot be combined with names", idFlag, id)
		}
	}
	return nil
}

// namesToIds resolves names to IDs, returned as decimal strings so they can be appended to the
// --uids and --gids values. An unresolvable name is an error: unlike printing, silently dropping
// an ID the user asked for would be wrong.
func namesToIds(names []string, idType string, nss bool) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(names))

	if nss && !build.CGO {
		req := idRequest{}
		if idType == "user" {
			req.Users = names
		} else {
			req.Groups = names
		}
		resp, err := resolver.resolve(req)
		if err != nil {
			return nil, err
		}
		found := resp.UserIDs
		if idType == "group" {
			found = resp.GroupIDs
		}
		for _, name := range names {
			id, ok := found[name]
			if !ok {
				if msg := resp.NameErrors[name]; msg != "" {
					return nil, fmt.Errorf("unable to look up %s %q: %s", idType, name, msg)
				}
				return nil, fmt.Errorf("unknown %s %q", idType, name)
			}
			ids = append(ids, strconv.FormatUint(uint64(id), 10))
		}
		return ids, nil
	}

	for _, name := range names {
		if idType == "user" {
			u, err := user.Lookup(name)
			if err != nil {
				return nil, err
			}
			ids = append(ids, u.Uid)
		} else {
			g, err := user.LookupGroup(name)
			if err != nil {
				return nil, err
			}
			ids = append(ids, g.Gid)
		}
	}
	return ids, nil
}
