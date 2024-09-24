package pool

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/pool"
	pb "github.com/thinkparq/protobuf/go/beegfs"
	pm "github.com/thinkparq/protobuf/go/management"
)

type createPool_Config struct {
	poolId  uint16
	alias   beegfs.Alias
	targets []beegfs.EntityId
	groups  []beegfs.EntityId
}

func newCreatePoolCmd() *cobra.Command {
	var targets []string
	var groups []string

	cfg := createPool_Config{}

	cmd := &cobra.Command{
		Use:   "create <alias>",
		Short: "Create a storage pool.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			alias, err := beegfs.AliasFromString(args[0])
			if err != nil {
				return err
			}
			cfg.alias = alias

			tp := beegfs.NewEntityIdParser(16, beegfs.Storage)
			for _, t := range targets {
				t2, err := tp.Parse(t)
				if err != nil {
					return err
				}
				cfg.targets = append(cfg.targets, t2)
			}

			gp := beegfs.NewEntityIdParser(16, beegfs.Storage)
			for _, g := range groups {
				t2, err := gp.Parse(g)
				if err != nil {
					return err
				}
				cfg.groups = append(cfg.groups, t2)
			}

			return runCreatePoolCmd(cmd, cfg)
		},
	}

	cmd.Flags().Uint16Var(&cfg.poolId, "num-id", 0, "Set the numeric id of the new pool. Auto-generated if unspecified.")
	cmd.Flags().StringArrayVarP(&targets, "target", "t", nil, "Targets to move to the new pool")
	cmd.Flags().StringArrayVarP(&groups, "group", "g", nil, "Buddy groups to move to the new pool")

	return cmd
}

func runCreatePoolCmd(cmd *cobra.Command, cfg createPool_Config) error {
	poolId := uint32(cfg.poolId)
	alias := string(cfg.alias)

	targets := []*pb.EntityIdSet{}
	for _, t := range cfg.targets {
		targets = append(targets, t.ToProto())
	}

	groups := []*pb.EntityIdSet{}
	for _, t := range cfg.groups {
		groups = append(groups, t.ToProto())
	}

	resp, err := backend.Create(cmd.Context(), &pm.CreatePoolRequest{
		NodeType:    pb.NodeType_STORAGE.Enum(),
		Alias:       &alias,
		NumId:       &poolId,
		Targets:     targets,
		BuddyGroups: groups,
	})
	if err != nil {
		return err
	}

	res, err := beegfs.EntityIdSetFromProto(resp.Pool)
	if err != nil {
		fmt.Printf("Pool created, but received no id info from the server. Please verify the creation using the `pool list` command.\n")
	} else {
		fmt.Printf("Pool created: %s\n", res)
	}

	return nil
}
