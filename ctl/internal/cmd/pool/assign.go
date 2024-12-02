package pool

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/pool"
	pb "github.com/thinkparq/protobuf/go/beegfs"
	pm "github.com/thinkparq/protobuf/go/management"
)

type assignPool_Config struct {
	pool    beegfs.EntityId
	targets []beegfs.EntityId
	groups  []beegfs.EntityId
}

func newAssignPoolCmd() *cobra.Command {
	var targets []string
	var groups []string

	cfg := assignPool_Config{}

	cmd := &cobra.Command{
		Use:   "assign <pool>",
		Short: "Assign targets and buddy groups to a storage pool",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spp := beegfs.NewEntityIdParser(16, beegfs.Storage)
			p, err := spp.Parse(args[0])
			if err != nil {
				return err
			}
			cfg.pool = p

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

			return runAssignPoolCmd(cmd, cfg)
		},
	}

	cmd.Flags().StringSliceVarP(&targets, "targets", "t", nil, "Comma separated list of target(s) to move to the pool")
	cmd.Flags().StringSliceVarP(&groups, "groups", "g", nil, "Comma separated list of buddy group(s) to move to the pool")
	cmd.MarkFlagsOneRequired("targets", "groups")

	return cmd
}

func runAssignPoolCmd(cmd *cobra.Command, cfg assignPool_Config) error {
	targets := []*pb.EntityIdSet{}
	for _, t := range cfg.targets {
		targets = append(targets, t.ToProto())
	}

	groups := []*pb.EntityIdSet{}
	for _, t := range cfg.groups {
		groups = append(groups, t.ToProto())
	}

	resp, err := backend.Assign(cmd.Context(), &pm.AssignPoolRequest{
		Pool:        cfg.pool.ToProto(),
		Targets:     targets,
		BuddyGroups: groups,
	})
	if err != nil {
		return err
	}

	res, err := beegfs.EntityIdSetFromProto(resp.Pool)
	if err != nil {
		fmt.Printf("Pool assigned, but received no id info from the server. Please verify the assignment using the `pool list` command.\n")
	} else {
		fmt.Printf("Pool assigned: %s\n", res)
	}

	return nil
}
