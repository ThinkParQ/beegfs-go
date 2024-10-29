package buddygroup

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/v8/common/beegfs"
	backend "github.com/thinkparq/beegfs-go/v8/ctl/pkg/ctl/buddygroup"
	pm "github.com/thinkparq/protobuf/go/management"
)

type createBuddyGroup_Config struct {
	nodeType beegfs.NodeType
	groupId  uint16
	alias    beegfs.Alias
	pTarget  beegfs.EntityId
	sTarget  beegfs.EntityId
}

func newCreateBuddyGroupCmd() *cobra.Command {
	cfg := createBuddyGroup_Config{pTarget: beegfs.InvalidEntityId{}, sTarget: beegfs.InvalidEntityId{}}

	cmd := &cobra.Command{
		Use:   "create <alias>",
		Short: "Create a buddy group.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			alias, err := beegfs.AliasFromString(args[0])
			if err != nil {
				return err
			}
			cfg.alias = alias

			return runCreateBuddyGroupCmd(cmd, cfg)
		},
	}

	cmd.Flags().Var(beegfs.NewNodeTypePFlag(&cfg.nodeType, beegfs.Meta, beegfs.Storage),
		"node-type", "Node type of the new buddy group")
	cmd.MarkFlagRequired("node-type")
	cmd.Flags().Uint16Var(&cfg.groupId, "num-id", 0, "Set the numeric id of the new buddy group. Auto-generated if unspecified.")
	cmd.Flags().Var(beegfs.NewEntityIdPFlag(&cfg.pTarget, 16, beegfs.Meta, beegfs.Storage),
		"primary", "The primary target")
	cmd.MarkFlagRequired("primary")
	cmd.Flags().Var(beegfs.NewEntityIdPFlag(&cfg.sTarget, 16, beegfs.Meta, beegfs.Storage),
		"secondary", "The secondary target")
	cmd.MarkFlagRequired("secondary")

	return cmd
}

func runCreateBuddyGroupCmd(cmd *cobra.Command, cfg createBuddyGroup_Config) error {
	groupId := uint32(cfg.groupId)
	alias := string(cfg.alias)

	resp, err := backend.Create(cmd.Context(), &pm.CreateBuddyGroupRequest{
		NodeType:        cfg.nodeType.ToProto(),
		NumId:           &groupId,
		Alias:           &alias,
		PrimaryTarget:   cfg.pTarget.ToProto(),
		SecondaryTarget: cfg.sTarget.ToProto(),
	})
	if err != nil {
		return err
	}

	res, err := beegfs.EntityIdSetFromProto(resp.Group)
	if err != nil {
		fmt.Printf("Buddy group created, but received no id info from the server. Please verify the creation using the `mirror list` command.\n")
	} else {
		fmt.Printf("Buddy group created: %s\n", res)
	}

	return nil
}
