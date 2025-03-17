package buddygroup

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup"
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
		Short: "Create a buddy group",
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

func newCreateBuddyGroupsAutomaticCmd() *cobra.Command {
	cfg := backend.AutoCreateConfig{}

	cmd := &cobra.Command{
		Use:   "autocreate <nodeType>",
		Short: "Automatically create buddy groups for a given node type",
		Long: `Automatically create buddy groups for a given node type. An even number of
nodes/targets is required and all targets/nodes need to have the same amount of
space available. If needed, use the --ignore-* flags to override the checks.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			tpe := beegfs.NodeTypeFromString(args[0])
			if tpe == beegfs.InvalidNodeType {
				return fmt.Errorf("invalid node type: \"%s\"", args[0])
			}
			cfg.NodeType = tpe

			return runCreateBuddyGroupsAutomaticCmd(cmd, cfg)
		},
	}

	cmd.Flags().BoolVar(&cfg.IgnoreUneven, "ignore-uneven", false, "Create buddy mirrors, even if there is an uneven number of nodes/targets. Can leave one node/target unmirrored.")
	cmd.Flags().BoolVar(&cfg.IgnoreSpace, "ignore-space", false, "Ignore the total inodes/space available on nodes/targets.")

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
		cmdfmt.Printf("Buddy group created, but received no id info from the server. Please verify the creation using the `mirror list` command.\n")
	} else {
		cmdfmt.Printf("Buddy group created: %s\n", res)
	}

	return nil
}

func runCreateBuddyGroupsAutomaticCmd(cmd *cobra.Command, cfg backend.AutoCreateConfig) error {
	results, warnings, err := backend.AutoCreate(cmd.Context(), cfg)
	if err != nil {
		return err
	}
	for _, bg := range results {
		cmdfmt.Printf("Buddy group created: %s\n", bg)
	}
	if len(warnings) > 0 {
		for _, err := range warnings {
			cmdfmt.Printf("WARNING: %v\n", err)
		}
		return util.NewCtlError(fmt.Errorf("partial success. Warnings encountered (see output). Created %d buddy groups", len(results)), util.PartialSuccess)
	}
	return nil
}
