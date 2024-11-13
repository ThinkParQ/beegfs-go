package node

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/node"
	pm "github.com/thinkparq/protobuf/go/management"
)

type deleteNode_Config struct {
	node    beegfs.EntityId
	execute bool
}

func newDeleteCmd() *cobra.Command {
	cfg := deleteNode_Config{}

	cmd := &cobra.Command{
		Use:   "delete <node>",
		Short: "Delete a node",
		Long: `Delete a node.

WARNING: Deleting non-empty nodes will break your file system. Don't do this unless you know what you are doing.
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spp := beegfs.NewEntityIdParser(16, beegfs.Client, beegfs.Meta, beegfs.Storage)
			p, err := spp.Parse(args[0])
			if err != nil {
				return err
			}
			cfg.node = p

			return runDeleteCmd(cmd, cfg)
		},
	}

	cmd.Flags().BoolVar(&cfg.execute, "yes", false, "This command is destructive and by default runs in dry-run mode. Specify --yes for it to actually take action.")

	return cmd
}

func runDeleteCmd(cmd *cobra.Command, cfg deleteNode_Config) error {
	resp, err := backend.Delete(cmd.Context(), &pm.DeleteNodeRequest{
		Node:    cfg.node.ToProto(),
		Execute: &cfg.execute,
	})
	if err != nil {
		return err
	}

	res, err := beegfs.EntityIdSetFromProto(resp.Node)
	if cfg.execute {
		if err != nil {
			fmt.Printf("Node deleted, but received no id info from the server. Please verify the deletion using the `node list` command.\n")
		} else {
			fmt.Printf("Node deleted: %s\n", res)
		}
	} else {
		if err != nil {
			// Since it was a dry run, we report this error
			return fmt.Errorf("received no id info from the server")
		} else {
			fmt.Printf("Node can be deleted: %s\n", res)
			if res.LegacyId.NodeType == beegfs.Meta {
				fmt.Print("Deleting a meta node might cause adverse effects to your file system and should only be done if no files using it or its internal target are left. If you really want to delete the node, please add the --yes flag to the command. ")
			} else {
				fmt.Print("Deleting a node should be safe if no targets are left assigned to it. If you really want to delete the node, please add the --yes flag to the command. ")
			}
		}
	}

	return nil
}
