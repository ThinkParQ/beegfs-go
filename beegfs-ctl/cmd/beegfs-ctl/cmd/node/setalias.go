package node

import (
	"fmt"

	"github.com/spf13/cobra"
	nodeCmd "github.com/thinkparq/beegfs-ctl/pkg/ctl/node"
	"github.com/thinkparq/gobee/beegfs"
)

func newSetAliasCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-alias node alias",
		Short: "Sets a node alias",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			eid, err := beegfs.NewNodeParser().Parse(args[0])
			if err != nil {
				return err
			}

			newAlias, err := beegfs.AliasFromString(args[1])
			if err != nil {
				return err
			}

			return runSetAliasCmd(cmd, eid, newAlias)
		},
	}

	return cmd
}

func runSetAliasCmd(cmd *cobra.Command, eid beegfs.EntityId, newAlias beegfs.Alias) error {
	err := nodeCmd.SetAlias(cmd.Context(), eid, newAlias)
	if err != nil {
		return err
	}

	fmt.Printf("Alias set to %s\n", newAlias)

	return nil
}
