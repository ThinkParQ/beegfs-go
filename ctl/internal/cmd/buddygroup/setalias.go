package buddygroup

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/v8/common/beegfs"
	backend "github.com/thinkparq/beegfs-go/v8/ctl/pkg/ctl/buddygroup"
)

func newSetAliasCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-alias <buddygroup> <alias>",
		Short: "Set a buddy group alias.",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			eid, err := beegfs.NewEntityIdParser(16, beegfs.Meta, beegfs.Storage).Parse(args[0])
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
	err := backend.SetAlias(cmd.Context(), eid, newAlias)
	if err != nil {
		return err
	}

	fmt.Printf("Alias set to %s\n", newAlias)

	return nil
}
