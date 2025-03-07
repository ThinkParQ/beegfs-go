package pool

import (
	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/pool"
)

func newSetAliasCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-alias <storage-pool> <alias>",
		Short: "Set storage pool aliases",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			eid, err := beegfs.NewEntityIdParser(16, beegfs.Storage).Parse(args[0])
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

	cmdfmt.Printf("Alias set to %s\n", newAlias)

	return nil
}
