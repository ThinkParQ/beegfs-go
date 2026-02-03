package rst

import "github.com/spf13/cobra"

func NewRSTCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remote",
		Aliases: []string{"remote-storage-target", "rst"},
		Short:   "Interact with Remote Storage Targets",
		Args:    cobra.NoArgs,
	}

	cmd.AddCommand(newPushCmd(), newPullCmd(), newJobCmd(), newListCmd(), newStatusCmd(), newCleanupOrphanedCmd())

	return cmd
}
