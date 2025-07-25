package entry

import (
	"github.com/spf13/cobra"
)

func NewEntryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "entry",
		Short: "Interact with files and directories in BeeGFS",
		Args:  cobra.NoArgs,
	}

	cmd.AddCommand(newEntryInfoCmd(), newEntrySetCmd(), newEntryDisposalCmd(), newMigrateCmd(), newCreateCmd(), newRefreshEntryInfoCmd())
	return cmd
}
