package entry

import (
	"github.com/spf13/cobra"
)

func NewEntryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "entry",
		Short: "Interact with files and directories in BeeGFS",
	}

	cmd.AddCommand(newEntryInfoCmd(), newEntrySetCmd(), newEntryDisposalCmd(), newMigrateCmd(), newCreateCmd())
	return cmd
}
