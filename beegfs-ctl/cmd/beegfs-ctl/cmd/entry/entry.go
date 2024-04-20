package entry

import (
	"fmt"

	"github.com/spf13/cobra"
)

func NewEntryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "entry",
		Short: "Interact with files and directories in BeeGFS.",
	}

	cmd.AddCommand(newEntryInfoCmd(), newEntrySetCmd())
	return cmd
}

// TODO: https://github.com/ThinkParQ/beegfs-ctl/issues/20
// Implement `ModeSetPattern`
func newEntrySetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set",
		Short: "Configure stripe patterns, storage pools, remote storage targets, and more.",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("Not implemented (yet).")
			return nil
		},
	}

	return cmd
}
