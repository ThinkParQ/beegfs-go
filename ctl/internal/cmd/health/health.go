package health

import (
	"github.com/spf13/cobra"
)

func NewHealthCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Inspect the health of BeeGFS",
		Args:  cobra.NoArgs,
	}
	cmd.AddCommand(newCheckCmd(), newNetCmd(), newDFCmd(), newBundleCmd())
	return cmd
}
