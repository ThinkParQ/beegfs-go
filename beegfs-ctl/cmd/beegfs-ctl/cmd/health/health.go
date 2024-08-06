package health

import (
	"github.com/spf13/cobra"
)

func NewHealthCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Inspect the health of BeeGFS",
	}
	cmd.AddCommand(newCheckCmd(), newNetCmd(), newDFCmd())
	return cmd
}
