package index

import (
	"github.com/spf13/cobra"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "index",
		Short: "Allows to create/update and run operations on the Filesystem Index.",
		Long:  "BeeGFS Hive index tool allows to create/update and run different metadata operations on the Index.",
	}

	cmd.AddCommand(newCreateCmd())
	cmd.AddCommand(newStatCmd())

	return cmd
}
