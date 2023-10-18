package cmd

import (
	"context"
	"os"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/cmd/node"
	"github.com/thinkparq/beegfs-ctl/internal"
)

var rootCmd = &cobra.Command{
	Use:   "beegfs-ctl",
	Short: "The BeeGFS commandline control tool",
	Long: `The BeeGFS commandline control tool blablablabla ablablablabalablabl
	blabla`,
	SilenceUsage: true,
}

func init() {
	rootCmd.AddCommand(node.NodeCmd)
}

func Execute() {
	ctx := context.Background()
	ctx = context.WithValue(ctx, internal.CtxConfigKey{}, &internal.Config{ManagementAddr: "127.0.0.1:8010"})

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}
