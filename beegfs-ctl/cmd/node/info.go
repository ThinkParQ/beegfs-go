package node

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/cmd/util"
	"github.com/thinkparq/beegfs-ctl/internal"
)

var infoCmd = &cobra.Command{
	Use:   "info <nodeAlias>",
	Short: "Shows information about a specific node",
	Args:  cobra.ExactArgs(1),
	RunE:  runInfoCmd,
}

func runInfoCmd(cmd *cobra.Command, args []string) error {
	alias := args[0]

	info, err := internal.GetNodeInfo(cmd.Context(), alias)
	if err != nil {
		return err
	}

	w := util.NewTableWriter(os.Stdout)
	defer w.Flush()

	fmt.Fprintf(&w, "Address\tName\tType\t\n")
	for _, e := range info.GetNics() {
		fmt.Fprintf(&w, "%s\t%s\t%s\t\n", e.Addr, e.Name, e.NicType.String())
	}

	return nil
}
