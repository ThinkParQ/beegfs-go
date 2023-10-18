package node

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/cmd/util"
	"github.com/thinkparq/beegfs-ctl/internal"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List BeeGFS nodes",
	RunE:  runListCmd,
}

func runListCmd(cmd *cobra.Command, args []string) error {
	nodes, err := internal.GetNodeList(cmd.Context())
	if err != nil {
		return err
	}

	w := util.NewTableWriter(os.Stdout)
	defer w.Flush()

	fmt.Fprintf(&w, "NodeUID\tNodeID\tType\tAlias\tPort (BeeMsg)\tPort (gRPC)\t\n")

	for _, e := range nodes.GetNodes() {
		fmt.Fprintf(&w, "%d\t%d\t%s\t%s\t%d\t%s\t\n", e.NodeUid, e.NodeId, e.NodeType, e.Alias, e.BeemsgPort, "-")
	}

	return nil
}
