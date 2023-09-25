package node

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/internal"
)

var listCmd = &cobra.Command{
	Use:   "list",
	Short: "List BeeGFS nodes",
	Run:   runListCmd,
}

func runListCmd(cmd *cobra.Command, args []string) {
	nodes, err := internal.GetNodes()
	if err != nil {
		fmt.Printf("Could not get node list: %s\n", err)
	}

	w := new(tabwriter.Writer)
	w.Init(os.Stdout, 8, 8, 0, ' ', 0)
	defer w.Flush()

	fmt.Fprintf(w, "NodeID \tNodeType \tAlias\t\n")
	for _, e := range nodes {
		fmt.Fprintf(w, "%d\t%d\tHello World\n", e.NodeID, e.NodeType)
	}
}
