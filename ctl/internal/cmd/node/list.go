package node

import (
	"fmt"
	"slices"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/node"
)

// Creates new list nodes command. Run when the command line tools structure is built, will be
// invoked by cobra
func newListCmd() *cobra.Command {
	// The commands configuration. No additional data conversion needed here, so its arguments
	// are filled directly from the command line flags below.
	cfg := backend.GetNodes_Config{}
	// Ctl shall exit with a non-zero value if any node is completely unreachable.
	// Note that this is not needed by the actual command, so it is not part of its config.
	reachabilityError := false

	// Define cobra command
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List BeeGFS nodes.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runListCmd(cmd, cfg, reachabilityError)
		},
	}

	// Define commands flags
	cmd.Flags().Var(beegfs.NewNodeTypePFlag(&cfg.FilterByNodeType, beegfs.Meta, beegfs.Storage, beegfs.Client, beegfs.Management), "node-type",
		"Only show nodes of the given type.")
	cmd.Flags().BoolVar(&cfg.WithNics, "with-nics", false,
		"Include the list of network addresses/interfaces the nodes reported to the management service.")
	cmd.Flags().BoolVar(&cfg.ReachabilityCheck, "reachability-check", false,
		"Check each node is alive and responding to requests (from the local machine).")
	cmd.Flags().DurationVar(&cfg.ReachabilityTimeout, "reachability-timeout", 1*time.Second,
		"Define the waiting time for responses when using --reachability-check.")
	cmd.Flags().BoolVar(&reachabilityError, "reachability-error", false,
		"Return an error if at least one node is completely unreachable.")

	return cmd
}

// Execute the list subcommand. This function is meant to process the user input, call the actual
// command handler and process its result (e.g. format the output). The actual command handling code
// shall be put under pkg/ctl with its own interface and called from here. This strict separation
// allows the implementation of potential alternative frontends later.
func runListCmd(cmd *cobra.Command, cfg backend.GetNodes_Config,
	reachabilityError bool) error {

	// Execute the actual command work
	nodes, err := backend.GetNodes(cmd.Context(), cfg)
	if err != nil {
		return err
	}

	// Sort output
	slices.SortFunc(nodes, func(a, b *backend.GetNodes_Node) int {
		if a.Node.Id.NodeType == b.Node.Id.NodeType {
			return int(a.Node.Id.NumId - b.Node.Id.NumId)
		} else {
			return int(a.Node.Id.NodeType - b.Node.Id.NodeType)
		}
	})

	allColumns := []string{"uid", "id", "alias", "nics", "reachable"}
	defaultColumns := []string{"id", "alias"}

	if viper.GetBool(config.DebugKey) {
		defaultColumns = allColumns
	} else {
		if cfg.WithNics || cfg.ReachabilityCheck {
			defaultColumns = append(defaultColumns, "nics")
			if cfg.ReachabilityCheck {
				defaultColumns = append(defaultColumns, "reachable")
			}
		}
	}

	tbl := cmdfmt.NewTableWrapper(allColumns, defaultColumns)
	defer tbl.PrintRemaining()
	hasUnreachableNode := false

	// Print and process node list
	for _, node := range nodes {

		nics := ""
		reachableNics := ""
		hasReachableNic := false
		for i, nic := range node.Nics {
			if i != 0 {
				nics += "\n"
				reachableNics += "\n"
			}
			nics += fmt.Sprintf("%s:%s (%s)", nic.Nic.Type, nic.Nic.Name, nic.Nic.Addr)
			if nic.Reachable {
				hasReachableNic = hasReachableNic || nic.Reachable
				reachableNics += "yes"
			} else {
				if cfg.ReachabilityCheck {
					reachableNics += "no"
				} else {
					reachableNics += "?"
				}
			}
		}
		hasUnreachableNode = hasUnreachableNode || !hasReachableNic

		tbl.Row(
			node.Node.Uid,
			node.Node.Id,
			node.Node.Alias,
			nics,
			reachableNics,
		)
	}

	if reachabilityError && hasUnreachableNode {
		return util.NewCtlError(fmt.Errorf("at least one node is unreachable"), 5)
	}

	return nil
}
