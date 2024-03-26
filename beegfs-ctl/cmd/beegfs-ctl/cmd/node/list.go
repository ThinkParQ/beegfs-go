package node

import (
	"fmt"
	"os"
	"slices"
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/util"
	"github.com/thinkparq/beegfs-ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
	nodeCmd "github.com/thinkparq/beegfs-ctl/pkg/ctl/node"
	"github.com/thinkparq/gobee/beegfs"
)

// Creates new list nodes command. Run when the command line tools structure is built, will be
// invoked by cobra
func newListCmd() *cobra.Command {
	// The commands configuration. No additional data conversion needed here, so its arguments
	// are filled directly from the command line flags below.
	cfg := nodeCmd.GetNodeList_Config{}
	// Ctl shall exit with a non-zero value if any node is completely unreachable.
	// Note that this is not needed by the actual command, so it is not part of its config.
	reachabilityError := false

	// Define cobra command
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List BeeGFS nodes",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runListCmd(cmd, cfg, reachabilityError)
		},
	}

	// Define commands flags
	cmd.Flags().Var(beegfs.NewNodeTypePFlag(&cfg.FilterByNodeType, beegfs.Meta, beegfs.Storage, beegfs.Client, beegfs.Management), "nodeType",
		"Only show nodes of the given type")
	cmd.Flags().BoolVar(&cfg.WithNics, "withNics", false,
		"Also request the list of network addresses/interfaces the nodes report to management")
	cmd.Flags().BoolVar(&cfg.ReachabilityCheck, "reachabilityCheck", false,
		"Checks each node for being alive and responding to requests (from the local machine)")
	cmd.Flags().DurationVar(&cfg.ReachabilityTimeout, "reachabilityTimeout", 1*time.Second,
		"Defines the waiting time for responses when using --reachabilityCheck")
	cmd.Flags().BoolVar(&reachabilityError, "reachabilityError", false,
		"Return an error if at least one node is completely unreachable")

	return cmd
}

// Execute the list subcommand. This function is meant to process the user input, call the actual
// command handler and process its result (e.g. format the output). The actual command handling code
// shall be put under pkg/ctl with its own interface and called from here. This strict separation
// allows the implementation of potential alternative frontends later.
func runListCmd(cmd *cobra.Command, cfg nodeCmd.GetNodeList_Config,
	reachabilityError bool) error {

	// Execute the actual command work
	nodes, err := nodeCmd.GetNodeList(cmd.Context(), cfg)
	if err != nil {
		return err
	}

	// Sort output
	slices.SortFunc(nodes, func(a, b *nodeCmd.GetNodeList_Node) int {
		if a.Node.Id.Type == b.Node.Id.Type {
			return int(a.Node.Id.Id - b.Node.Id.Id)
		} else {
			return int(a.Node.Id.Type - b.Node.Id.Type)
		}
		// return strings.Compare(a.Node.Alias.String(), b.Node.Alias.String())
	})

	// Create a new tabwriter with fixed settings for spacings and so on. This should be usually
	// used for table like output
	w := cmdfmt.NewTableWriter(os.Stdout)
	// The returned Writer must be flushed to actually output anything. We do so after we are done
	// writing to it.
	defer w.Flush()

	// Print the tables header. Columns must end with a tabstop \t, including the last one.
	if config.Get().Debug {
		fmt.Fprint(&w, "UID\t")
	}
	fmt.Fprint(&w, "NodeID\tAlias\t")
	if cfg.WithNics || cfg.ReachabilityCheck {
		fmt.Fprint(&w, "Nics\t\t\t")
		if cfg.ReachabilityCheck {
			fmt.Fprint(&w, "Reachable\t")
		}
	}
	fmt.Fprint(&w, "\n")

	hasUnreachableNode := false

	// Print and process node list
	for _, node := range nodes {
		// Print a line corresponding to the columns above
		if config.Get().Debug {
			fmt.Fprintf(&w, "%d\t", node.Node.Uid)
		}
		fmt.Fprintf(&w, "%s\t%s\t", node.Node.Id, node.Node.Alias)

		// If we requested the nic list for each node, print them, one each line.
		if cfg.WithNics || cfg.ReachabilityCheck {
			hasReachableNic := false
			first := true
			for _, nic := range node.Nics {
				// Align Nic on additional lines
				if !first {
					fmt.Fprintf(&w, "\n\t\t")
					if config.Get().Debug {
						fmt.Fprintf(&w, "\t")
					}
				}
				first = false

				fmt.Fprintf(&w, "%s\t%s\t%s\t", nic.Nic.Name, nic.Nic.Addr, nic.Nic.Type)

				if cfg.ReachabilityCheck {
					if nic.Reachable {
						hasReachableNic = true
						fmt.Fprint(&w, "yes")
					} else {
						fmt.Fprint(&w, "no")
					}
				}
			}
			fmt.Fprint(&w, "\t")

			// A node is (completely) unreachable if all its Nics are unreachable.
			if !hasReachableNic {
				hasUnreachableNode = true
			}
		}

		fmt.Fprint(&w, "\n")
	}

	if reachabilityError && hasUnreachableNode {
		return util.NewCtlError(fmt.Errorf("at least one node is unreachable"), 5)
	}

	return nil
}
