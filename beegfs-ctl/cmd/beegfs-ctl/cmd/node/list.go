package node

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/cmd/beegfs-ctl/util"
	"github.com/thinkparq/beegfs-ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/node"
)

// NOTE
// This file is meant as an example to create your own commands. Please follow the general
// structure, but do not copy the excessive generic comments. Only write meaningful, command specific
// comments.

// Config flags to control output. Everything that does not need to get passed into the actual
// command (e.g. node.GetNodeList()) should go in here to provide a bit of separation.
type getNodeList_Config struct {
	// Controls whether the app should exit with an error code if at least one node is not reachable
	reachabilityError bool
	// Filter the output by nodetype.
	filterByNodeType string
}

// Creates new list nodes command. Run when the command line tools structure is built.
func newListCmd() *cobra.Command {
	// In this case, cfg correspond perfectly with the command implementations config struct, so
	// we just use it directly to store cfg. If this was not the case, e.g. the passed cfg might
	// need some transformation, a separate struct can be used.
	cfg := &node.GetNodeList_Config{}
	localCfg := &getNodeList_Config{}

	cmd := &cobra.Command{
		// The long form of the command
		Use: "list",
		// The short description of the command
		Short: "List BeeGFS nodes",
		// Called when the command is actually run
		RunE: func(cmd *cobra.Command, args []string) error {
			// Forward execution to its own function
			return runListCmd(cmd, cfg, localCfg)
		},

		// There are a lot more optional argument to give here if needed, look up the documentation.
	}

	// Add flags to the command
	cmd.Flags().StringVar(&localCfg.filterByNodeType, "nodeType", "",
		"Only show nodes of the given type")
	cmd.Flags().BoolVar(&cfg.WithNics, "withNics", false,
		"Also request the list of network addresses/interfaces the nodes report to management")
	cmd.Flags().BoolVar(&cfg.ReachabilityCheck, "reachabilityCheck", false,
		"Checks each node for being alive and responding to requests (from the local machine)")
	cmd.Flags().DurationVar(&cfg.ReachabilityTimeout, "reachabilityTimeout", 1*time.Second,
		"Defines the waiting time for responses when using --reachabilityCheck")
	cmd.Flags().BoolVar(&localCfg.reachabilityError, "reachabilityError", false,
		"Return an error if at least one node is completely unreachable")

	return cmd
}

// Execute the list subcommand. This function is meant to process the user input, call the actual
// command handler and process its result (e.g. format the output). The actual command handling code
// shall be put under pkg/ctl with its own interface and called from here. This strict separation
// allows the implementation of potential alternative frontends later.
func runListCmd(cmd *cobra.Command, cfg *node.GetNodeList_Config,
	localCfg *getNodeList_Config) error {
	// Execute the actual command work
	nodes, err := node.GetNodeList(cmd.Context(), *cfg)
	if err != nil {
		return err
	}

	// Create a new tabwriter with fixed settings for spacings and so on. This should be usually
	// used for table like output
	w := cmdfmt.NewTableWriter(os.Stdout)
	// The returned Writer must be flushed to actually output anything. We do so after we are done
	// writing to it.
	defer w.Flush()

	// Print the tables header. Columns must end with a tabstop \t, including the last one.
	if config.Get().Debug {
		fmt.Fprint(&w, "NodeUID\t")
	}
	fmt.Fprint(&w, "NodeID\tType\tAlias\t")
	if cfg.WithNics || cfg.ReachabilityCheck {
		fmt.Fprint(&w, "Nics\t")
	}
	fmt.Fprint(&w, "\n")

	hasUnreachableNode := false

	// Print and process node list
	for _, node := range nodes {
		// Respect filter
		if localCfg.filterByNodeType != "" &&
			!strings.EqualFold(node.Type, localCfg.filterByNodeType) {
			continue
		}

		// Print a line corresponding to the columns above
		if config.Get().Debug {
			fmt.Fprintf(&w, "%d\t", node.Uid)
		}
		fmt.Fprintf(&w, "%d\t%s\t%s\t", node.Id, node.Type, node.Alias)

		// If we requested the nic list for each node, print the available addresses, separated by ,
		if cfg.WithNics || cfg.ReachabilityCheck {
			hasReachableNic := false
			for _, nic := range node.Nics {
				r := ""
				if cfg.ReachabilityCheck {
					if nic.Reachable {
						hasReachableNic = true
						r = ", reachable: yes"
					} else {
						r = ", reachable: no"
					}
				}

				fmt.Fprintf(&w, "%s[addr: %s:%d, type: %s%s], ", nic.Name, nic.Addr, node.BeemsgPort, nic.Type, r)
			}
			fmt.Fprint(&w, "\t")

			if !hasReachableNic {
				hasUnreachableNode = true
			}
		}

		fmt.Fprint(&w, "\n")
	}

	if localCfg.reachabilityError && hasUnreachableNode {
		return util.NewCtlError(fmt.Errorf("at least one node is unreachable"), 5)
	}

	return nil
}
