package health

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/procfs"
)

const (
	forceConnectionsFlag  = "force-connections"
	connectionTimeoutFlag = "connection-timeout"
)

type netCfg struct {
	noFilterByMgmtd   bool
	connectionTimeout time.Duration
}

func newNetCmd() *cobra.Command {

	frontendCfg := netCfg{}
	backendCfg := procfs.GetBeeGFSClientsConfig{}

	cmd := &cobra.Command{
		Use:     "network [<mount-path>] ...",
		Aliases: []string{"net"},
		Short:   "Show all local network connections currently established to BeeGFS server nodes (beegfs-net).",
		Long: `Show all local network connections currently established to BeeGFS server nodes (beegfs-net).

If there are multiple BeeGFS mount points, connections will be displayed for each.

NOTE: BeeGFS clients establish connections on demand, and periodically drop idle connections.
Thus the lack of connections (<none>) to a particular server does not indicate any issues.
By default this command will first use "df" to force the client module to establish connections to storage nodes.
This can cause the command to block if any storage nodes are unreachable (use --skip-df if needed).
		`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runNetCmd(cmd, args, frontendCfg, backendCfg)
		},
	}
	cmd.Flags().DurationVar(&frontendCfg.connectionTimeout, connectionTimeoutFlag, time.Second*1, "Timeout when attempting to establish connections for the network connection check.")
	cmd.Flags().BoolVar(&backendCfg.ForceConnections, forceConnectionsFlag, true, "By default the network connection check will first attempt to establish storage server connections by running df. Connections may be <none> if this is set to false.")
	cmd.Flags().BoolVar(&frontendCfg.noFilterByMgmtd, "all", false, "By default only BeeGFS mounts for the management service configured with CTL are displayed. Set to include all BeeGFS mounts found on this client.")
	return cmd
}

func runNetCmd(cmd *cobra.Command, filterByMounts []string, frontendCfg netCfg, backendCfg procfs.GetBeeGFSClientsConfig) error {
	procCtx, procCtxCancel := context.WithTimeout(cmd.Context(), frontendCfg.connectionTimeout)
	clients, err := getFilteredClientList(procCtx, frontendCfg.noFilterByMgmtd, filterByMounts, backendCfg)
	procCtxCancel()
	if err != nil {
		return err
	}
	for _, client := range clients {
		printClientHeader(client, "=")
		printBeeGFSNet(client)
	}
	return nil
}

// printBeeGFSNet() prints similar output as the old "beegfs-net" command.
func printBeeGFSNet(client procfs.Client) {
	printNodes := func(nodes []procfs.Node) {
		for _, node := range nodes {
			fmt.Printf("%s [ID: %d]\n", node.Alias, node.NumID)
			fmt.Printf("   Connections: ")
			if len(node.Peers) == 0 {
				fmt.Printf("<none>")
			}
			for _, peer := range node.Peers {
				fallback := ""
				if peer.Fallback {
					fallback = " [fallback route]"
				}
				fmt.Printf("%s: %d (%s%s);", peer.Type, peer.Connections, peer.IP, fallback)
			}
			fmt.Println("")
		}
	}

	printHeader("Management Node", "-")
	printNodes(client.MgmtdNodes)
	printHeader("Metadata Nodes", "-")
	printNodes(client.MetaNodes)
	printHeader("Storage Nodes", "-")
	printNodes(client.StorageNodes)
	fmt.Println()
}
