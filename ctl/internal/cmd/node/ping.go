package node

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/node"
	"go.uber.org/zap"
)

// Creates new "ping" command
func newPingCmd() *cobra.Command {
	cfg := node.PingConfig{NodeIDs: []beegfs.EntityId{}}

	cmd := &cobra.Command{
		Use:   "ping [(node|nodeType) ...]",
		Short: "Ping BeeGFS nodes through the client module",
		Long: fmt.Sprintf(`Ping uses a mounted BeeGFS client to ping nodes that are part of the BeeGFS
instance managed by the configured mgmtd.

If no mount point is supplied, the first mount point for the configured BeeGFS
instance is used automatically.

Accepted positional arguments can be a mix of individual node aliases/IDs or
node types. If no positional arguments are passed, all nodes known to the
configured mgmtd will be pinged. Only nodes that match will be pinged and any
duplicates will be ignored.

Optionally, the ping count (-c/--count) for each node and the interval
(-i/--interval) between individual pings can be supplied. In most cases
the defaults (10 pings, 1ms interval) should work fine.

If -p/--parallel is supplied, nodes will be pinged in parallel instead of
sequentially. The global --%s flag will be used to determine how
many pings to do in parallel. `, config.NumWorkersKey),
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runPingCmd(cmd, cfg)
		},
	}

	cmd.Flags().StringVarP(&cfg.Mountpoint, "mount-point", "m", "",
		"Mount point of the client that will be used to ping the node. Optional.\nIf not configured the first mount point found for the configured mgmtd will be used.")
	cmd.Flags().Uint32VarP(&cfg.Count, "count", "c", 10, "How many pings to send")
	cmd.Flags().DurationVarP(&cfg.Interval, "interval", "i", 1*time.Millisecond, "Time in ms to wait between subsequent pings")
	cmd.Flags().BoolVarP(&cfg.Parallel, "parallel", "p", false,
		fmt.Sprintf("Ping nodes in parallel. Uses --%s to determine how many parallel pings to do.", config.NumWorkersKey))

	return cmd
}

func runPingCmd(cmd *cobra.Command, cfg node.PingConfig) error {
	log, _ := config.GetLogger()

	if cfg.Count == 0 {
		return fmt.Errorf("ping count can not be zero")
	}

	if cmd.Flags().NArg() > 0 {
		var nodeNames []string
		addedNodes := make(map[string]struct{}, cmd.Flags().NArg())
		for _, node := range cmd.Flags().Args() {
			if _, exists := addedNodes[node]; !exists {
				nodeType := beegfs.NodeTypeFromString(node)
				if nodeType != beegfs.InvalidNodeType {
					cfg.NodeTypes = append(cfg.NodeTypes, nodeType)
				} else {
					nodeNames = append(nodeNames, node)
				}
				addedNodes[node] = struct{}{}
			}
		}

		if len(nodeNames) > 0 {
			idParser := beegfs.NewEntityIdSliceParser(16, beegfs.Management, beegfs.Meta, beegfs.Storage)

			var err error
			if cfg.NodeIDs, err = idParser.Parse(strings.Join(nodeNames, ",")); err != nil {
				return fmt.Errorf("unable to parse args: %w", err)
			}

			log.Debug("Parsed nodeIDs:", zap.Any("nodeIDs", cfg.NodeIDs))
			if len(cfg.NodeIDs) != len(nodeNames) {
				return fmt.Errorf("invalid node or node type! Check node list and try again")
			}

		}
	}

	results, errs, err := node.PingNodes(cmd.Context(), cfg)
	if err != nil {
		return err
	}

	successful := 0
	failed := 0
	conv := 1000.0 // measurements are in ns, but we want to display µs
	unit := "µs"
fetch_results:
	for {
		var res *node.PingResult
		var err *node.PingError
		var ok bool
		select {
		case res, ok = <-results:
			if !ok {
				log.Debug("Results channel closed. Exiting")
				break fetch_results
			}
			successful += 1
		case err, ok = <-errs:
			if !ok {
				log.Debug("Errors channel closed. Exiting")
				break fetch_results
			}
			failed += 1
		}
		if err != nil {
			fmt.Printf("Error pinging %v\n", err.NodeID)
			fmt.Printf("================================================================================\n")
			fmt.Println(err.Error())
			fmt.Printf("Is the node online and reachable by the client?\n\n")
			continue fetch_results
		}
		fmt.Printf("Pinged node %v. Successful: %d, failed: %d.\n", res.Node.Id.StringLong(), res.OutSuccess, res.OutErrors)
		fmt.Printf("================================================================================\n")
		min := math.MaxFloat32
		max := 0.0
		total := 0.0
		times := []float64{}
		for i := 0; i < int(res.OutSuccess); i++ {
			t := float64(res.OutPingTime[i])
			fmt.Printf("Ping time:\t%6.2f%s\t(%6.0fns) (protocol: %s)\n", t/conv, unit, t, res.OutPingType[i])
			min = math.Min(min, t/conv)
			max = math.Max(max, t/conv)
			times = append(times, t)
			total += t
		}
		slices.Sort(times)
		fmt.Printf("Average: %.2f%s, Median: %.2f%s, Min: %.2f%s, Max: %.2f%s\n\n",
			total/float64(cfg.Count)/conv, unit,
			times[cfg.Count/2]/conv, unit,
			min, unit, max, unit)
	}

	if failed > 0 {
		if successful > 0 {
			return util.NewCtlError(fmt.Errorf("ping failed for at least one node"), util.PartialSuccess)
		} else {
			return fmt.Errorf("ping failed for all nodes")
		}
	}
	return nil
}
