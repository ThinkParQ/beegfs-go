package health

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	tgtFrontend "github.com/thinkparq/beegfs-go/ctl/internal/cmd/target"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	tgtBackend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
)

func newDFCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:         "capacity",
		Aliases:     []string{"df"},
		Short:       "Show available disk space and inodes on metadata targets, storage targets, and storage pools (beegfs-df)",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args:        cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			targets, err := tgtBackend.GetTargets(cmd.Context())
			if err != nil {
				return err
			}
			cfg := tgtFrontend.PrintConfig{Capacity: true}
			if config.OutputType(viper.GetString(config.OutputKey)).IsJSON() {
				// PrintTargetList honors --output; call it once (without the human section headers)
				// so JSON output is a single valid document rather than headers + two arrays. The
				// storage pool section is omitted for the same reason - it is an aggregate of the
				// targets already in this document, so consumers can group by storage_pool
				// themselves.
				tgtFrontend.PrintTargetList(cmd.Context(), cfg, targets)
			} else {
				printDF(cmd.Context(), targets, cfg)
			}
			return nil
		},
	}
	return cmd
}

// printDF() is a wrapper for PrintTargetList() that prints metadata and storage targets as separate
// lists sorted by target ID, followed by the storage targets aggregated per storage pool.
func printDF(ctx context.Context, targets []tgtBackend.GetTargets_Result, printConfig tgtFrontend.PrintConfig) {
	metaTargets := []tgtBackend.GetTargets_Result{}
	storageTargets := []tgtBackend.GetTargets_Result{}
	for _, tgt := range targets {
		if tgt.NodeType == beegfs.Meta {
			metaTargets = append(metaTargets, tgt)
		} else if tgt.NodeType == beegfs.Storage {
			storageTargets = append(storageTargets, tgt)
		}
	}
	sort.Slice(metaTargets, func(i, j int) bool {
		return metaTargets[i].Target.LegacyId.NumId <= metaTargets[j].Target.LegacyId.NumId
	})
	sort.Slice(storageTargets, func(i, j int) bool {
		return storageTargets[i].Target.LegacyId.NumId <= storageTargets[j].Target.LegacyId.NumId
	})

	printHeader("Metadata Targets", "-")
	tgtFrontend.PrintTargetList(ctx, printConfig, metaTargets)

	printHeader("Storage Targets", "-")
	tgtFrontend.PrintTargetList(ctx, printConfig, storageTargets)

	printHeader("Storage Pools", "-")
	printStoragePoolList(printConfig, aggregateStoragePools(storageTargets))
}

// storagePoolCapacity is the capacity and state of all storage targets in a single storage pool,
// rolled up into one entry. Space and inode fields stay nil until at least one target in the pool
// reports them so a pool nothing is known about is distinguishable from a genuinely empty one, the
// same way tgtBackend.GetTargets_Result treats unreported capacity.
type storagePoolCapacity struct {
	pool            beegfs.EntityIdSet
	targets         uint64
	reachability    map[string]uint64
	consistency     map[string]uint64
	capacityPools   map[string]uint64
	freeSpaceBytes  *uint64
	totalSpaceBytes *uint64
	freeInodes      *uint64
	totalInodes     *uint64
}

// aggregateStoragePools groups the given targets by the storage pool they are assigned to and sums
// their capacity, returning one entry per pool sorted by pool ID. Targets with no storage pool
// assignment are skipped. Note pools that currently have no targets are not represented because
// they contribute no capacity and the target list is the only input - this keeps printDF() a pure
// function of the target list already fetched by its callers.
func aggregateStoragePools(targets []tgtBackend.GetTargets_Result) []storagePoolCapacity {
	byPool := make(map[beegfs.Uid]*storagePoolCapacity)
	for _, t := range targets {
		if t.StoragePool == nil {
			continue
		}
		agg, ok := byPool[t.StoragePool.Uid]
		if !ok {
			agg = &storagePoolCapacity{
				pool:          t.StoragePool.Clone(),
				reachability:  make(map[string]uint64),
				consistency:   make(map[string]uint64),
				capacityPools: make(map[string]uint64),
			}
			byPool[t.StoragePool.Uid] = agg
		}

		agg.targets++
		agg.reachability[orUnknown(t.ReachabilityState)]++
		agg.consistency[orUnknown(t.ConsistencyState)]++
		agg.capacityPools[orUnknown(t.CapacityPool)]++
		addOptional(&agg.freeSpaceBytes, t.FreeSpaceBytes)
		addOptional(&agg.totalSpaceBytes, t.TotalSpaceBytes)
		addOptional(&agg.freeInodes, t.FreeInodes)
		addOptional(&agg.totalInodes, t.TotalInodes)
	}

	pools := make([]storagePoolCapacity, 0, len(byPool))
	for _, agg := range byPool {
		pools = append(pools, *agg)
	}
	sort.Slice(pools, func(i, j int) bool {
		return pools[i].pool.LegacyId.NumId < pools[j].pool.LegacyId.NumId
	})
	return pools
}

// addOptional adds an optional per-target value into an optional running total, leaving the total
// nil for as long as no target has reported a value.
func addOptional(total **uint64, value *uint64) {
	if value == nil {
		return
	}
	if *total == nil {
		sum := *value
		*total = &sum
		return
	}
	**total += *value
}

// printStoragePoolList prints the aggregated pools using the same columns as the storage target list
// wherever a column still makes sense once rolled up. Per-target identity columns (target ID, node)
// are replaced by the pool identity and a target count, and the per-target state columns become
// counts of the targets in each state.
func printStoragePoolList(cfg tgtFrontend.PrintConfig, pools []storagePoolCapacity) {
	allColumns := []string{"uid", "id", "alias", "targets", "reachability", "consistency", "cap_pool", "space", "space_used", "space_free", "inodes", "inodes_used", "inodes_free"}
	defaultColumns := []string{"id", "alias", "targets"}
	if viper.GetBool(config.DebugKey) {
		defaultColumns = allColumns
	} else {
		if cfg.State {
			defaultColumns = append(defaultColumns, "reachability", "consistency")
		}
		if cfg.Capacity {
			defaultColumns = append(defaultColumns, "cap_pool", "space", "space_used", "space_free", "inodes", "inodes_used", "inodes_free")
		}
	}

	tbl := cmdfmt.NewPrintomatic(allColumns, defaultColumns)
	defer tbl.PrintRemaining()

	for _, p := range pools {
		tbl.AddItem(
			p.pool.Uid,
			p.pool.LegacyId,
			p.pool.Alias,
			p.targets,
			summarizeStates(p.reachability, reachabilityOrder),
			summarizeStates(p.consistency, consistencyOrder),
			summarizeStates(p.capacityPools, capacityPoolOrder),
			cmdfmt.FormatSpace(p.totalSpaceBytes),
			cmdfmt.FormatSpaceUsed(p.totalSpaceBytes, p.freeSpaceBytes),
			cmdfmt.FormatSpace(p.freeSpaceBytes),
			cmdfmt.FormatInodes(p.totalInodes),
			cmdfmt.FormatInodesUsed(p.totalInodes, p.freeInodes),
			cmdfmt.FormatInodes(p.freeInodes),
		)
	}
}

// The order states are listed in when summarized for a pool, from best to worst, so the output is
// stable and the state that needs attention is easy to spot.
var (
	reachabilityOrder = []string{tgtBackend.ReachabilityOnline, tgtBackend.ReachabilityProbablyOffline, tgtBackend.ReachabilityOffline}
	consistencyOrder  = []string{tgtBackend.ConsistencyGood, tgtBackend.ConsistencyNeedsResync, tgtBackend.ConsistencyBad}
	capacityPoolOrder = []string{tgtBackend.CapacityNormal, tgtBackend.CapacityLow, tgtBackend.CapacityEmergency}
)

// summarizeStates renders how many targets are in each state, for example "2 Online, 1 Offline".
// States in the given order are listed first, then any state not in that order alphabetically so
// unrecognized values from a newer management service are still shown.
func summarizeStates(counts map[string]uint64, order []string) string {
	parts := make([]string, 0, len(counts))
	listed := make(map[string]bool, len(order))
	for _, state := range order {
		listed[state] = true
		if n := counts[state]; n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, state))
		}
	}

	unordered := make([]string, 0, len(counts))
	for state, n := range counts {
		if !listed[state] && n > 0 {
			unordered = append(unordered, state)
		}
	}
	sort.Strings(unordered)
	for _, state := range unordered {
		parts = append(parts, fmt.Sprintf("%d %s", counts[state], state))
	}

	if len(parts) == 0 {
		return cmdfmt.NotAvailable
	}
	return strings.Join(parts, ", ")
}

// orUnknown labels a state the management service left unset so it is counted under a name instead
// of an empty string.
func orUnknown(state string) string {
	if state == "" {
		return "Unknown"
	}
	return state
}
