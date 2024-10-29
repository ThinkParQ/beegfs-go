package target

import (
	"fmt"

	"github.com/dsnet/golib/unitconv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
)

type PrintConfig struct {
	NodeType beegfs.NodeType
	Capacity bool
	State    bool
	// Set to nil or beegfs.InvalidEntityId{} to include all targets.
	StoragePool beegfs.EntityId
}

func newListCmd() *cobra.Command {
	cfg := PrintConfig{StoragePool: beegfs.InvalidEntityId{}}

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List BeeGFS targets.",
		RunE: func(cmd *cobra.Command, args []string) error {
			targets, err := target.GetTargets(cmd.Context())
			if err != nil {
				return err
			}
			PrintTargetList(cfg, targets)
			return nil
		},
	}

	cmd.Flags().Var(beegfs.NewNodeTypePFlag(&cfg.NodeType, beegfs.Meta, beegfs.Storage), "node-type",
		"Filter by node type")
	cmd.Flags().Var(beegfs.NewEntityIdPFlag(&cfg.StoragePool, 16, beegfs.Storage), "pool", "Filter by storage pool.")
	cmd.Flags().BoolVar(&cfg.Capacity, "capacity", false, "Print capacity information.")
	cmd.Flags().BoolVar(&cfg.State, "state", false, "Print states as seen by the management service.")

	return cmd
}

// PrintTargetList prints out the provided list of targets based on the given list config. It is
// exported for reuse in other packages like health that need to print the target list.
func PrintTargetList(cfg PrintConfig, targets []target.GetTargets_Result) {

	allColumns := []string{"uid", "id", "alias", "on node", "pool", "reachability", "last contact", "consistency", "cap pool", "space", "sused", "sfree", "inodes", "iused", "ifree"}
	defaultColumns := []string{"id", "alias", "on node", "pool"}
	if viper.GetBool(config.DebugKey) {
		defaultColumns = allColumns
	} else {
		if cfg.State {
			defaultColumns = append(defaultColumns, "reachability", "last contact", "consistency")
		}
		if cfg.Capacity {
			defaultColumns = append(defaultColumns, "capacity pool", "space", "sused", "sfree", "inodes", "iused", "ifree")
		}
	}

	tbl := cmdfmt.NewTableWrapper(allColumns, defaultColumns)
	defer tbl.PrintRemaining()

	for _, t := range targets {
		if cfg.NodeType != beegfs.InvalidNodeType && t.NodeType != cfg.NodeType {
			continue
		}

		if cfg.StoragePool != nil {
			if t.StoragePool != nil {
				switch v := cfg.StoragePool.(type) {
				case beegfs.Uid:
					if t.StoragePool.Uid != v {
						continue
					}
				case beegfs.Alias:
					if t.StoragePool.Alias != v {
						continue
					}
				case beegfs.LegacyId:
					if t.StoragePool.LegacyId != v {
						continue
					}
				}
				// Otherwise type beegfs.InvalidEntityId (aka don't filter by pool).
			} else {
				// If there isn't a storage pool set on the target this should be a metadata target.
				// If we're filtering by storage pools, metadata targets should never be included.
				if _, ok := cfg.StoragePool.(beegfs.InvalidEntityId); !ok {
					continue
				}
				// cfg.StoragePool is type beegfs.InvalidEntityId (aka don't filter by pool).
			}
		} // Otherwise nil (aka don't filter by pool).

		node := t.Node.Alias.String()
		if viper.GetBool(config.DebugKey) {
			node = t.Node.String()

		}
		pool := "n/a"
		if t.StoragePool != nil {
			if viper.GetBool(config.DebugKey) {
				pool = t.StoragePool.String()
			} else {
				pool = t.StoragePool.Alias.String()
			}
		}
		lastContact := "unknown"
		if t.LastContactS != nil {
			lastContact = fmt.Sprintf("%ds ago", *t.LastContactS)
		}

		spaceTotal := "-"
		if t.TotalSpaceBytes != nil {
			if viper.GetBool(config.RawKey) {
				spaceTotal = fmt.Sprintf("%d", *t.TotalSpaceBytes)
			} else {
				spaceTotal = fmt.Sprintf("%sB", unitconv.FormatPrefix(float64(*t.TotalSpaceBytes), unitconv.IEC, 1))
			}
		}
		spaceUsed := "-"
		if t.FreeSpaceBytes != nil && t.TotalSpaceBytes != nil {
			if viper.GetBool(config.RawKey) {
				spaceUsed = fmt.Sprintf("%d", *t.TotalSpaceBytes-*t.FreeSpaceBytes)
			} else {
				spaceUsed = fmt.Sprintf("%sB", unitconv.FormatPrefix(float64(*t.TotalSpaceBytes)-float64(*t.FreeSpaceBytes), unitconv.IEC, 1))
			}
			spaceUsed += fmt.Sprintf(" (%.2f%%)", 100-(float64(*t.FreeSpaceBytes)/float64(*t.TotalSpaceBytes))*100)
		}
		spaceFree := "-"
		if t.FreeSpaceBytes != nil {
			if viper.GetBool(config.RawKey) {
				spaceFree = fmt.Sprintf("%d", *t.FreeSpaceBytes)
			} else {
				spaceFree = fmt.Sprintf("%sB", unitconv.FormatPrefix(float64(*t.FreeSpaceBytes), unitconv.IEC, 1))
			}
		}

		inodesTotal := "-"
		if t.TotalInodes != nil {
			if viper.GetBool(config.RawKey) {
				inodesTotal = fmt.Sprintf("%d", *t.TotalInodes)
			} else {
				inodesTotal = fmt.Sprintf("%s", unitconv.FormatPrefix(float64(*t.TotalInodes), unitconv.SI, 1))
			}
		}
		inodesUsed := "-"
		if t.FreeInodes != nil && t.TotalInodes != nil {
			if viper.GetBool(config.RawKey) {
				inodesUsed = fmt.Sprintf("%d", *t.TotalInodes-*t.FreeInodes)
			} else {
				inodesUsed = fmt.Sprintf("%s", unitconv.FormatPrefix(float64(*t.TotalInodes)-float64(*t.FreeInodes), unitconv.SI, 1))
			}
			inodesUsed += fmt.Sprintf(" (%.2f%%)", 100-(float64(*t.FreeInodes)/float64(*t.TotalInodes))*100)
		}
		inodesFree := "-"
		if t.FreeInodes != nil {
			if viper.GetBool(config.RawKey) {
				inodesFree = fmt.Sprintf("%d", *t.FreeInodes)
			} else {
				inodesFree = fmt.Sprintf("%s", unitconv.FormatPrefix(float64(*t.FreeInodes), unitconv.SI, 1))
			}
		}

		tbl.Row(
			t.Target.Uid,
			t.Target.LegacyId,
			t.Target.Alias,
			node,
			pool,
			t.ReachabilityState,
			lastContact,
			t.ConsistencyState,
			t.CapacityPool,
			spaceTotal,
			spaceUsed,
			spaceFree,
			inodesTotal,
			inodesUsed,
			inodesFree,
		)
	}
}
