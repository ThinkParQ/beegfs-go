package target

import (
	"context"
	"fmt"

	"github.com/dsnet/golib/unitconv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup/resync"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"go.uber.org/zap"
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
		Use:         "list",
		Short:       "List BeeGFS targets",
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		RunE: func(cmd *cobra.Command, args []string) error {
			targets, err := target.GetTargets(cmd.Context())
			if err != nil {
				return err
			}
			PrintTargetList(cmd.Context(), cfg, targets)
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
func PrintTargetList(ctx context.Context, cfg PrintConfig, targets []target.GetTargets_Result) {

	logger, _ := config.GetLogger()

	allColumns := []string{"uid", "id", "alias", "on_node", "pool", "reachability", "last_contact", "consistency", "sync_state", "cap_pool", "space", "sused", "sfree", "inodes", "iused", "ifree"}
	defaultColumns := []string{"id", "alias", "on_node", "pool"}
	if viper.GetBool(config.DebugKey) {
		defaultColumns = allColumns
	} else {
		if cfg.State {
			defaultColumns = append(defaultColumns, "reachability", "last_contact", "consistency", "sync_state")
		}
		if cfg.Capacity {
			defaultColumns = append(defaultColumns, "cap_pool", "space", "sused", "sfree", "inodes", "iused", "ifree")
		}
	}

	tbl := cmdfmt.NewPrintomatic(allColumns, defaultColumns)
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
				inodesTotal = unitconv.FormatPrefix(float64(*t.TotalInodes), unitconv.SI, 1)
			}
		}
		inodesUsed := "-"
		if t.FreeInodes != nil && t.TotalInodes != nil {
			if viper.GetBool(config.RawKey) {
				inodesUsed = fmt.Sprintf("%d", *t.TotalInodes-*t.FreeInodes)
			} else {
				inodesUsed = unitconv.FormatPrefix(float64(*t.TotalInodes)-float64(*t.FreeInodes), unitconv.SI, 1)
			}
			inodesUsed += fmt.Sprintf(" (%.2f%%)", 100-(float64(*t.FreeInodes)/float64(*t.TotalInodes))*100)
		}
		inodesFree := "-"
		if t.FreeInodes != nil {
			if viper.GetBool(config.RawKey) {
				inodesFree = fmt.Sprintf("%d", *t.FreeInodes)
			} else {
				inodesFree = unitconv.FormatPrefix(float64(*t.FreeInodes), unitconv.SI, 1)
			}
		}

		// Include the actual resync state if the consistency state is not good. Otherwise the
		// consistency may be needs-resync while a resync is already underway which would be
		// confusing. Note we get the resync state from the frontend instead of the backend because
		// (a) it would currently cause a cyclical import if the backend target package tries to
		// call functionality in the backend resync package and (b) we don't need this information
		// in most circumstances but it be confusing/bug prone to add resync state as a field on
		// GetTargets_Result without always populating it. Better instead the frontend determine if
		// surfacing this detail to the user is important and get it only when necessary.
		syncState := "Healthy"
		if t.ConsistencyState != target.ConsistencyGood {
			syncState = "Unknown"
			mappings, err := util.GetMappings(ctx)
			if err != nil {
				logger.Debug("unable to determine resync job state because there was an error getting entity mappings", zap.Error(err))
			} else {
				// Resync stats should be determined from the node that owns the primary target.
				primary, err := mappings.MirroredTargetToPrimary.Get(t.Target.Uid)
				if err != nil {
					logger.Debug("unable to map secondary target to primary target", zap.Any("target", t.Target), zap.Error(err))
				} else {
					if t.NodeType == beegfs.Meta {
						if resp, err := resync.GetMetaResyncStats(ctx, primary); err != nil {
							logger.Debug("error getting resync job state", zap.Error(err))
						} else {
							// Ensure the resync stats are from an active/current resync, not
							// leftover from some historical resync. The end time will be zero if a
							// resync is active. Note if someone were to be actively watching this
							// command there may be a brief period where either (1) the consistency
							// is needs-resync but the sync state is success (observed), or (2)
							// where the sync state is unknown even though the resync was successful
							// (in theory). But in all cases we should always only display the sync
							// state for the current sync.
							if resp.EndTime == 0 {
								syncState = resp.State.String()
							} else {
								syncState = "Not-started"
							}
						}
					} else if t.NodeType == beegfs.Storage {
						if resp, err := resync.GetStorageResyncStats(ctx, primary); err != nil {
							logger.Debug("error getting resync job state", zap.Error(err))
						} else {
							if resp.EndTime == 0 {
								syncState = resp.State.String()
							} else {
								syncState = "Not-started"
							}
						}
					}

				}
			}
		}

		tbl.AddItem(
			t.Target.Uid,
			t.Target.LegacyId,
			t.Target.Alias,
			node,
			pool,
			t.ReachabilityState,
			lastContact,
			t.ConsistencyState,
			syncState,
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
