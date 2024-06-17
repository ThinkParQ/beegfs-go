package target

import (
	"fmt"
	"os"

	"github.com/dsnet/golib/unitconv"
	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/target"
	"github.com/thinkparq/gobee/beegfs"
)

type list_Config struct {
	NodeType    beegfs.NodeType
	Capacity    bool
	State       bool
	StoragePool beegfs.EntityId
}

func newListCmd() *cobra.Command {
	cfg := list_Config{StoragePool: beegfs.InvalidEntityId{}}

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List BeeGFS targets.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runListCmd(cmd, cfg)
		},
	}

	cmd.Flags().Var(beegfs.NewNodeTypePFlag(&cfg.NodeType, beegfs.Meta, beegfs.Storage), "node-type",
		"Filter by node type")
	cmd.Flags().Var(beegfs.NewEntityIdPFlag(&cfg.StoragePool, 16, beegfs.Storage), "pool", "Filter by storage pool.")
	cmd.Flags().BoolVar(&cfg.Capacity, "capacity", false, "Print capacity information.")
	cmd.Flags().BoolVar(&cfg.State, "state", false, "Print states as seen by the management service.")

	return cmd
}

func runListCmd(cmd *cobra.Command, cfg list_Config) error {
	targets, err := target.GetTargets(cmd.Context())
	if err != nil {
		return err
	}

	w := cmdfmt.NewTableWriter(os.Stdout)
	defer w.Flush()

	if config.Get().Debug {
		fmt.Fprint(&w, "UID\t")
	}
	fmt.Fprint(&w, "Alias\tID\tOn Node\tOn Storage Pool\tLast Contact\t")

	if cfg.State {
		fmt.Fprint(&w, "Reachability\tConsistency\t")
	}

	if cfg.Capacity {
		fmt.Fprint(&w, "Space\tInodes\tCapacity Pool\t")
	}

	fmt.Fprintln(&w)

	for _, t := range targets {
		if cfg.NodeType != beegfs.InvalidNodeType && t.NodeType != cfg.NodeType {
			continue
		}

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

		} else {
			if _, ok := cfg.StoragePool.(beegfs.InvalidEntityId); !ok {
				continue
			}
		}

		if config.Get().Debug {
			fmt.Fprintf(&w, "%d\t", beegfs.Uid(t.Target.Uid))
		}

		fmt.Fprintf(&w, "%s\t%s\t", t.Target.Alias, t.Target.LegacyId)

		if config.Get().Debug {
			fmt.Fprintf(&w, "%v\t", t.Node)
		} else {
			fmt.Fprintf(&w, "%s\t", t.Node.Alias)
		}

		if t.StoragePool != nil {
			if config.Get().Debug {
				fmt.Fprintf(&w, "%v\t", t.StoragePool)
			} else {
				fmt.Fprintf(&w, "%s\t", t.StoragePool.Alias)
			}
		} else {
			fmt.Fprintf(&w, "\t")
		}

		if t.LastContactS != nil {
			fmt.Fprintf(&w, "%d s ago\t", *t.LastContactS)
		} else {
			fmt.Fprint(&w, "-\t")
		}

		if cfg.State {
			fmt.Fprintf(&w, "%s\t%s\t", t.ReachabilityState, t.ConsistencyState)
		}

		if cfg.Capacity {
			if t.FreeSpaceBytes != nil {
				if config.Get().Raw {
					fmt.Fprintf(&w, "%d/", *t.FreeSpaceBytes)
				} else {
					fmt.Fprintf(&w, "%sB/", unitconv.FormatPrefix(float64(*t.FreeSpaceBytes), unitconv.IEC, 0))
				}
			} else {
				fmt.Fprint(&w, "-/")
			}

			if t.TotalSpaceBytes != nil {
				if config.Get().Raw {
					fmt.Fprintf(&w, "%d\t", *t.TotalSpaceBytes)
				} else {
					fmt.Fprintf(&w, "%sB\t", unitconv.FormatPrefix(float64(*t.TotalSpaceBytes), unitconv.IEC, 0))
				}
			} else {
				fmt.Fprint(&w, "-\t")
			}

			if t.FreeInodes != nil {
				if config.Get().Raw {
					fmt.Fprintf(&w, "%d/", *t.FreeInodes)
				} else {
					fmt.Fprintf(&w, "%s/", unitconv.FormatPrefix(float64(*t.FreeInodes), unitconv.SI, 0))
				}
			} else {
				fmt.Fprint(&w, "-/")
			}

			if t.TotalInodes != nil {
				if config.Get().Raw {
					fmt.Fprintf(&w, "%d\t", *t.TotalInodes)
				} else {
					fmt.Fprintf(&w, "%s\t", unitconv.FormatPrefix(float64(*t.TotalInodes), unitconv.SI, 0))
				}
			} else {
				fmt.Fprint(&w, "-\t")
			}

			fmt.Fprintf(&w, "%s\t", t.CapacityPool)
		}

		fmt.Fprintln(&w)
	}

	return nil
}
