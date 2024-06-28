package pool

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/pool"
)

func newListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List storage pools.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runListCmd(cmd)
		},
	}

	return cmd
}

func runListCmd(cmd *cobra.Command) error {
	pools, err := pool.GetStoragePools(cmd.Context())
	if err != nil {
		return err
	}

	w := cmdfmt.NewTableWriter(os.Stdout)
	defer w.Flush()

	if viper.GetBool(config.DebugKey) {
		fmt.Fprint(&w, "UID\t")
	}
	fmt.Fprint(&w, "Alias\tID\tMembers\t")

	fmt.Fprintln(&w)

	for _, p := range pools {
		if viper.GetBool(config.DebugKey) {
			fmt.Fprintf(&w, "%d\t", p.Pool.Uid)
		}

		fmt.Fprintf(&w, "%s\t%s\t", p.Pool.Alias, p.Pool.LegacyId)

		first := true
		for _, t := range p.Targets {
			if !first {
				fmt.Fprint(&w, "\n\t\t")
				if viper.GetBool(config.DebugKey) {
					fmt.Fprint(&w, "\t")
				}
			}
			first = false

			if viper.GetBool(config.DebugKey) {
				fmt.Fprintf(&w, "%v\t", t)
			} else {
				fmt.Fprintf(&w, "%s\t", t.Alias.String())
			}
		}

		for _, t := range p.BuddyGroups {
			if !first {
				fmt.Fprint(&w, "\n\t\t")
				if viper.GetBool(config.DebugKey) {
					fmt.Fprint(&w, "\t")
				}
			}
			first = false

			if viper.GetBool(config.DebugKey) {
				fmt.Fprintf(&w, "%v\t", t)
			} else {
				fmt.Fprintf(&w, "%s\t", t.Alias.String())
			}
		}

		fmt.Fprintln(&w)
	}

	return nil
}
