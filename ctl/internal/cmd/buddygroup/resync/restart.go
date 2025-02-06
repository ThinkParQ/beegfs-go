package resync

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
)

func newRestartCmd() *cobra.Command {
	var err error
	cfg := startResync_config{buddyGroup: beegfs.InvalidEntityId{}, restart: true}

	cmd := &cobra.Command{
		Use:   "restart <buddy-group>",
		Short: "Abort any active resyncs for a storage mirror and start a new one.",
		Long: `Abort any active resyncs for a storage mirror and start a new one.
This mode is not supported with metadata targets because metadata targets must always be completely resynchronized and cannot be restarted based on a timestamp or timespan.`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.buddyGroup, err = beegfs.NewEntityIdParser(16, beegfs.Storage).Parse(args[0])
			if err != nil {
				return err
			}

			return runStartResyncCmd(cmd, &cfg)
		},
	}

	cmd.Flags().Int64Var(&cfg.timestampSec, "timestamp", -1,
		"Override last buddy communication timestamp with a specific Unix timestamp (storage targets only).")
	cmd.Flags().DurationVar(&cfg.timespan, "timespan", -1*time.Second,
		"Resync entries modified in the given timespan, for example 24h for the last day or 1h for the last hour (storage targets only).")

	cmd.MarkFlagsMutuallyExclusive("timestamp", "timespan")
	cmd.MarkFlagsOneRequired("timestamp", "timespan")

	return cmd
}
