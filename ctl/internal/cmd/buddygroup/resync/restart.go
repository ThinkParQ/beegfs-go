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
		Short: "Restarts a resync of a storage target from its buddy. (Restart resync for meta targets are not supported).",
		Long:  "Restarts a resync of a storage target from its buddy. Meta targets resync cannot be aborted or restarted.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.buddyGroup, err = beegfs.NewEntityIdParser(16, beegfs.Storage).Parse(args[0])
			if err != nil {
				return err
			}

			return runStartResyncCmd(cmd, &cfg)
		},
	}

	cmd.Flags().Int64Var(&cfg.timestampSec, "timestamp", -1,
		"Timestamp in UNIX epoch format, representing the time from which to resync until now.")
	cmd.Flags().DurationVar(&cfg.timespan, "timespan", -1*time.Second,
		"Resync all entries modified from the specified time until now.")

	cmd.MarkFlagsMutuallyExclusive("timestamp", "timespan")
	cmd.MarkFlagsOneRequired("timestamp", "timespan")

	return cmd
}
