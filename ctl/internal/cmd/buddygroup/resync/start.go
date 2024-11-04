package resync

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup/resync"
)

type startResync_config struct {
	buddyGroup   beegfs.EntityId
	restart      bool
	timestampSec int64
	timespan     time.Duration
}

func newStartCmd() *cobra.Command {
	var err error
	cfg := startResync_config{buddyGroup: beegfs.InvalidEntityId{}, restart: false}

	cmd := &cobra.Command{
		Use:   "start <buddy-group>",
		Short: "Starts a resync of a storage or metadata target from its buddy.",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.buddyGroup, err = beegfs.NewEntityIdParser(16, beegfs.Meta, beegfs.Storage).Parse(args[0])
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

	return cmd
}

func runStartResyncCmd(cmd *cobra.Command, cfg *startResync_config) error {

	if cfg.timespan >= 0 {
		cfg.timestampSec = time.Now().Add(-cfg.timespan).Unix()
	}

	err := backend.StartResync(cmd.Context(), cfg.buddyGroup, cfg.timestampSec, cfg.restart)
	if err != nil {
		return err
	}

	return nil
}
