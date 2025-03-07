package resync

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup/resync"
)

type startResync_config struct {
	buddyGroup   beegfs.EntityId
	restart      bool
	timestampSec int64
	timespan     time.Duration
}

func newStartResyncCmd() *cobra.Command {
	var err error
	cfg := startResync_config{buddyGroup: beegfs.InvalidEntityId{}, restart: false}

	cmd := &cobra.Command{
		Use:   "start <buddy-group>",
		Short: "Start a resync of a storage or metadata target from its buddy target.",
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
		"Override last buddy communication timestamp with a specific Unix timestamp (storage targets only).")
	cmd.Flags().DurationVar(&cfg.timespan, "timespan", -1*time.Second,
		"Resync entries modified in the given timespan, for example 24h for the last day or 1h for the last hour (storage targets only).")
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

	cmdfmt.Printf("Successfully sent resync request. Progress can be monitored with 'beegfs mirror resync stats'.\n")
	return nil
}
