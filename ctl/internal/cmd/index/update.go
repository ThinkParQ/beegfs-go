package index

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	indexPkg "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/index"
	"github.com/thinkparq/protobuf/go/beeindex"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func newUpdateCmd(globalCfg *indexPkg.GlobalCfg, parentFlags *pflag.FlagSet) *cobra.Command {
	var (
		every      time.Duration
		showStatus bool
		repair     bool
		since      time.Duration
		noMetadata bool
	)
	cmd := &cobra.Command{
		Use:   "update",
		Short: "Apply the changes beegfs-index-updater collected to the index",
		Long: `Apply the changes beegfs-index-updater collected to the index.

beegfs-index-updater receives file system events from BeeGFS Watch and keeps
the list of directories whose index entries changed. An update writes that
list to a suspect file and applies it to the index with
gufi_incremental_update. Only then are the events acknowledged to Watch. This
command talks to the service at --updater-addr, or the "updater" entry of
.beegfs.index.

Without flags it runs an update now and prints its summary. If an update is
already running, the request is served by the run that follows it. The exit
status is non-zero when the run failed.

A repair rebuilds directories judged from file times rather than events. When
Watch lost events, the service schedules a full repair on its own, which
rebuilds every directory. A full repair covers every change made before it
started: it consumes the list and serves an update queued before it.
--repair runs a full repair now. --since rebuilds only the directories with an
entry changed since the given time ago, unless a full repair is pending: then
the run is a full repair.

The interval set with --every lasts until the service restarts. The service's
configuration file sets the interval it starts with. The service also starts
an update on its own when Watch holds many unacknowledged events.

Example: run an update now

  beegfs index update

Example: run an update every ten minutes

  beegfs index update --every 10m

Example: repair everything changed in the last two hours

  beegfs index update --since 2h

Example: show what each stage is doing, the backlog, the schedule and the last run

  beegfs index update --status
`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := resolveGlobalCfg(cmd.Context(), *globalCfg, primeFSPath(args))
			client, err := indexPkg.UpdaterClient(cfg)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			switch {
			case showStatus:
				return printStatus(ctx, client)
			case cmd.Flags().Changed("every"):
				req := &beeindex.SetScheduleRequest{Every: durationpb.New(every)}
				resp, err := client.SetSchedule(ctx, req)
				if err != nil {
					return err
				}
				printSchedule(resp.GetSchedule(), "")
				return nil
			default:
				req := &beeindex.RunUpdateRequest{}
				if repair || cmd.Flags().Changed("since") {
					req.Mode = beeindex.RunUpdateRequest_REPAIR
				}
				if cmd.Flags().Changed("since") {
					if since <= 0 {
						return fmt.Errorf("--since must be a positive duration, got %s", since)
					}
					req.Since = timestamppb.New(time.Now().Add(-since))
				}
				req.NoBeegfsMetadata = noMetadata
				return runUpdate(ctx, client, req)
			}
		},
	}

	cmd.Flags().AddFlagSet(parentFlags)
	cmd.Flags().DurationVar(&every, "every", 0, "Run an update at least this often, until the service restarts. 0 disables the timer, otherwise at least 1m.")
	cmd.Flags().BoolVar(&showStatus, "status", false, "Show the service status instead of running an update.")
	cmd.Flags().BoolVar(&repair, "repair", false, "Run a full repair now: rebuild every directory.")
	cmd.Flags().DurationVar(&since, "since", 0, "Run a repair of the directories with an entry changed this long ago or later. A pending full repair makes it full.")
	cmd.Flags().BoolVarP(&noMetadata, "no-beegfs-metadata", "B", false, "Skip the BeeGFS plugin for this run (rebuilt directories get no BeeGFS metadata).")
	// One invocation shows the status, sets the schedule or runs an update, never two of these.
	schedule, run := []string{"every"}, []string{"repair", "since", "no-beegfs-metadata"}
	for _, f := range slices.Concat(schedule, run) {
		cmd.MarkFlagsMutuallyExclusive("status", f)
	}
	for _, s := range schedule {
		for _, r := range run {
			cmd.MarkFlagsMutuallyExclusive(s, r)
		}
	}
	cmd.MarkFlagsMutuallyExclusive("repair", "since")
	return cmd
}

func runUpdate(ctx context.Context, client beeindex.IndexUpdaterClient, req *beeindex.RunUpdateRequest) error {
	resp, err := client.RunUpdate(ctx, req)
	if err != nil {
		return err
	}
	summary := resp.GetSummary()
	printSummary(summary, "")
	if summary.GetError() != "" {
		return errors.New(summary.GetError())
	}
	return nil
}

func printSummary(s *beeindex.RunSummary, prefix string) {
	fmt.Printf("%smode: %s\n", prefix, strings.ToLower(s.GetMode().String()))
	fmt.Printf("%sstarted: %s\n", prefix, local(s.GetStarted()))
	fmt.Printf("%stook: %s\n", prefix, s.GetFinished().AsTime().Sub(s.GetStarted().AsTime()).Round(time.Millisecond))
	if s.GetSince() != nil {
		fmt.Printf("%ssince: %s\n", prefix, local(s.GetSince()))
	} else if s.GetMode() == beeindex.RunUpdateRequest_REPAIR {
		fmt.Printf("%ssince: the beginning (full rebuild)\n", prefix)
	}
	fmt.Printf("%sbatches: %d, directories: %d, error lines: %d, exit code: %d\n",
		prefix, s.GetBatches(), s.GetSuspectDirs(), s.GetErrorLines(), s.GetExitCode())
	if s.GetError() != "" {
		fmt.Printf("%serror: %s\n", prefix, s.GetError())
	}
}

func printSchedule(s *beeindex.Schedule, prefix string) {
	fmt.Printf("%srun every: %s\n", prefix, s.GetEvery().AsDuration())
}

func local(t *timestamppb.Timestamp) string {
	return t.AsTime().Local().Format(time.RFC3339)
}

func printStatus(ctx context.Context, client beeindex.IndexUpdaterClient) error {
	s, err := client.GetStatus(ctx, &beeindex.GetStatusRequest{})
	if err != nil {
		return err
	}
	printCollector(s.GetCollector())
	printMarkwalk(s.GetMarkwalk())
	printUpdate(s.GetUpdate())
	return nil
}

func printCollector(c *beeindex.CollectorStatus) {
	state := "idle, no event since the last batch closed"
	switch c.GetState() {
	case beeindex.CollectorStatus_GATHERING:
		state = "gathering events into the open batch"
	case beeindex.CollectorStatus_BLOCKED:
		state = "blocked, a closed batch waits for markwalk, so nothing is read from Watch"
	}
	fmt.Printf("collector: %s\n", state)
	fmt.Printf("  queued events: %s, open batch: %s, batches waiting for markwalk: %s, acknowledgements waiting: %s\n",
		queue(c.GetEvents()), queue(c.GetOpenBatch()), queue(c.GetBatches()), queue(c.GetAcks()))
	fmt.Printf("  since start: events %d, batches closed %d, acknowledged %d\n",
		c.GetEventsReceived(), c.GetBatchesClosed(), c.GetBatchesAcknowledged())
	for _, m := range c.GetMetas() {
		fmt.Printf("  metadata node %d: received %d, acknowledged %d, waiting for an update %d", m.GetMeta(),
			m.GetReceivedSeq(), m.GetAcknowledgedSeq(), m.GetReceivedSeq()-m.GetAcknowledgedSeq())
		if m.GetMissingEvents() > 0 {
			fmt.Printf(", lost %d events", m.GetMissingEvents())
		}
		fmt.Println()
	}
}

func queue(q *beeindex.Queue) string {
	return fmt.Sprintf("%d of %d", q.GetSize(), q.GetCapacity())
}

func printMarkwalk(m *beeindex.MarkwalkStatus) {
	switch m.GetState() {
	case beeindex.MarkwalkStatus_PROCESSING:
		fmt.Printf("markwalk: processing batch %d (%d events) since %s: %s\n",
			m.GetBatch(), m.GetEvents(), local(m.GetSince()), m.GetStep())
	case beeindex.MarkwalkStatus_FAILING:
		fmt.Printf("markwalk: batch %d (%d events) failing since %s: %s\n",
			m.GetBatch(), m.GetEvents(), local(m.GetSince()), m.GetError())
	default:
		fmt.Println("markwalk: waiting for a batch")
	}
	fmt.Printf("  since start: batches done %d, directories named %d\n", m.GetBatchesDone(), m.GetSuspectDirs())
}

func printUpdate(u *beeindex.UpdateStatus) {
	if r := u.GetRunning(); r != nil {
		line := fmt.Sprintf("update: running %s since %s", strings.ToLower(r.GetMode().String()), local(r.GetStarted()))
		if r.GetSince() != nil {
			line += ", rebuilding what changed since " + local(r.GetSince())
		} else if r.GetMode() == beeindex.RunUpdateRequest_REPAIR {
			line += ", rebuilding every directory"
		}
		fmt.Println(line)
	} else {
		fmt.Println("update: idle")
	}
	printSchedule(u.GetSchedule(), "  ")
	if u.GetNextRun() != nil {
		fmt.Printf("  next run: %s\n", local(u.GetNextRun()))
	}
	if u.GetRepairPending() {
		fmt.Println("  full repair pending: Watch lost events")
	}
	if last := u.GetLastRun(); last != nil {
		fmt.Println("  last run:")
		printSummary(last, "    ")
	}
	fmt.Printf("  since start: runs %d (repairs %d, failed %d), batches applied %d, directories named %d, time in runs %s\n",
		u.GetRuns(), u.GetRepairs(), u.GetFailedRuns(), u.GetBatchesApplied(), u.GetSuspectDirsApplied(),
		u.GetRunTime().AsDuration().Round(time.Millisecond))
}
