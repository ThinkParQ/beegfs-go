package rst

import (
	"errors"
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
)

func newCancelCmd() *cobra.Command {

	cfg := rst.UpdateJobCfg{
		NewState: beeremote.UpdateJobRequest_CANCELLED,
	}
	cmd := &cobra.Command{
		Use:   "cancel <path>",
		Short: "Cancel job(s) for a file path",
		Long:  "Cancel job(s) for a file path. By default all incomplete jobs are cancelled.",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.Path = args[0]
			return runCancelCmd(cmd, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.JobID, "job-id", "", "If there are multiple jobs for this path, only update the specified job.")
	cmd.Flags().BoolVar(&cfg.Force, "force", false, `Use force to attempt to cancel a job and all work requests regardless of their current state.
	This attempts to be as thorough as possible and will ignore errors it encounters along the way to complete the following:
	(a) Always verifies worker nodes are no longer running work requests even if they were already cancelled.
	(b) Aborts outstanding external transfers (such as multipart uploads), even if some work requests could not be definitely cancelled.
	(c) Updates the job state to cancelled so it can be cleaned up (deleted).
	The force flag can also be used to cancel completed jobs, which are normally ignored by job updates.
	Force cancelling already completed jobs should generally never be necessary during normal operation.`)

	return cmd
}

func runCancelCmd(cmd *cobra.Command, cfg rst.UpdateJobCfg) error {

	response, err := rst.UpdateJobs(cmd.Context(), cfg)
	if err != nil {
		if errors.Is(err, filesystem.ErrInitFSClient) {
			return fmt.Errorf("%w (hint: use the --%s=none flag to interact with files that no longer exist or if BeeGFS is not mounted)", err, config.BeeGFSMountPointKey)
		}
		return err
	}

	tbl := newJobsTable(withDefaultColumns([]string{"ok", "path", "target", "last update", "job id", "request type"}))
	sort.Slice(response.Results, func(i, j int) bool {
		return response.Results[i].Job.Status.Updated.AsTime().After(response.Results[j].Job.Status.Updated.AsTime())
	})

	for _, job := range response.Results {
		tbl.Row(job)
	}
	tbl.PrintRemaining()

	if !response.Ok {
		return util.NewCtlError(fmt.Errorf("unable to cancel one or more jobs: %s", response.Message), util.PartialSuccess)
	}
	if !cfg.Force {
		cmdfmt.Printf("Success: cancelled all jobs except ones that were already completed\n")
	} else {
		cmdfmt.Printf("Success: cancelled all jobs\n")
	}
	return nil
}
