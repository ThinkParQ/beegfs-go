package rst

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newCancelCmd() *cobra.Command {

	cfg := rst.UpdateJobCfg{
		NewState: beeremote.UpdateJobRequest_CANCELLED,
	}
	cmd := &cobra.Command{
		Use:   "cancel <path>",
		Short: "Cancel job(s) for a file path.",
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
	cmd.Flags().BoolVar(&cfg.Force, "force", false, `
	Use force to attempt to cancel a job and all work requests regardless of their current state.
	This mode skips verifying the path stills exists to allow cancelling up jobs for deleted paths.
	This attempts to be as thorough as possible and will ignore errors it encounters along the way to complete the following:
	(a) Always verifies worker nodes are no longer running work requests even if they were already cancelled.
	(b) Aborts outstanding external transfers (such as multipart uploads), even if some work requests could not be definitely cancelled.
	(c) Updates the job state to cancelled so it can be cleaned up (deleted).
	The force flag can also be used to cancel completed jobs, which are normally ignored by job updates.
	Force cancelling already completed jobs should generally never be necessary during normal operation.
	IMPORTANT: Only relative paths inside BeeGFS can be used for forced updates. 
	For example given the absolute path "/mnt/beegfs/myfile" you would specify "/myfile".
	`)

	return cmd
}

func runCancelCmd(cmd *cobra.Command, cfg rst.UpdateJobCfg) error {

	response, err := rst.UpdateJobs(cmd.Context(), cfg)
	if err != nil {
		if sts, ok := status.FromError(err); ok {
			if sts.Code() == codes.NotFound && cfg.Force {
				return fmt.Errorf("%w\n(hint: forced job updates can only be made using relative paths)", err)
			}
		}
		return err
	}

	if !response.Ok {
		fmt.Printf("Unable to cancel one or more jobs: %s\n\n", response.Message)
	} else {
		if !cfg.Force {
			fmt.Printf("Cancelled all jobs except ones that were already completed:\n\n")
		} else {
			fmt.Printf("Cancelled all jobs:\n\n")
		}
	}

	tbl := newJobsTable(withDefaultColumns([]string{"ok", "path", "target", "last update", "job id", "request type"}))
	defer tbl.PrintRemaining()

	sort.Slice(response.Results, func(i, j int) bool {
		return response.Results[i].Job.Status.Updated.AsTime().After(response.Results[j].Job.Status.Updated.AsTime())
	})

	for _, job := range response.Results {
		tbl.Row(job)
	}
	return nil
}
