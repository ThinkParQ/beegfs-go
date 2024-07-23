package rst

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/rst"
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

	w := cmdfmt.NewDeprecatedTableWriter(os.Stdout)
	defer w.Flush()

	if !response.Ok {
		fmt.Fprintf(&w, "Unable to cancel one or more jobs: %s\n", response.Message)
	} else {
		if !cfg.Force {
			fmt.Fprintf(&w, "Cancelled all jobs except ones that were already completed:\n")
		} else {
			fmt.Fprintf(&w, "Cancelled all jobs:\n")
		}
	}

	for _, job := range response.Results {
		fmt.Fprintf(&w, "\tJob: %s\n", job.GetJob())
		if viper.GetBool(config.DebugKey) {
			fmt.Fprintf(&w, "\t\tWork Requests:\n")
			for _, wr := range job.GetWorkRequests() {
				fmt.Fprintf(&w, "\t\t\t%s\n", wr)
			}
			fmt.Fprintf(&w, "\t\tWork Responses:\n")
			for _, wr := range job.GetWorkResults() {
				fmt.Fprintf(&w, "\t\t\t%s\n", wr)
			}
		}
		fmt.Fprintf(&w, "\n")
	}
	return nil
}
