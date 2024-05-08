package rst

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
)

func newCleanupCmd() *cobra.Command {

	cfg := rst.UpdateJobCfg{
		NewState: beeremote.UpdateJobRequest_DELETED,
	}
	cmd := &cobra.Command{
		Use:   "cleanup <path>",
		Short: "Cleanup (delete) jobs for a file path.",
		Long:  "Cleanup (delete) jobs for a file path. By default all incomplete jobs are deleted.",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.Path = args[0]
			return runCleanupCmd(cmd, cfg)
		},
	}
	cmd.Flags().StringVar(&cfg.JobID, "job-id", "", "If there are multiple jobs for this path, only cleanup the specified job.")
	cmd.Flags().BoolVar(&cfg.Force, "force", false, `
	By default completed jobs are skipped when cleaning up so they can be used to determine when a path was synchronized with an RST.
	Optionally completed jobs can be forcibly cleaned up (generally you will want to use the jobID flag to limit what jobs are deleted).
	This mode skips verifying the path stills exists to allow cleaning up jobs for deleted paths.
	IMPORTANT: Only relative paths inside BeeGFS can be used for forced updates. 
	For example given the absolute path "/mnt/beegfs/myfile" you would specify "/myfile".
	`)
	return cmd
}

func runCleanupCmd(cmd *cobra.Command, cfg rst.UpdateJobCfg) error {

	response, err := rst.UpdateJobs(cmd.Context(), cfg)
	if err != nil {
		return err
	}

	w := cmdfmt.NewTableWriter(os.Stdout)
	defer w.Flush()

	if !response.Ok {
		fmt.Fprintf(&w, "Unable to cleanup one or more jobs: %s\n", response.Message)
	} else {
		if !cfg.Force {
			fmt.Fprintf(&w, "Cleaned up all jobs except ones that were already completed:\n")
		} else {
			fmt.Fprintf(&w, "Cleaned up all jobs:\n")
		}
	}

	for _, job := range response.Results {
		fmt.Fprintf(&w, "\tJob: %s\n", job.GetJob())
		if config.Get().Debug {
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
