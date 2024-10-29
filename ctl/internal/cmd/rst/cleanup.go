package rst

import (
	"fmt"
	"sort"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/v8/ctl/pkg/ctl/rst"
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

	if !response.Ok {
		fmt.Printf("Unable to cleanup one or more jobs: %s\n\n", response.Message)
	} else {
		if !cfg.Force {
			fmt.Printf("Cleaned up all jobs except ones that were already completed:\n\n")
		} else {
			fmt.Printf("Cleaned up all jobs:\n\n")
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
