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

func newCleanupCmd() *cobra.Command {

	cfg := rst.UpdateJobCfg{
		NewState: beeremote.UpdateJobRequest_DELETED,
	}
	cmd := &cobra.Command{
		Use:   "cleanup <path>",
		Short: "Cleanup (delete) jobs for a file path",
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
	cmd.Flags().BoolVar(&cfg.Force, "force", false, `By default completed jobs are skipped when cleaning up so they can be used to determine when a path was synchronized with an RST.
	Optionally completed jobs can be forcibly cleaned up (generally you will want to use the jobID flag to limit what jobs are deleted).`)
	return cmd
}

func runCleanupCmd(cmd *cobra.Command, cfg rst.UpdateJobCfg) error {

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
		return util.NewCtlError(fmt.Errorf("unable to cleanup one or more jobs: %s", response.Message), util.PartialSuccess)
	}
	if !cfg.Force {
		cmdfmt.Printf("Success: cleaned up all jobs except ones that were already completed\n")
	} else {
		cmdfmt.Printf("Success: cleaned up all jobs\n")
	}
	return nil
}
