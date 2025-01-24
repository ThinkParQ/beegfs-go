package rst

import (
	"context"
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
	"go.uber.org/zap"
)

func newCancelCmd() *cobra.Command {

	cfg := rst.UpdateJobCfg{
		NewState: beeremote.UpdateJobsRequest_CANCELLED,
	}
	cmd := &cobra.Command{
		Use:   "cancel <path>",
		Short: "Cancel job(s) for the specified path",
		Long: `Cancel job(s) for the specified path.
By default all incomplete jobs for the exact <path> specified are cancelled. Also specify the job ID to update a single job.
To update multiple paths at once use the recurse flag to update all paths prefixed by <path>.

Note: Because jobs can exist for paths that no longer exist in BeeGFS, recursive updates are based on the Remote database, not what files currently exist.`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfg.Recurse {
				if y, err := cmd.Flags().GetBool("yes"); err != nil {
					return fmt.Errorf("%w (this is probably a bug)", err)
				} else if !y {
					return fmt.Errorf("the recurse mode updates the specified entry and ALL child entries, if you're sure this is what you want add the --yes flag")
				}
			}
			cfg.Path = args[0]
			return updateJobRunner(cmd.Context(), &cfg)
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
	cmd.Flags().UintSliceVar(&cfg.RemoteTargets, "remote-targets", nil, "Only update jobs for the specified remote targets. By default jobs for all remote targets are updated.")
	cmd.Flags().BoolVar(&cfg.Recurse, "recurse", false, "Treat the provided path as a prefix and update all matching paths.")
	cmd.Flags().Bool("yes", false, "Use to acknowledge when running this command may update a large number of entries.")

	return cmd
}

func newCleanupCmd() *cobra.Command {

	cfg := rst.UpdateJobCfg{
		NewState: beeremote.UpdateJobsRequest_DELETED,
	}
	cmd := &cobra.Command{
		Use:   "cleanup <path>",
		Short: "Cleanup (delete) jobs for the specified path",
		Long: `Cleanup (delete) jobs for the specified path.
By default all incomplete jobs for the exact <path> specified are deleted. Also specify the job ID to update a single job.
To update multiple paths at once use the recurse flag to update all paths prefixed by <path>.

Note: Because jobs can exist for paths that no longer exist in BeeGFS, recursive updates are based on the Remote database, not what files currently exist.`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if cfg.Recurse {
				if y, err := cmd.Flags().GetBool("yes"); err != nil {
					return fmt.Errorf("%w (this is probably a bug)", err)
				} else if !y {
					return fmt.Errorf("the recurse mode updates the specified entry and ALL child entries, if you're sure this is what you want add the --yes flag")
				}
			}
			cfg.Path = args[0]
			return updateJobRunner(cmd.Context(), &cfg)
		},
	}
	cmd.Flags().StringVar(&cfg.JobID, "job-id", "", "If there are multiple jobs for this path, only cleanup the specified job.")
	cmd.Flags().BoolVar(&cfg.Force, "force", false, `By default completed jobs are skipped when cleaning up so they can be used to determine when a path was synchronized with an RST.
	Optionally completed jobs can be forcibly cleaned up (generally you will want to use the jobID flag to limit what jobs are deleted).`)
	cmd.Flags().UintSliceVar(&cfg.RemoteTargets, "remote-targets", nil, "Only update jobs for the specified remote targets. By default jobs for all remote targets are updated.")
	cmd.Flags().BoolVar(&cfg.Recurse, "recurse", false, "Treat the provided path as a prefix and update all matching paths.")
	cmd.Flags().Bool("yes", false, "Use to acknowledge when running this command may update a large number of entries.")
	return cmd
}

func updateJobRunner(ctx context.Context, cfg *rst.UpdateJobCfg) error {

	log, _ := config.GetLogger()
	responses := make(chan *rst.UpdateJobsResponse, 1024)
	err := rst.UpdateJobsByPaths(ctx, cfg, responses)
	if err != nil {
		if errors.Is(err, filesystem.ErrInitFSClient) {
			return fmt.Errorf("%w (hint: use the --%s=none flag to interact with files that no longer exist or if BeeGFS is not mounted)", err, config.BeeGFSMountPointKey)
		}
		return err
	}

	tbl := newJobsTable(withDefaultColumns([]string{"ok", "path", "target", "last update", "job id", "request type"}))
	// Set to false to control when the remaining table entries are printed instead of having them
	// print automatically when the function returns (i.e., if an error happens).
	autoPrintRemaining := true
	defer func() {
		if autoPrintRemaining {
			tbl.PrintRemaining()
		}
	}()

	allOk := true
	processedPaths := 0

writeResponses:
	for {
		select {
		case <-ctx.Done():
			break writeResponses
		case resp, ok := <-responses:
			if !ok {
				break writeResponses
			}
			if resp.Err != nil {
				return resp.Err
			}

			// Sort jobs by when they were last updated. This sorting is not strictly required and
			// could be changed or removed in the future as it is mainly to ensure the jobs we just
			// updated are sorted to the top under their respective paths.
			sort.Slice(resp.Result.Results, func(i, j int) bool {
				return resp.Result.Results[i].Job.Status.Updated.AsTime().After(resp.Result.Results[j].Job.Status.Updated.AsTime())
			})
			// Append the results to the table.
			for _, job := range resp.Result.Results {
				tbl.Row(job)
			}
			allOk = resp.Result.GetOk() && allOk
			processedPaths++
			log.Debug("finished updating jobs for path", zap.String("path", resp.Path), zap.Bool("ok", resp.Result.GetOk()), zap.Any("message", resp.Result.GetMessage()))
		}
	}
	autoPrintRemaining = false
	tbl.PrintRemaining()
	if !allOk {
		return util.NewCtlError(errors.New("unable to update one or more jobs (see individual results for details)"), util.PartialSuccess)
	}

	if processedPaths == 0 {
		cmdfmt.Printf("Warning: no jobs were found for the specified path\n")
	} else if !cfg.Force {
		cmdfmt.Printf("Success: updated all jobs except ones that were already completed for %d paths\n", processedPaths)
	} else {
		cmdfmt.Printf("Success: updated all jobs for %d paths\n", processedPaths)
	}
	return nil
}
