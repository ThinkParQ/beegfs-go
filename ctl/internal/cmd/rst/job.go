package rst

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
	"go.uber.org/zap"
)

func newJobCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "job",
		Short: "Interact with existing job(s)",
	}
	cmd.AddCommand(newCancelCmd(), newCleanupCmd(), newListJobsCmd())
	return cmd
}

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
		cmdfmt.Printf("no jobs were found for the specified path\n")
	} else if !cfg.Force {
		cmdfmt.Printf("Success: updated all jobs except ones that were already completed for %d paths\n", processedPaths)
	} else {
		cmdfmt.Printf("Success: updated all jobs for %d paths\n", processedPaths)
	}
	return nil
}

type listJobsConfig struct {
	history int
	retro   bool
	verbose bool
	width   int
}

func newListJobsCmd() *cobra.Command {

	frontendCfg := listJobsConfig{}
	backendCfg := rst.GetJobsConfig{}
	cmd := &cobra.Command{
		Use:   "list <path-prefix>",
		Short: "List active and historical jobs by file or directory path prefix",
		Long: fmt.Sprintf(`List active and historical jobs by file or directory path prefix.
Whether or not a job is consider "ok" is based on the job state and does not reflect if the path is currently in sync (use the status command).
All jobs in the database for paths that match the specified prefix will be returned regardless if those paths still exist in BeeGFS.
If the path prefix does not exist in BeeGFS (for example if the file was deleted) specify --%s=%s and the path relative to the BeeGFS root.
Jobs for each path are grouped together and sorted by remote target then by when they were created.`, config.BeeGFSMountPointKey, config.BeeGFSMountPointNone),
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			backendCfg.Path = args[0]
			return runListJobsCmd(cmd, frontendCfg, backendCfg)
		},
	}

	cmd.Flags().StringVar(&backendCfg.JobID, "job-id", "", "If a file path is specified, only return results for this specific job.")
	cmd.Flags().IntVar(&frontendCfg.history, "history", 1, "Limit the number of jobs returned for each path+RST combination (defaults to only the most recently created job).")
	cmd.Flags().BoolVar(&frontendCfg.retro, "retro", false, "Don't print output in a table and return all possible fields grouping jobs for each path by RST and sorting by when they were created.")
	cmd.Flags().BoolVar(&frontendCfg.verbose, "verbose", false, "Print additional details about each job (use --debug) to also print work requests and results.")
	cmd.Flags().IntVar(&frontendCfg.width, "column-width", 30, "Set the maximum width of some columns before they overflow.")
	return cmd
}

func runListJobsCmd(cmd *cobra.Command, frontendCfg listJobsConfig, backendCfg rst.GetJobsConfig) error {

	responses := make(chan *rst.GetJobsResponse, 1024)
	err := rst.GetJobs(cmd.Context(), backendCfg, responses)
	if err != nil {
		if errors.Is(err, filesystem.ErrInitFSClient) {
			return fmt.Errorf("%w (hint: use the --%s=%s flag to interact with files that no longer exist or if BeeGFS is not mounted)", err, config.BeeGFSMountPointKey, config.BeeGFSMountPointNone)
		}
		return err
	}

	withDebug := viper.GetBool(config.DebugKey)
	tbl := newJobsTable(withJobDetails(frontendCfg.verbose), withColumnWidth(frontendCfg.width))

	// Set to false to control when the remaining table entries are printed instead of having them
	// print automatically when the function returns (i.e., if an error happens).
	autoPrintRemaining := true
	defer func() {
		if autoPrintRemaining {
			tbl.PrintRemaining()
		}
	}()

	totalPaths := 0

writeResponses:
	for {
		select {
		case <-cmd.Context().Done():
			break writeResponses
		case resp, ok := <-responses:
			if !ok {
				break writeResponses
			}

			if resp.Err != nil {
				return err
			}

			// Sort jobs by when they were created so the most recently created job always determines
			// the path's status for that RST when printing horizontally:
			sort.Slice(resp.Results, func(i, j int) bool {
				return resp.Results[i].Job.Created.GetSeconds() > resp.Results[j].Job.Created.GetSeconds()
			})

			// Print jobs by RST ID:
			rstsForPath := []uint32{}
			jobsPerRST := map[uint32][]*beeremote.JobResult{}
			for _, job := range resp.Results {
				if jobsPerRST[job.Job.Request.RemoteStorageTarget] == nil {
					jobsPerRST[job.Job.Request.RemoteStorageTarget] = make([]*beeremote.JobResult, 0, 1)
					rstsForPath = append(rstsForPath, job.Job.Request.RemoteStorageTarget)
				}
				jobsPerRST[job.Job.Request.RemoteStorageTarget] = append(jobsPerRST[job.Job.Request.RemoteStorageTarget], job)
			}
			// Sort by RST ID:
			sort.Slice(rstsForPath, func(i, j int) bool {
				return rstsForPath[i] < rstsForPath[j]
			})

			totalPaths++

			// Print output using a table:
			if !frontendCfg.retro {
				for _, rst := range rstsForPath {
					for i, job := range jobsPerRST[rst] {
						if i >= frontendCfg.history {
							break
						}
						tbl.Row(job)
					}
				}
				continue
			}

			// Otherwise print in a more information dense horizontal format:
			strBuilder := strings.Builder{}
			if len(resp.Results) == 0 {
				fmt.Fprintf(&strBuilder, "Path: %s - No jobs found for path.\n", resp.Path)
				continue
			} else {
				fmt.Fprintf(&strBuilder, "Path: %s\n", resp.Path)
			}

			for _, id := range rstsForPath {
				fmt.Fprintf(&strBuilder, "%s Remote Storage Target: %d\n", convertJobStateToEmoji(jobsPerRST[id][0].Job.Status.State), id)
				for i, job := range jobsPerRST[id] {
					if i >= frontendCfg.history {
						break
					}
					fmt.Fprintf(&strBuilder, "  %s Job:", convertJobStateToEmoji(job.Job.Status.State))
					fmt.Fprintf(&strBuilder, " id: %s, state: %s, ", job.Job.Id, job.Job.Status.State)
					// Optimize printing jobs we know about (for example no need to print path again):
					switch job.Job.Request.Type.(type) {
					case *beeremote.JobRequest_Sync:
						fmt.Fprintf(&strBuilder, "request: {remote_storage_target: %d, type: sync, %s}", job.Job.Request.RemoteStorageTarget, job.Job.Request.GetSync())
					default:
						fmt.Fprintf(&strBuilder, "request: {%s}", job.Job.GetRequest())
					}

					if withDebug {
						fmt.Fprintf(&strBuilder, ", message: %s", job.Job.Status.Message)
						if job.Job.ExternalId != "" {
							fmt.Fprintf(&strBuilder, ", external_id: %s", job.Job.ExternalId)
						} else {
							fmt.Fprintf(&strBuilder, ", external_id: none")
						}
					}

					fmt.Fprintf(&strBuilder, "\n")

					if withDebug {
						if len(job.WorkRequests) == len(job.WorkResults) {
							sort.Slice(job.WorkRequests, func(i, j int) bool {
								return job.WorkRequests[i].RequestId <= job.WorkRequests[j].RequestId
							})
							sort.Slice(job.WorkResults, func(i, j int) bool {
								return job.WorkResults[i].Work.RequestId <= job.WorkResults[j].Work.RequestId
							})
							for i, wr := range job.GetWorkResults() {
								fmt.Fprintf(&strBuilder, "    %s Work ID: %s\n", convertWorkStateToEmoji(wr.Work.Status.State), wr.Work.RequestId)
								fmt.Fprintf(&strBuilder, "         Request: %s\n", job.WorkRequests[i])
								fmt.Fprintf(&strBuilder, "         Result: %s\n", wr)
								if job.WorkRequests[i].RequestId != wr.Work.RequestId {
									fmt.Fprintf(&strBuilder, "WARNING: There are an equal number of requests/results but the request and result IDs don't all line up (likely this is a bug in BeeRemote).")
								}
							}
						} else {
							// This generally shouldn't happen unless there is a bug.
							fmt.Fprintf(&strBuilder, "WARNING: The number of work requests and results does not match, printing individual results (likely this is a bug)).")
							fmt.Fprintf(&strBuilder, "      Work Requests:\n")
							for _, wr := range job.GetWorkRequests() {
								fmt.Fprintf(&strBuilder, "           %s\n", wr)
							}
							fmt.Fprintf(&strBuilder, "      Work Results:\n")
							for _, wr := range job.GetWorkResults() {
								fmt.Fprintf(&strBuilder, "        %s %s\n", convertWorkStateToEmoji(wr.Work.Status.State), wr)
							}
						}
					}
					fmt.Fprintf(&strBuilder, "     (created: %s | last updated: %s)\n", job.Job.Created.AsTime(), job.Job.Status.Updated.AsTime())
					fmt.Fprintf(&strBuilder, "\n")
				}
			}
			fmt.Print(strBuilder.String())
		}
	}

	autoPrintRemaining = false
	tbl.PrintRemaining()
	cmdfmt.Printf("Success: total paths found: %d\n", totalPaths)
	return nil
}
