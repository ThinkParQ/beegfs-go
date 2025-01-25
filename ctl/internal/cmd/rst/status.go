package rst

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
)

type statusConfig struct {
	history int
	retro   bool
	verbose bool
	width   int
}

func newStatusCmd() *cobra.Command {

	frontendCfg := statusConfig{}
	backendCfg := rst.GetJobsConfig{}
	cmd := &cobra.Command{
		Use:   "status <path>",
		Short: "Get the status of jobs for a file or directory path",
		Long:  "Get the status of jobs for a file or directory path.\nJobs for each path are grouped together and sorted by remote target then by when they were created.",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			backendCfg.Path = args[0]
			return runGetStatusCmd(cmd, frontendCfg, backendCfg)
		},
	}

	cmd.Flags().StringVar(&backendCfg.JobID, "job-id", "", "If a file path is specified, only return results for this specific job.")
	cmd.Flags().IntVar(&frontendCfg.history, "history", 1, "Limit the number of jobs returned for each path+RST combination (defaults to only the most recently created job).")
	cmd.Flags().BoolVar(&frontendCfg.retro, "retro", false, "Don't print output in a table and return all possible fields grouping jobs for each path by RST and sorting by when they were created.")
	cmd.Flags().BoolVar(&frontendCfg.verbose, "verbose", false, "Print additional details about each job (use --debug) to also print work requests and results.")
	cmd.Flags().IntVar(&frontendCfg.width, "width", 30, "Set the maximum width of some columns before they overflow.")
	return cmd
}

func runGetStatusCmd(cmd *cobra.Command, frontendCfg statusConfig, backendCfg rst.GetJobsConfig) error {

	responses := make(chan *rst.GetJobsResponse, 1024)
	err := rst.GetJobs(cmd.Context(), backendCfg, responses)
	if err != nil {
		if errors.Is(err, filesystem.ErrInitFSClient) {
			return fmt.Errorf("%w (hint: use the --%s=none flag to interact with files that no longer exist or if BeeGFS is not mounted)", err, config.BeeGFSMountPointKey)
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
