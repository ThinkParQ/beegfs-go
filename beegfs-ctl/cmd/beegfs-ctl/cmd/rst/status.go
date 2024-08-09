package rst

import (
	"fmt"
	"os"
	"sort"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

func newStatusCmd() *cobra.Command {

	cfg := rst.GetJobsConfig{}
	cmd := &cobra.Command{
		Use:   "status <path>",
		Short: "Get the status of jobs running for a file or directory path.",
		Long:  "Get the status of jobs running for a file or directory path. Jobs are grouped by RST, and sorted by when they were created. The most recently created Job determines the path's status for that RST.",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.Path = args[0]
			return runGetStatusCmd(cmd, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.JobID, "job-id", "", "If a file path is specified, only return results for this specific job.")
	cmd.Flags().BoolVar(&cfg.Verbose, "verbose", false, "Include details about individual work requests and responses.")
	cmd.Flags().IntVar(&cfg.LimitJobsPerPath, "limit", 1, "Limit the number of jobs returned for each path+RST combination (defaults to only the most recent job).")
	return cmd
}

func runGetStatusCmd(cmd *cobra.Command, cfg rst.GetJobsConfig) error {

	responses := make(chan *rst.GetJobsResponse, 1024)
	err := rst.GetJobs(cmd.Context(), cfg, responses)
	if err != nil {
		return err
	}
	w := cmdfmt.NewDeprecatedTableWriter(os.Stdout)
	defer w.Flush()

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

			if len(resp.Results) == 0 {
				fmt.Fprintf(&w, "Path: %s - No jobs found for path.\n", resp.Path)
				continue
			} else {
				fmt.Fprintf(&w, "Path: %s\n", resp.Path)
				totalPaths++
			}

			// Sort jobs by when they were created so the most recently created job always determines
			// the path's status for that RST.
			sort.Slice(resp.Results, func(i, j int) bool {
				return resp.Results[i].Job.Created.GetSeconds() > resp.Results[j].Job.Created.GetSeconds()
			})

			// Print jobs by RST ID:
			rstsForPath := []string{}
			jobsPerRST := map[string][]*beeremote.JobResult{}
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

			for _, id := range rstsForPath {
				fmt.Fprintf(&w, "%s Remote Storage Target: %s\n", convertJobStateToEmoji(jobsPerRST[id][0].Job.Status.State), id)
				for i, job := range jobsPerRST[id] {
					if i >= cfg.LimitJobsPerPath {
						break
					}
					fmt.Fprintf(&w, "\t%s Job:", convertJobStateToEmoji(job.Job.Status.State))
					fmt.Fprintf(&w, " id: %s, state: %s, ", job.Job.Id, job.Job.Status.State)
					// Optimize printing jobs we know about (for example no need to print path again):
					switch job.Job.Request.Type.(type) {
					case *beeremote.JobRequest_Sync:
						fmt.Fprintf(&w, "request: {remote_storage_target: %s, type: sync, %s}", job.Job.Request.RemoteStorageTarget, job.Job.Request.GetSync())
					default:
						fmt.Fprintf(&w, "request: {%s}", job.Job.GetRequest())
					}

					if cfg.Verbose {
						fmt.Fprintf(&w, ", message: %s", job.Job.Status.Message)
						if job.Job.ExternalId != "" {
							fmt.Fprintf(&w, ", external_id: %s", job.Job.ExternalId)
						} else {
							fmt.Fprintf(&w, ", external_id: none")
						}
					}

					fmt.Fprintf(&w, "\n")

					if cfg.Verbose {
						if len(job.WorkRequests) == len(job.WorkResults) {
							sort.Slice(job.WorkRequests, func(i, j int) bool {
								return job.WorkRequests[i].RequestId <= job.WorkRequests[j].RequestId
							})
							sort.Slice(job.WorkResults, func(i, j int) bool {
								return job.WorkResults[i].Work.RequestId <= job.WorkResults[j].Work.RequestId
							})
							for i, wr := range job.GetWorkResults() {
								fmt.Fprintf(&w, "\t\t%s Work ID: %s\n", convertWorkStateToEmoji(wr.Work.Status.State), wr.Work.RequestId)
								fmt.Fprintf(&w, "\t\t\t\t Request: %s\n", job.WorkRequests[i])
								fmt.Fprintf(&w, "\t\t\t\t Result: %s\n", wr)
								if job.WorkRequests[i].RequestId != wr.Work.RequestId {
									fmt.Fprintf(&w, "WARNING: There are an equal number of requests/results but the request and result IDs don't all line up (likely this is a bug in BeeRemote).")
								}
							}
						} else {
							// This generally shouldn't happen unless there is a bug.
							fmt.Fprintf(&w, "WARNING: The number of work requests and results does not match, printing individual results (likely this is a bug)).")
							fmt.Fprintf(&w, "\t\t\tWork Requests:\n")
							for _, wr := range job.GetWorkRequests() {
								fmt.Fprintf(&w, "\t\t\t\t   %s\n", wr)
							}
							fmt.Fprintf(&w, "\t\t\tWork Results:\n")
							for _, wr := range job.GetWorkResults() {
								fmt.Fprintf(&w, "\t\t\t\t%s %s\n", convertWorkStateToEmoji(wr.Work.Status.State), wr)
							}
						}
					}
					fmt.Fprintf(&w, "\t   (created: %s | last updated: %s)\n", job.Job.Created.AsTime(), job.Job.Status.Updated.AsTime())
					fmt.Fprintf(&w, "\n")
				}
			}
		}
	}

	fmt.Fprintf(&w, "Total Paths Found: %d\n", totalPaths)
	return nil
}

func convertJobStateToEmoji(state beeremote.Job_State) string {
	representation, exists := jobStateMap[state]
	if !exists {
		return "�"
	}
	if !viper.GetBool(config.DisableEmojisKey) {
		return representation.emoji
	}
	return representation.alternative
}

type jobStateEmoji struct {
	emoji       string
	alternative string
}

var jobStateMap = map[beeremote.Job_State]jobStateEmoji{
	beeremote.Job_UNKNOWN:    {"❓", "(UNKNOWN)"},
	beeremote.Job_UNASSIGNED: {"⏳", "(UNASSIGNED)"},
	beeremote.Job_SCHEDULED:  {"⏳", "(SCHEDULED)"},
	beeremote.Job_RUNNING:    {"⏳", "(RUNNING)"},
	beeremote.Job_ERROR:      {"\u26A0\ufe0f ", "(ERROR)"},
	beeremote.Job_FAILED:     {"❌", "(FAILED)"},
	beeremote.Job_CANCELLED:  {"🚫", "(CANCELLED)"},
	beeremote.Job_COMPLETED:  {"✅", "(COMPLETED)"},
}

func convertWorkStateToEmoji(state flex.Work_State) string {
	representation, exists := workStateMap[state]
	if !exists {
		return "�"
	}
	if !viper.GetBool(config.DisableEmojisKey) {
		return representation.emoji
	}
	return representation.alternative
}

type workStateEmoji struct {
	emoji       string
	alternative string
}

var workStateMap = map[flex.Work_State]workStateEmoji{
	flex.Work_UNKNOWN:   {"❓", "(UNKNOWN)"},
	flex.Work_CREATED:   {"⏳", "(CREATED)"},
	flex.Work_SCHEDULED: {"⏳", "(SCHEDULED)"},
	flex.Work_RUNNING:   {"⏳", "(RUNNING)"},
	flex.Work_ERROR:     {"\u26A0\ufe0f ", "(ERROR)"},
	flex.Work_FAILED:    {"❌", "(FAILED)"},
	flex.Work_CANCELLED: {"🚫", "(CANCELLED)"},
	flex.Work_COMPLETED: {"✅", "(COMPLETED)"},
}
