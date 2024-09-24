package rst

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
)

type pushPullCfg struct {
	detail bool
	width  int
}

func newPushCmd() *cobra.Command {
	frontendCfg := pushPullCfg{}
	backendCfg := rst.SyncJobRequestCfg{}
	cmd := &cobra.Command{
		Use:   "push --rst=<id> <path>",
		Short: "Upload a file or directory in BeeGFS to a Remote Storage Target.",
		Long: `Upload a file or directory in BeeGFS to a Remote Storage Target.
By default the Remote Storage Target where entries are pushed is determined by the RST ID(s) set on each entry.
Optionally an RST ID can be provided to perform a one-time push to that RST.
WARNING: When uploading multiple entries, any entries that do not have RSTs configured are ignored.
		`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			backendCfg.Path = args[0]
			return runPushOrPullCmd(cmd, frontendCfg, backendCfg)
		},
	}
	cmd.Flags().Uint32Var(&backendCfg.RSTID, "rst", 0, "Perform a one time push to the specified Remote Storage Target ID.")
	cmd.Flags().BoolVar(&backendCfg.Force, "force", false, "Force push file(s) to the RST even if another client currently has them open for writing (note the job may later fail or the uploaded file may not be the latest version).")
	cmd.Flags().MarkHidden("force")
	cmd.Flags().BoolVar(&frontendCfg.detail, "detail", false, "Print additional details about each job (use --debug) to also print work requests and results.")
	cmd.Flags().IntVar(&frontendCfg.width, "width", 35, "Set the maximum width of some columns before they overflow.")
	return cmd
}

func newPullCmd() *cobra.Command {
	frontendCfg := pushPullCfg{}
	backendCfg := rst.SyncJobRequestCfg{
		Download: true,
	}
	cmd := &cobra.Command{
		Use:   "pull --rst=<id> <path>",
		Short: "Download a file to BeeGFS from a Remote Storage Target.",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			if backendCfg.RSTID == 0 {
				return fmt.Errorf("invalid rst. The rst id must be greater than zero")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			backendCfg.Path = args[0]
			return runPushOrPullCmd(cmd, frontendCfg, backendCfg)
		},
	}
	cmd.Flags().Uint32Var(&backendCfg.RSTID, "rst", 0, "The ID of the Remote Storage Target where the file should be pulled from.")
	cmd.MarkFlagRequired("rst")
	cmd.Flags().BoolVar(&backendCfg.Overwrite, "overwrite", false, "When downloading a file, if a file already exists at the specified path in BeeGFS, an error is returned by default. Optionally the file can be overwritten instead. Note files are always uploaded and will be overwritten unless the RST has file/object versioning enabled.")
	cmd.Flags().StringVar(&backendCfg.RemotePath, "remote-path", "", "By default when downloading files/objects, the path where the file should be downloaded in BeeGFS is assumed to also be the file path/object key in the RST. Optionally the remote path can be specified to restore a file in an RST to a different location in BeeGFS (this is ignored for uploads).")
	cmd.Flags().BoolVar(&backendCfg.Force, "force", false, "Force pulling file(s) from the RST even if another client currently has them open for reading or writing (note other clients may see errors, the job may later fail, or the downloaded file may not be the latest version).")
	cmd.Flags().MarkHidden("force")
	cmd.Flags().BoolVar(&frontendCfg.detail, "detail", false, "Print additional details about each job (use --debug) to also print work requests and results.")
	cmd.Flags().IntVar(&frontendCfg.width, "width", 35, "Set the maximum width of some columns before they overflow.")
	return cmd
}

func runPushOrPullCmd(cmd *cobra.Command, frontendCfg pushPullCfg, backendCfg rst.SyncJobRequestCfg) error {

	// This could be made user configurable if it ever makes sense.
	backendCfg.ChanSize = 1024
	responses, err := rst.SubmitSyncJobRequests(cmd.Context(), backendCfg)
	if err != nil {
		return err
	}
	totalJobs := 0
	totalIgnored := 0
	totalErrors := 0

	tbl := newJobsTable(withJobDetails(frontendCfg.detail), withColumnWidth(frontendCfg.width))
	defer tbl.PrintRemaining()

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
				if resp.FatalErr {
					return resp.Err
				}
				if errors.Is(resp.Err, rst.ErrFileHasNoRSTs) {
					totalIgnored++
					if viper.GetBool(config.DebugKey) || frontendCfg.detail {
						tbl.MinimalRow(resp.Path, fmt.Errorf("%s (ignoring file)", resp.Err))

					}
				} else {
					totalErrors++
					tbl.MinimalRow(resp.Path, fmt.Errorf("%s (skipping file)", resp.Err))
				}
				continue
			}
			totalJobs++
			if viper.GetBool(config.DebugKey) || frontendCfg.detail {
				tbl.Row(resp.Result)
			}
		}
	}
	fmt.Printf("\nTotal Jobs Scheduled: %d | Paths Skipped Due to Errors: %d | Ignored Paths: %d\n", totalJobs, totalErrors, totalIgnored)
	return nil
}
