package rst

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/rst"
)

func newPushCmd() *cobra.Command {
	cfg := rst.SyncJobRequestCfg{}
	cmd := &cobra.Command{
		Use:   "push --rst=<id> <path>",
		Short: "Upload a file or directory in BeeGFS to a Remote Storage Target.",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.Path = args[0]
			return runPushOrPullCmd(cmd, cfg)
		},
	}
	cmd.Flags().StringVar(&cfg.RSTID, "rst", "", "The ID of the Remote Storage Target to synchronize with.")
	cmd.MarkFlagRequired("rst")
	return cmd
}

func newPullCmd() *cobra.Command {
	cfg := rst.SyncJobRequestCfg{
		Download: true,
	}
	cmd := &cobra.Command{
		Use:   "pull --rst=<id> <path>",
		Short: "Download a file to BeeGFS from a Remote Storage Target.",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg.Path = args[0]
			return runPushOrPullCmd(cmd, cfg)
		},
	}
	cmd.Flags().StringVar(&cfg.RSTID, "rst", "", "The ID of the Remote Storage Target to synchronize with.")
	cmd.MarkFlagRequired("rst")
	cmd.Flags().BoolVar(&cfg.Overwrite, "overwrite", false, "When downloading a file, if a file already exists at the specified path in BeeGFS, an error is returned by default. Optionally the file can be overwritten instead. Note files are always uploaded and will be overwritten unless the RST has file/object versioning enabled.")
	cmd.Flags().StringVar(&cfg.RemotePath, "remote-path", "", "By default when downloading files/objects, the path where the file should be downloaded in BeeGFS is assumed to also be the file path/object key in the RST. Optionally the remote path can be specified to restore a file in an RST to a different location in BeeGFS (this is ignored for uploads).")
	return cmd
}

func runPushOrPullCmd(cmd *cobra.Command, cfg rst.SyncJobRequestCfg) error {

	// This could be made user configurable if it ever makes sense.
	cfg.ChanSize = 1024
	responses, err := rst.SubmitSyncJobRequests(cmd.Context(), cfg)
	if err != nil {
		return err
	}
	totalJobs := 0
	totalErrors := 0

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
				totalErrors++
				fmt.Printf("Unable to schedule job for file: %s \n", resp.Err)
				continue
			}
			totalJobs++
			if config.Get().Debug {
				fmt.Printf("Scheduled job for file: %s\n", resp.Result.GetJob())
			}
		}
	}
	fmt.Printf("\nTotal Jobs Scheduled: %d | Paths Skipped Due to Errors: %d\n", totalJobs, totalErrors)
	return nil
}
