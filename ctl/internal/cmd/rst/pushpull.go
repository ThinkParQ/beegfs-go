package rst

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"

	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

type pushPullCfg struct {
	verbose bool
	width   int
}

func newPushCmd() *cobra.Command {
	frontendCfg := pushPullCfg{}
	backendCfg := flex.JobRequestCfg{}
	cmd := &cobra.Command{
		Use:   "push <path>",
		Short: "Upload a file or directory in BeeGFS to a Remote Storage Target",
		Long: `Upload a file or directory in BeeGFS to a Remote Storage Target.
By default the Remote Storage Target where entries are pushed is determined by the RST ID(s) set on each entry.
Optionally an RST ID can be provided to perform a one-time push to that RST.

When uploading multiple entries, any entries that do not have RSTs configured are silently ignored.
If there is an error uploading any of the entries the return code will be 2.
If a fatal error occurs and the command exits early before trying to upload all entries, the return code will be 1.

WARNING: Files are always uploaded and existing files overwritten unless the remote target has file/object versioning enabled.`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("missing <path> argument")
			}
			if len(args) > 1 {
				return fmt.Errorf("invalid number of arguments. Be sure to quote file glob pattern")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			backendCfg.SetPath(args[0])
			return runPushOrPullCmd(cmd, frontendCfg, &backendCfg)
		},
	}
	cmd.Flags().Uint32VarP(&backendCfg.RemoteStorageTarget, "remote-target", "r", 0, "Perform a one time push to the specified Remote Storage Target ID.")
	cmd.Flags().BoolVar(&backendCfg.Force, "force", false, "Force push file(s) to the remote target even if the file is already in sync or another client currently has them open for writing (note the job may later fail or the uploaded file may not be the latest version).")
	cmd.Flags().MarkHidden("force")
	cmd.Flags().BoolVarP(&frontendCfg.verbose, "verbose", "v", false, "Print additional details about each job (use --debug) to also print work requests and results.")
	cmd.Flags().IntVar(&frontendCfg.width, "column-width", 35, "Set the maximum width of some columns before they overflow.")
	cmd.Flags().BoolVarP(&backendCfg.StubLocal, "stub-local", "s", false, "Replace with a stub after the file is uploaded.")
	return cmd
}

func newPullCmd() *cobra.Command {
	frontendCfg := pushPullCfg{}
	backendCfg := flex.JobRequestCfg{
		Download: true,
	}
	cmd := &cobra.Command{
		Use:   "pull --remote-target=<id> --remote-path=<path> <path>",
		Short: "Download a file to BeeGFS from a Remote Storage Target",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 1 {
				return fmt.Errorf("missing <path> argument")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			backendCfg.SetPath(args[0])
			return runPushOrPullCmd(cmd, frontendCfg, &backendCfg)
		},
	}
	cmd.Flags().Uint32VarP(&backendCfg.RemoteStorageTarget, "remote-target", "r", 0, "The ID of the Remote Storage Target where the file should be pulled from.")
	cmd.Flags().BoolVar(&backendCfg.Overwrite, "overwrite", false, "Overwrite existing files in BeeGFS. Note this only overwrites the file's contents, metadata including any configured RSTs will remain.")
	cmd.Flags().StringVarP(&backendCfg.RemotePath, "remote-path", "p", "", "The name/path of the object/file in the remote target you wish to download. If absent, the in-mount path will be used.")
	cmd.MarkFlagRequired("remote-path")
	cmd.Flags().BoolVarP(&backendCfg.StubLocal, "stub-local", "s", false, "Create stub files for the remote objects or files.")
	cmd.Flags().BoolVar(&backendCfg.Flatten, "flatten", false, "Flatten the remote directory structure. The directory delimiter will be replaced with an underscore. Only applicable to BuilderJob requests.")
	cmd.Flags().BoolVar(&backendCfg.Force, "force", false, "Force pulling file(s) from the remote target even if the file is already in sync or another client currently has them open for reading or writing (note other clients may see errors, the job may later fail, or the downloaded file may not be the latest version).")
	cmd.Flags().MarkHidden("flatten")
	cmd.Flags().MarkHidden("force")
	cmd.Flags().BoolVarP(&frontendCfg.verbose, "verbose", "v", false, "Print additional details about each job (use --debug) to also print work requests and results.")
	cmd.Flags().IntVar(&frontendCfg.width, "column-width", 35, "Set the maximum width of some columns before they overflow.")
	return cmd
}

func runPushOrPullCmd(cmd *cobra.Command, frontendCfg pushPullCfg, backendCfg *flex.JobRequestCfg) error {

	// This could be made user configurable if it ever makes sense.
	responses, err := rst.SubmitJobRequest(cmd.Context(), backendCfg, 1024)
	if err != nil {
		return err
	}

	noRSTSpecified := 0
	fileNotSupported := 0
	errStartingSync := 0
	syncStarted := 0
	syncCompleted := 0
	syncOffloaded := 0
	syncInProgress := 0
	syncNotAllowed := 0

	tbl := newJobsTable(withJobDetails(frontendCfg.verbose), withColumnWidth(frontendCfg.width))

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
					// If we return early ensure to print any buffered entries.
					tbl.PrintRemaining()
					return resp.Err
				}
				if errors.Is(resp.Err, rst.ErrFileHasNoRSTs) {
					noRSTSpecified++
					if viper.GetBool(config.DebugKey) || frontendCfg.verbose {
						tbl.MinimalRow(resp.Path, fmt.Sprintf("%s (ignored)", resp.Err.Error()))
					}
				} else if errors.Is(resp.Err, rst.ErrFileTypeUnsupported) {
					fileNotSupported++
					if viper.GetBool(config.DebugKey) || frontendCfg.verbose {
						tbl.MinimalRow(resp.Path, fmt.Sprintf("%s (ignored)", resp.Err.Error()))
					}
				} else {
					errStartingSync++
					tbl.MinimalRow(resp.Path, resp.Err.Error())
				}
				continue
			}

			switch resp.Status {
			case beeremote.SubmitJobResponse_CREATED:
				syncStarted++
			case beeremote.SubmitJobResponse_ALREADY_COMPLETE:
				syncCompleted++
			case beeremote.SubmitJobResponse_ALREADY_OFFLOADED:
				syncOffloaded++
			case beeremote.SubmitJobResponse_EXISTING:
				syncInProgress++
			case beeremote.SubmitJobResponse_NOT_ALLOWED:
				// This indicates the last job failed and requires manual intervention. Always print
				// these jobs as the user will likely want to see them anyway.
				syncNotAllowed++
				tbl.Row(resp.Result)
				continue
			default:
				tbl.PrintRemaining()
				tbl.Row(resp.Result)
				return fmt.Errorf("unknown response status: %s (are the versions of CTL and Remote compatible?)", resp.Status.String())
			}

			if viper.GetBool(config.DebugKey) || frontendCfg.verbose {
				tbl.Row(resp.Result)
			}
		}
	}

	tbl.PrintRemaining()

	var result string
	if viper.GetBool(config.DisableEmojisKey) {
		result = fmt.Sprintf("%d synced | %d offloaded | %d syncing | %d scheduled | %d previous sync failure | %d error starting sync | %d no remote target (ignored) | %d not supported (ignored)\n",
			syncCompleted, syncOffloaded, syncInProgress, syncStarted, syncNotAllowed, errStartingSync, noRSTSpecified, fileNotSupported)
	} else {
		result = fmt.Sprintf("✅ %d synced | ☁️ %d offloaded | 🔄 %d syncing | ⏳ %d scheduled | ❌ %d previous sync failure | \u26A0\ufe0f\u200C %d error starting sync | ⛔ %d no remote target (ignored) | 🚫 %d not supported (ignored)\n",
			syncCompleted, syncOffloaded, syncInProgress, syncStarted, syncNotAllowed, errStartingSync, noRSTSpecified, fileNotSupported)
	}

	if errStartingSync != 0 || syncNotAllowed != 0 {
		return util.NewCtlError(errors.New(result), util.PartialSuccess)
	}
	cmdfmt.Printf("Success: %s", result)
	return nil
}
