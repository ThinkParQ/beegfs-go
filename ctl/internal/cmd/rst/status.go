package rst

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	iUtil "github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"go.uber.org/zap"
)

type statusConfig struct {
	stdinDelimiter string
	recurse        bool
	verbose        bool
	summarize      bool
}

func newStatusCmd() *cobra.Command {

	frontendCfg := statusConfig{}
	backendCfg := rst.GetStatusCfg{}
	cmd := &cobra.Command{
		Use:   "status <path>",
		Short: "Check if files in BeeGFS are synchronized with their remote targets",
		Long: `Check if files in BeeGFS are synchronized with their remote targets.
This mode checks if files in BeeGFS have been modified since the most recently created job for each remote target.
Use "job list" for additional details and a complete list of jobs including paths that no longer exist, or do not exist yet.

By default only files that are not currently in sync with their configured or the manually specified remote targets are listed.
Use the verbose flag to print all entries including ones that are synchronized or do not have remote targets configured.
Use the debug flag to print additional details such as the last job ID and modification timestamp from the file and last job.

By default, sync status is determined by comparing the local BeeGFS file with the most recent job state recorded in the Remote database. 
This avoids unnecessary API requests to the remote target. Use --verify-remote to instead query each file's status with the remote target.
This is useful if you suspect changes to the remote file that occurred outside of BeeGFS Remote.

File status checks run in parallel by default. To enforce sequential processing (and preserve output order), set BEEGFS_NUM_WORKERS=1.

Specifying Paths:
* A single file can be specified, or a directory can be specified with the --recurse flag to check all files in that directory are synchronized.
* When supported by the current shell, standard wildcards (globbing patterns) can be used in each path to return info about multiple entries.
* Multiple entries can be provided using stdin by specifying '-' as the path (example: 'cat file_list.txt | beegfs entry info -').`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			frontendCfg.verbose = frontendCfg.verbose || viper.GetBool(config.DebugKey)
			backendCfg.Debug = viper.GetBool(config.DebugKey)
			return runStatusCmd(cmd, frontendCfg, backendCfg)
		},
	}

	cmd.Flags().VarP(iUtil.NewRemoteTargetsFlag(&backendCfg.RemoteTargets), "remote-targets", "r", `Ignore the remote targets configured on each entry and only check files are synchronized with this comma-separated list of target IDs.`)
	cmd.Flags().StringVar(&frontendCfg.stdinDelimiter, "stdin-delimiter", "\n", "Change the string delimiter used to determine individual paths when read from stdin (e.g., --stdin-delimiter=\"\\x00\" for NULL).")
	cmd.Flags().BoolVar(&frontendCfg.recurse, "recurse", false, "When <path> is a single directory recursively print information about all entries beneath the path (this may return large amounts of output, for example if the BeeGFS root is the provided path).")
	cmd.Flags().BoolVar(&frontendCfg.verbose, "verbose", false, fmt.Sprintf("Print all paths, not just ones that are unsynchronized. Use %s to print additional details for debugging.", config.DebugKey))
	cmd.Flags().BoolVar(&frontendCfg.summarize, "summarize", false, "Don't print results for individual paths and only print a summary.")
	cmd.Flags().StringVar(&backendCfg.FilterExpr, "filter-files", "", filesystem.FilterFilesHelp)
	cmd.Flags().BoolVar(&backendCfg.VerifyRemote, "verify-remote", false, "Also queries the remote storage target(s) to detect changes not tracked by BeeGFS Remote (slower than local-only verification).")
	cmd.MarkFlagsMutuallyExclusive("verbose", "summarize")
	return cmd
}

func runStatusCmd(cmd *cobra.Command, frontendCfg statusConfig, backendCfg rst.GetStatusCfg) error {

	log, _ := config.GetLogger()
	paths := cmd.Flags().Args()
	ctx := cmd.Context()

	// Setup the method for sending paths to the backend:
	method, err := util.DeterminePathInputMethod(paths, frontendCfg.recurse, frontendCfg.stdinDelimiter)
	if err != nil {
		return err
	}

	results, wait, err := rst.GetStatus(ctx, method, backendCfg)
	if err != nil {
		return err
	}

	// Setup the table to print results
	tbl := cmdfmt.NewPrintomatic([]string{"ok", "path", "explanation"}, []string{"ok", "path", "explanation"})

	// Set to false to control when the remaining table entries are printed instead of having them
	// print automatically when the function returns (i.e., if an error happens).
	autoPrintRemaining := true
	defer func() {
		if autoPrintRemaining {
			tbl.PrintRemaining()
		}
	}()

	totalEntries := 0
	unsyncedFiles := 0
	offloadedFiles := 0
	syncedFiles := 0
	notAttemptedFiles := 0
	noTargetFiles := 0
	notSupportedFiles := 0
	directories := 0
	printRowByDefault := false

run:
	for {
		select {
		case <-ctx.Done():
			break run
		case path, ok := <-results:
			if !ok {
				break run
			}
			totalEntries++
			switch path.SyncStatus {
			case rst.Synchronized:
				syncedFiles++
				printRowByDefault = false
			case rst.Offloaded:
				offloadedFiles++
				printRowByDefault = false
			case rst.Unsynchronized:
				unsyncedFiles++
				printRowByDefault = true
			case rst.NotAttempted:
				notAttemptedFiles++
				printRowByDefault = true
			case rst.NoTargets:
				noTargetFiles++
				printRowByDefault = false
			case rst.NotSupported:
				notSupportedFiles++
				printRowByDefault = false
			case rst.Directory:
				directories++
				log.Debug("ignoring directory", zap.Any("path", path), zap.Any("reason", path.SyncReason))
				continue
			default:
				return fmt.Errorf("unknown sync status %d for path %s", path.SyncStatus, path.Path)
			}

			if !frontendCfg.summarize && (frontendCfg.verbose || printRowByDefault || path.Warning) {
				tbl.AddItem(path.SyncStatus, path.Path, path.SyncReason)
			}
		}
	}

	autoPrintRemaining = false
	tbl.PrintRemaining()

	if viper.GetBool(config.DisableEmojisKey) {
		cmdfmt.Printf("Summary: found %d entries | %d synchronized | %d offloaded | %d unsynchronized | %d not attempted | %d without remote targets | %d not supported | %d directories\n",
			totalEntries, syncedFiles, offloadedFiles, unsyncedFiles, notAttemptedFiles, noTargetFiles, notSupportedFiles, directories)
	} else {
		cmdfmt.Printf("Summary: found %d entries | %s %d synchronized | %s %d offloaded | %s %d unsynchronized | %s %d not attempted | %s %d without remote targets | %s %d not supported | %s %d directories\n",
			totalEntries, rst.Synchronized, syncedFiles, rst.Offloaded, offloadedFiles, rst.Unsynchronized, unsyncedFiles, rst.NotAttempted, notAttemptedFiles, rst.NoTargets, noTargetFiles, rst.NotSupported, notSupportedFiles, rst.Directory, directories)
	}
	if noTargetFiles != 0 {
		cmdfmt.Printf("INFO: not all files have remote targets configured\n")
	}

	if totalEntries != (syncedFiles + offloadedFiles + unsyncedFiles + notAttemptedFiles + noTargetFiles + notSupportedFiles + directories) {
		return fmt.Errorf("the total number of entries does not match the number of entries in various states (this is probably a bug)")
	}

	if err = wait(); err != nil {
		return err
	}
	if unsyncedFiles != 0 {
		return iUtil.NewCtlError(errors.New("not all files are synchronized"), iUtil.PartialSuccess)
	}
	return nil
}
