package rst

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
)

type cleanupOrphanedConfig struct {
	yes     bool
	recurse bool
	verbose bool
}

func newCleanupOrphanedCmd() *cobra.Command {
	frontendCfg := cleanupOrphanedConfig{}
	backendCfg := rst.CleanupOrphanedCfg{}

	cmd := &cobra.Command{
		Use:   "cleanup-orphaned <path-prefix>",
		Short: "Remove Remote DB entries for paths that no longer exist in BeeGFS",
		Long: `Remove Remote DB entries for paths that no longer exist in BeeGFS.
This command only removes BeeGFS Remote database entries. It does not modify BeeGFS files
or remote objects.

Use --recurse to treat <path-prefix> as a prefix and update all matching entries (requires --yes).
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			backendCfg.PathPrefix = args[0]
			if frontendCfg.recurse && !frontendCfg.yes {
				return fmt.Errorf("the recurse mode updates the specified entry and ALL child entries, if you're sure this is what you want add the --yes flag")
			}
			backendCfg.Recurse = frontendCfg.recurse
			frontendCfg.verbose = frontendCfg.verbose || viper.GetBool(config.DebugKey)
			return runCleanupOrphanedCmd(cmd, frontendCfg, backendCfg)
		},
	}

	cmd.Flags().BoolVar(&frontendCfg.yes, "yes", false, "Required when using --recurse.")
	cmd.Flags().BoolVar(&frontendCfg.recurse, "recurse", false, "Treat the provided path as a prefix and update all matching paths.")
	cmd.Flags().BoolVar(&frontendCfg.verbose, "verbose", false, "Print results for each deleted or skipped path (errors are always printed).")
	return cmd
}

func runCleanupOrphanedCmd(cmd *cobra.Command, frontendCfg cleanupOrphanedConfig, backendCfg rst.CleanupOrphanedCfg) error {
	results, wait, err := rst.CleanupOrphaned(cmd.Context(), backendCfg)
	if err != nil {
		if errors.Is(err, filesystem.ErrUnmounted) {
			return fmt.Errorf("%w (hint: BeeGFS must be mounted to check path existence)", err)
		}
		return err
	}

	var total int
	var deleted int
	var skipped int
	var errs int

	tbl := cmdfmt.NewPrintomatic([]string{"result", "path", "message"}, []string{"result", "path", "message"})

	for res := range results {
		total++
		if res.Err != nil {
			errs++
			tbl.AddItem("error", res.Path, res.Err.Error())
			continue
		}
		if res.Deleted {
			deleted++
		} else if res.Skipped {
			skipped++
		}
		if frontendCfg.verbose && (res.Deleted || res.Skipped) {
			action := "skipped"
			if res.Deleted {
				action = "deleted"
			}
			tbl.AddItem(action, res.Path, res.Message)
		}
	}

	tbl.PrintRemaining()

	cmdfmt.Printf("Summary: scanned %d entries | %d deleted | %d skipped | %d errors\n", total, deleted, skipped, errs)
	if total == 0 {
		cmdfmt.Printf("No matching entries found.\n")
	}

	if err := wait(); err != nil {
		return err
	}
	if errs != 0 {
		return util.NewCtlError(errors.New("one or more entries could not be cleaned"), util.PartialSuccess)
	}
	return nil
}
