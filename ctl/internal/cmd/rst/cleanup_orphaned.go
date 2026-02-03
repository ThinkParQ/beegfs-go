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
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			backendCfg.PathPrefix = args[0]
			normalizedPrefix := backendCfg.PathPrefix
			if client, err := config.BeeGFSClient(backendCfg.PathPrefix); err == nil {
				if rel, err := client.GetRelativePathWithinMount(backendCfg.PathPrefix); err == nil {
					normalizedPrefix = rel
				}
			}
			if normalizedPrefix == "/" && !frontendCfg.yes {
				return fmt.Errorf("cleanup-orphaned for '/' requires --yes")
			}
			frontendCfg.verbose = frontendCfg.verbose || viper.GetBool(config.DebugKey)
			return runCleanupOrphanedCmd(cmd, frontendCfg, backendCfg)
		},
	}

	cmd.Flags().BoolVar(&frontendCfg.yes, "yes", false, "Required when <path-prefix> is '/'.")
	cmd.Flags().BoolVar(&frontendCfg.verbose, "verbose", false, "Print results for each missing path.")
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
	var missing int
	var deleted int
	var skipped int
	var errs int

	for res := range results {
		total++
		if res.Err != nil {
			errs++
			if frontendCfg.verbose {
				cmdfmt.Printf("error: %s: %s\n", res.Path, res.Err)
			}
			continue
		}
		if res.Missing {
			missing++
		}
		if res.Deleted {
			deleted++
		} else if res.Skipped {
			skipped++
		}
		if frontendCfg.verbose && res.Missing {
			action := "skipped"
			if res.Deleted {
				action = "deleted"
			}
			if res.Message != "" {
				cmdfmt.Printf("%s: %s (%s)\n", action, res.Path, res.Message)
			} else {
				cmdfmt.Printf("%s: %s\n", action, res.Path)
			}
		}
	}

	cmdfmt.Printf("Summary: scanned %d entries | %d missing | %d deleted | %d skipped | %d errors\n", total, missing, deleted, skipped, errs)
	if total == 0 {
		cmdfmt.Printf("No matching entries found.\n")
	}

	if err := wait(); err != nil {
		return err
	}
	if errs != 0 || skipped != 0 {
		return util.NewCtlError(errors.New("one or more entries could not be cleaned"), util.PartialSuccess)
	}
	return nil
}
