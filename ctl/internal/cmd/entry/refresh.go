package entry

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/types"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
)

type frontendCfg struct {
	recurse            bool
	stdinDelimiter     string
	confirmBulkUpdates bool
}

func newRefreshEntryInfoCmd() *cobra.Command {
	var cfg frontendCfg
	cmd := &cobra.Command{
		Use:   "refresh <path> [<path>] ...",
		Short: "Refresh the metadata information for the specified files or directories.",
		Long: `Refresh the metadata information for the specified files or directories. Under normal circumstances, this operation 
is not required. However, it ensures that BeeGFS metadata is fully synchronized with the current state of the underlying
file system. This can be particularly useful in scenarios where inconsistencies arise, such as after using beegfs-fsck
to resolve corruptions or when there are delayed metadata updates from the supporting file systems.`,
		Args: func(cmd *cobra.Command, paths []string) error {
			if len(paths) == 0 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, paths []string) error {
			return runRefreshEntryInfoCmd(cmd, paths, cfg)
		},
	}

	cmd.Flags().StringVar(&cfg.stdinDelimiter, "stdin-delimiter", "\\n", `Change the string delimiter used to separate paths when reading from stdin. For example,
use --stdin-delimiter=\"\\x00\" to use a NULL character as the delimiter.`)
	cmd.Flags().BoolVar(&cfg.confirmBulkUpdates, "yes", false, "Acknowledge potential metadata refresh for a large number of entries when executing this command.")
	cmd.Flags().BoolVar(&cfg.recurse, "recurse", false, "Recursively update metadata for entries if <path> is a directory.")
	return cmd
}

func runRefreshEntryInfoCmd(cmd *cobra.Command, paths []string, cfg frontendCfg) error {
	if cfg.recurse {
		if !cfg.confirmBulkUpdates {
			return fmt.Errorf("the recurse mode updates the specified entry and ALL child entries, if you're sure this is what you want add the --yes flag")
		}
	}

	method, err := entry.DetermineInputMethod(paths, cfg.recurse, cfg.stdinDelimiter)
	if err != nil {
		return err
	}

	resultsChan, errChan, err := entry.RefreshEntriesInfo(cmd.Context(), method)
	if err != nil {
		return err
	}

	var multiErr types.MultiError
	columns := []string{"path", "status", "entry_id"}
	tbl := cmdfmt.NewPrintomatic(columns, columns)
	anyErrors := false

run:
	for {
		select {
		case r, ok := <-resultsChan:
			if !ok {
				break run
			}
			if r.Status != beegfs.OpsErr_SUCCESS {
				anyErrors = true
			}
			tbl.AddItem(r.Path, r.Status, r.EntryID)
		case err, ok := <-errChan:
			if !ok {
				break run
			}
			multiErr.Errors = append(multiErr.Errors, err)
		}

	}
	tbl.PrintRemaining()

	if anyErrors {
		multiErr.Errors = append(multiErr.Errors, fmt.Errorf("unable to refresh one or more entries (see individual results for details)"))
	}

	if len(multiErr.Errors) != 0 {
		return error(&multiErr)
	}
	return nil
}
