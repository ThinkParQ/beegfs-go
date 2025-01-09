package entry

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/types"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
)

type entryDisposalCfg struct {
	printFiles bool
}

func newEntryDisposalCmd() *cobra.Command {
	frontendCfg := entryDisposalCfg{}
	backendCfg := entry.DisposalCfg{}

	cmd := &cobra.Command{
		Use:   "dispose-unused",
		Short: "Cleanup files that have been unlinked by users while they were still open",
		Long: `Cleanup files that have been unlinked by users while they were still open.
Generally manually disposing files not required unless a metadata service stopped unexpectedly before the unlinked file(s) were closed on all clients.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runEntryDisposalCmd(cmd, frontendCfg, backendCfg)
		},
	}

	cmd.Flags().BoolVar(&backendCfg.Dispose, "dispose", false, "By default the mode does a dry run returning what files would be cleaned up. Set to actually attempt deleting the files.")
	cmd.Flags().BoolVar(&frontendCfg.printFiles, "print-files", false, "In the dry-run mode print files marked for disposal. When disposing files, print the result for each file.")

	return cmd
}

func runEntryDisposalCmd(cmd *cobra.Command, frontendCfg entryDisposalCfg, backendCfg entry.DisposalCfg) error {
	resultsChan, errChan, err := entry.CleanupDisposals(cmd.Context(), backendCfg)
	if err != nil {
		return err
	}

	allColumns := []string{"metadata node", "entry", "disposal result"}
	tbl := cmdfmt.NewPrintomatic(allColumns, allColumns)
	statsDisposedFiles := 0
	statsTotalFiles := 0
	multiErr := &types.MultiError{}

run:
	for {
		select {
		case result, ok := <-resultsChan:
			if !ok {
				break run
			}
			statsTotalFiles += 1
			disposalResult := "Not attempted"
			if result.Result != nil {
				if *result.Result == beegfs.OpsErr_SUCCESS {
					statsDisposedFiles += 1
				}
				disposalResult = result.Result.String()
			}

			if frontendCfg.printFiles {
				tbl.AddItem(
					fmt.Sprintf("%s (%d)", result.Node.Alias, result.Node.Id.NumId),
					result.EntryID,
					disposalResult,
				)
			}
		case err, ok := <-errChan:
			if ok {
				// Once an error happens the resultsChan will be closed, however this could be a
				// buffered channel so there may still be valid results we should finish printing
				// before returning the error.
				multiErr.Errors = append(multiErr.Errors, err)
			}
		}
	}

	tbl.PrintRemaining()
	cmdfmt.Printf("Summary: disposed files: %d | total files: %d\n", statsDisposedFiles, statsTotalFiles)
	if len(multiErr.Errors) != 0 {
		return multiErr
	}
	return nil
}
