package entry

import (
	"fmt"
	"strings"

	"github.com/dsnet/golib/unitconv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/types"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
	"go.uber.org/zap"
)

type entryInfoCfg struct {
	stdinDelimiter string
	recurse        bool
	retro          bool
	retroPaths     bool
	verbose        bool
}

func newEntryInfoCmd() *cobra.Command {

	frontendCfg := entryInfoCfg{}
	backendCfg := entry.GetEntriesCfg{}
	cmd := &cobra.Command{
		Use:   "info <path> [<path>] ...",
		Short: "Get details about one or more entries in BeeGFS",
		Long: `Get details about one or more entries in BeeGFS.

Specifying Paths:
When supported by the current shell, standard wildcards (globbing patterns) can be used in each path to return info about multiple entries.
Alternatively multiple entries can be provided using stdin by specifying '-' as the path (example: 'cat file_list.txt | beegfs entry info -').`,
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if frontendCfg.verbose {
				backendCfg.Verbose = true
			}
			return runEntryInfoCmd(cmd, args, frontendCfg, backendCfg)
		},
	}

	cmd.Flags().BoolVar(&frontendCfg.recurse, "recurse", false, "When <path> is a single directory recursively print information about all entries beneath the path (WARNING: this may return large amounts of output, for example if the BeeGFS root is the provided path).")
	// TODO: https://github.com/thinkparq/ctl/issues/56
	// Update help text when verbose no longer just applies to the retro mode.
	cmd.Flags().BoolVar(&frontendCfg.verbose, "verbose", false, "In the \"retro\" mode, print more information about each entry, such as chunk and dentry paths on the servers.")
	cmd.Flags().BoolVar(&frontendCfg.retro, "retro", false, "Display entry information using the \"retro\" vertical style of the original beegfs-ctl.")
	// The same default used for stdin-delimiter to allow the help output to print correctly can't
	// be used directly. If the default changes update where this is set in getDelimiterFromString.
	cmd.Flags().StringVar(&frontendCfg.stdinDelimiter, "stdin-delimiter", "\n", "Change the string delimiter used to determine individual paths when read from stdin (e.g., --stdin-delimiter=\"\\x00\" for NULL).")
	cmd.Flags().BoolVar(&frontendCfg.retroPaths, "retro-print-paths", false, "Print paths at the top of each entry in the retro output.")
	cmd.Flags().MarkHidden("retro-print-paths")
	return cmd
}

func runEntryInfoCmd(cmd *cobra.Command, args []string, frontendCfg entryInfoCfg, backendCfg entry.GetEntriesCfg) error {

	log, _ := config.GetLogger()

	// Setup the method for sending paths to the backend:
	method, err := entry.DetermineInputMethod(args, frontendCfg.recurse, frontendCfg.stdinDelimiter)
	if err != nil {
		return err
	}

	entriesChan, errChan, err := entry.GetEntries(cmd.Context(), method, backendCfg)
	if err != nil {
		return err
	}
	defaultColumns := []string{"path", "entry id", "type", "meta node", "meta mirror", "storage pool", "stripe pattern", "storage targets", "buddy groups", "remote targets", "cool down"}
	allColumns := append(defaultColumns, "client sessions")
	numColumns := len(allColumns)
	var tbl cmdfmt.Printomatic
	if frontendCfg.retro {
		// For simplicity still use the table wrapper to handle printing the retro output. Just put
		// all the output into one unlabeled column.
		tbl = cmdfmt.NewPrintomatic([]string{}, []string{})
	} else {
		if frontendCfg.verbose {
			defaultColumns = allColumns
		}
		tbl = cmdfmt.NewPrintomatic(allColumns, defaultColumns)
	}
	defer tbl.PrintRemaining()

	var multiErr types.MultiError

run:
	for {
		select {
		case info, ok := <-entriesChan:
			if !ok {
				break run
			}
			// Still try and print as much information as possible if verbose details are not available.
			if len(info.Entry.Verbose.Err.Errors) != 0 {
				log.Warn("unable to generate all output needed to print verbose details, some information may be missing (ignoring)", zap.Error(err))
			}
			if frontendCfg.retro {
				tbl.AddItem(assembleRetroEntry(info, frontendCfg))
			} else {
				row := assembleTableRow(info, numColumns)
				if len(row) != numColumns {
					// Sanity check in case new columns are added and assembleTableRow() is not updated.
					log.Warn("number of columns in the row does not equal the expected number of columns, this is likely a bug and table output will probably not be formatted correctly (ignoring)", zap.Any("expected", numColumns), zap.Any("actual", len(row)))
				}
				tbl.AddItem(row...)
			}
		case err, ok := <-errChan:
			if ok {
				// Once an error happens the entriesChan will be closed, however this is a buffered
				// channel so there may still be valid entries we should finish printing before
				// returning the error.
				multiErr.Errors = append(multiErr.Errors, err)
			}
		}
	}

	if len(multiErr.Errors) != 0 {
		return &multiErr
	}
	return nil
}

func assembleRetroEntry(info *entry.GetEntryCombinedInfo, frontendCfg entryInfoCfg) string {
	entryToPrint := &strings.Builder{}

	if frontendCfg.retroPaths {
		fmt.Fprintf(entryToPrint, "Path: %s\n", info.Path)
	}
	fmt.Fprintf(entryToPrint, "Entry type: %s\n", info.Entry.Type)
	fmt.Fprintf(entryToPrint, "EntryID: %s\n", info.Entry.EntryID)
	if info.Entry.Type == beegfs.EntryDirectory && frontendCfg.verbose {
		fmt.Fprintf(entryToPrint, "ParentID: %s\n", info.Entry.ParentEntryID)
	}

	// When printing verbose details meta node info is printed later in the output and printed
	// twice, once for the parent as part of the dentry info, and again for the entry as part of the
	// inode info for this entry.
	printMetaNodeInfo := func(entry entry.Entry, printPlus bool) {
		frontText := ""
		if printPlus {
			frontText = " + "
		}
		if entry.FeatureFlags.IsBuddyMirrored() {
			fmt.Fprintf(entryToPrint, "%sMetadata buddy group: %d\n", frontText, entry.MetaBuddyGroup)
			fmt.Fprintf(entryToPrint, "%sCurrent primary metadata node: %s [ID: %d]\n", frontText, entry.MetaOwnerNode.Alias, entry.MetaOwnerNode.Id.NumId)
		} else {
			fmt.Fprintf(entryToPrint, "%sMetadata node: %s [ID: %d]\n", frontText, entry.MetaOwnerNode.Alias, entry.MetaOwnerNode.Id.NumId)
		}
	}

	if !frontendCfg.verbose {
		printMetaNodeInfo(info.Entry, false)
	}

	fmt.Fprintf(entryToPrint, "Stripe pattern details:\n")
	fmt.Fprintf(entryToPrint, "+ Type: %s\n", info.Entry.Pattern.Type)
	if viper.GetBool(config.RawKey) {
		fmt.Fprintf(entryToPrint, "+ Chunksize: %d\n", info.Entry.Pattern.Chunksize)
	} else {
		fmt.Fprintf(entryToPrint, "+ Chunksize: %s\n", unitconv.FormatPrefix(float64(info.Entry.Pattern.Chunksize), unitconv.Base1024, 0))
	}
	fmt.Fprintf(entryToPrint, "+ Number of storage targets: ")
	actualNumStorageTgts := len(info.Entry.Pattern.TargetIDs)
	if actualNumStorageTgts == 0 {
		// Expected if this is a directory.
		fmt.Fprintf(entryToPrint, "desired: %d\n", info.Entry.Pattern.DefaultNumTargets)
	} else {
		fmt.Fprintf(entryToPrint, "desired: %d; actual: %d\n", info.Entry.Pattern.DefaultNumTargets, actualNumStorageTgts)
	}

	if info.Entry.Type == beegfs.EntryDirectory {
		fmt.Fprintf(entryToPrint, "+ Storage Pool: %d  (%s)\n", info.Entry.Pattern.StoragePoolID, info.Entry.Pattern.StoragePoolName)
	}

	if actualNumStorageTgts != 0 {
		if info.Entry.Pattern.Type == beegfs.StripePatternBuddyMirror {
			fmt.Fprintf(entryToPrint, "+ Storage mirror buddy groups:\n")
			for _, tgt := range info.Entry.Pattern.TargetIDs {
				fmt.Fprintf(entryToPrint, "  + %d\n", tgt)
			}
		} else {
			fmt.Fprintf(entryToPrint, "+ Storage targets:\n")
			for tgt, node := range info.Entry.Pattern.StorageTargets {
				// Here we differ slightly from the old CTL. The old CTL method of getting target mappings
				// first determined the node ID, then used this to determine the alias. It was possible
				// to get the node ID but not the alias, then it would print out <unknown(nodeNumID)>.
				// The new target mapper works differently, looking up both the node ID and alias at once,
				// so if anything goes wrong we can't even print the node ID.
				if node == nil {
					fmt.Fprintf(entryToPrint, "+ %d @ <unknown>\n", tgt)
				} else {
					fmt.Fprintf(entryToPrint, "+ %d @ %s [ID: %d]\n", tgt, node.Alias, node.LegacyId.NumId)
				}

			}
		}
	}

	if len(info.Entry.Remote.Targets) != 0 {
		fmt.Fprintf(entryToPrint, "Remote Storage Target Details:\n")
		fmt.Fprintf(entryToPrint, "+ CoolDown: %d\n", info.Entry.Remote.CoolDownPeriod)
		fmt.Fprintf(entryToPrint, "+ Remote Storage Targets:\n")
		for rstID, rst := range info.Entry.Remote.Targets {
			if rst == nil {
				fmt.Fprintf(entryToPrint, "  + <not available> [ID: %d]\n", rstID)
			} else {
				fmt.Fprintf(entryToPrint, "  + %s [ID: %d]\n", rst.Name, rstID)
			}
		}
	}

	if frontendCfg.verbose {
		if info.Entry.Type.IsFile() {
			fmt.Fprintf(entryToPrint, "Chunk path: %s\n", info.Entry.Verbose.ChunkPath)
		}
		switch info.Entry.FeatureFlags.IsInlined() {
		case true:
			fmt.Fprintf(entryToPrint, "Inlined inode: yes\n")
		default:
			fmt.Fprintf(entryToPrint, "Inlined inode: no\n")
		}
		if info.Entry.EntryID != "root" && len(info.Entry.Verbose.DentryPath) != 0 {
			fmt.Fprintf(entryToPrint, "Dentry info:\n")
			fmt.Fprintf(entryToPrint, " + Path: %s\n", info.Entry.Verbose.DentryPath[1:])
			printMetaNodeInfo(info.Parent, true)
		}
		if !info.Entry.FeatureFlags.IsInlined() && len(info.Entry.Verbose.HashPath) != 0 {
			fmt.Fprintf(entryToPrint, "Inode info:\n")
			fmt.Fprintf(entryToPrint, " + Path: %s\n", info.Entry.Verbose.HashPath[1:])
			printMetaNodeInfo(info.Entry, true)
		}
		if info.Entry.Type == beegfs.EntryRegularFile {
			fmt.Fprintf(entryToPrint, "Client Sessions\n")
			fmt.Fprintf(entryToPrint, "+ Reading: %d\n+ Writing: %d\n", info.Entry.NumSessionsRead, info.Entry.NumSessionsWrite)
		}
	}
	return entryToPrint.String()
}

// TODO: https://github.com/thinkparq/ctl/issues/56
// Finish implementing (mainly just verbose output).
func assembleTableRow(info *entry.GetEntryCombinedInfo, rowLen int) []any {
	row := make([]any, 0, rowLen)

	row = append(row,
		info.Path,
		info.Entry.EntryID,
		info.Entry.Type,
		fmt.Sprintf("%s (%d)", info.Entry.MetaOwnerNode.Alias, info.Entry.MetaOwnerNode.Id.NumId),
	)

	if info.Entry.FeatureFlags.IsBuddyMirrored() {
		row = append(row, info.Entry.MetaBuddyGroup)
	} else {
		row = append(row, "(unmirrored)")
	}

	row = append(row, fmt.Sprintf("%s (%d)", info.Entry.Pattern.StoragePoolName, info.Entry.Pattern.StoragePoolID))

	if viper.GetBool(config.RawKey) {
		row = append(row, fmt.Sprintf("%s (%dx%d)", info.Entry.Pattern.Type, info.Entry.Pattern.DefaultNumTargets, info.Entry.Pattern.Chunksize))
	} else {
		row = append(row, fmt.Sprintf("%s (%dx%s)", info.Entry.Pattern.Type, info.Entry.Pattern.DefaultNumTargets, unitconv.FormatPrefix(float64(info.Entry.Pattern.Chunksize), unitconv.Base1024, 0)))
	}

	fmtTgtIDsFunc := func(targetIDs []uint16) string {
		var targetsBuilder strings.Builder
		for i, tgt := range targetIDs {
			if i > 0 {
				targetsBuilder.WriteString(",")
			}
			targetsBuilder.WriteString(fmt.Sprintf("%d", tgt))
		}
		return targetsBuilder.String()
	}

	if info.Entry.Type == beegfs.EntryDirectory {
		row = append(row, "(directory)", "(directory)")
	} else {
		if info.Entry.Pattern.Type == beegfs.StripePatternBuddyMirror {
			row = append(row, "(mirrored)", fmtTgtIDsFunc(info.Entry.Pattern.TargetIDs))
		} else {
			row = append(row, fmtTgtIDsFunc(info.Entry.Pattern.TargetIDs), "(unmirrored)")
		}
	}

	if len(info.Entry.Remote.Targets) != 0 {
		i := 0
		var rstBuilder strings.Builder
		for rstID, rst := range info.Entry.Remote.Targets {
			if i > 0 {
				rstBuilder.WriteString(",")
			}
			if rst == nil {
				rstBuilder.WriteString(fmt.Sprintf("%d-not-available", rstID))
			} else {
				rstBuilder.WriteString(fmt.Sprintf("%d-%s", rstID, rst.Name))
			}
			i++
		}
		row = append(row, rstBuilder.String(), fmt.Sprintf("%ds", info.Entry.Remote.CoolDownPeriod))
	} else {
		row = append(row, "(none)", "(n/a)")
	}

	if info.Entry.Type == beegfs.EntryRegularFile {
		row = append(row, fmt.Sprintf("Reading: %d, Writing: %d", info.Entry.NumSessionsRead, info.Entry.NumSessionsWrite))
	} else {
		row = append(row, "(directory)")
	}

	return row
}
