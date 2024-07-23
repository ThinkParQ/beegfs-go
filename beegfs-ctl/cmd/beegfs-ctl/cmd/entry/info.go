package entry

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/dsnet/golib/unitconv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/entry"
	"github.com/thinkparq/gobee/beegfs"
	"github.com/thinkparq/gobee/types"
)

type entryInfoCfg struct {
	stdinDelimiter string
	recurse        bool
	retro          bool
	retroPaths     bool
	verbose        bool
	// When printing multiple rows flushing each row will result in misaligned columns. Instead
	// buffer multiple rows and print them all at once along with their header. The flushInterval
	// determines how "real time" the output is and how often the header is reprinted.
	flushInterval int
}

func newEntryInfoCmd() *cobra.Command {

	frontendCfg := entryInfoCfg{}
	backendCfg := entry.GetEntriesConfig{}
	cmd := &cobra.Command{
		Use:   "info <path> [<path>] ...",
		Short: "Get details about one or more entries in BeeGFS.",
		Long: `Get details about one or more entries in BeeGFS.

Specifying Paths:
When supported by the current shell, standard wildcards (globbing patterns) can be used in each path to return info about multiple entries.
Alternatively multiple entries can be provided using stdin by specifying '-' as the path (example: 'cat file_list.txt | beegfs entry set -').
		`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			} else if len(args) > 1 && frontendCfg.recurse {
				return fmt.Errorf("only one path can be specified when recursively printing entries")
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
	cmd.Flags().BoolVar(&frontendCfg.verbose, "verbose", false, "Print more information about each entry (such as chunk and dentry paths on the servers).")
	cmd.Flags().BoolVar(&frontendCfg.retro, "retro", false, "Display entry information using the \"retro\" vertical style of the original beegfs-ctl.")
	// The same default used for stdin-delimiter to allow the help output to print correctly can't
	// be used directly. If the default changes update where this is set in getDelimiterFromString.
	cmd.Flags().StringVar(&frontendCfg.stdinDelimiter, "stdin-delimiter", "\n", "Change the string delimiter used to determine individual paths when read from stdin (e.g., --stdin-delimiter=\"\\x00\" for NULL).")
	cmd.Flags().BoolVar(&frontendCfg.retroPaths, "retro-print-paths", false, "Print paths at the top of each entry in the retro output.")
	cmd.Flags().MarkHidden("retro-print-paths")
	cmd.Flags().IntVar(&frontendCfg.flushInterval, "flush-interval", 1000, "Set the number of lines to buffer before flushing output and reprinting the header. Increasing this number can improve alignment but decrease real-time responsiveness. Set to zero to disable printing headers.")
	return cmd
}

func runEntryInfoCmd(cmd *cobra.Command, args []string, frontendCfg entryInfoCfg, backendCfg entry.GetEntriesConfig) error {

	// Setup the method for sending paths to the backend:
	stdinErrChan := make(chan error, 1)
	if args[0] == "-" {
		pathsChan := make(chan string, 1024)
		backendCfg.PathsViaChan = pathsChan
		d, err := getDelimiterFromString(frontendCfg.stdinDelimiter)
		if err != nil {
			return err
		}
		readPathsFromStdin(cmd.Context(), d, pathsChan, stdinErrChan)
	} else if frontendCfg.recurse {
		backendCfg.PathsViaRecursion = args[0]
	} else {
		backendCfg.PathsViaList = args
	}

	entriesChan, errChan, err := entry.GetEntries(cmd.Context(), backendCfg)
	if err != nil {
		return err
	}

	w := cmdfmt.NewDeprecatedTableWriter(os.Stdout)
	defer w.Flush()
	var multiErr types.MultiError
	// When printing a table, print the header initially then only when flushing the buffer.
	printHeader := true
	// If the interval is zero don't ever print the header.
	if frontendCfg.flushInterval == 0 {
		printHeader = false
	}
	count := 0

run:
	for {
		count++
		select {
		case info, ok := <-entriesChan:
			if !ok {
				break run
			}
			// Still try and print as much information as possible if verbose details are not available.
			if len(info.Entry.Verbose.Err.Errors) != 0 {
				fmt.Fprintf(&w, "warning: unable to generate all output needed to print verbose details, some information may be missing: %s (ignoring)\n", info.Entry.Verbose.Err.Error())
			}
			if frontendCfg.retro {
				printRetro(&w, info, frontendCfg)
			} else {
				printTable(&w, info, printHeader, frontendCfg)
				printHeader = false
			}
			if frontendCfg.flushInterval > 0 && count%frontendCfg.flushInterval == 0 {
				printHeader = true
				w.Flush()
			} else if frontendCfg.flushInterval == 0 {
				w.Flush()
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

func printRetro(w *tabwriter.Writer, info *entry.GetEntryCombinedInfo, frontendCfg entryInfoCfg) {

	if frontendCfg.retroPaths {
		fmt.Fprintf(w, "Path: %s\n", info.Path)
	}
	fmt.Fprintf(w, "Entry type: %s\n", info.Entry.Type)
	fmt.Fprintf(w, "EntryID: %s\n", info.Entry.EntryID)
	if info.Entry.Type == beegfs.EntryDirectory && frontendCfg.verbose {
		fmt.Fprintf(w, "ParentID: %s\n", info.Entry.ParentEntryID)
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
			fmt.Fprintf(w, "%sMetadata buddy group: %d\n", frontText, entry.MetaBuddyGroup)
			fmt.Fprintf(w, "%sCurrent primary metadata node: %s [ID: %d]\n", frontText, entry.MetaOwnerNode.Alias, entry.MetaOwnerNode.Id.NumId)
		} else {
			fmt.Fprintf(w, "%sMetadata node: %s [ID: %d]\n", frontText, entry.MetaOwnerNode.Alias, entry.MetaOwnerNode.Id.NumId)
		}
	}

	if !frontendCfg.verbose {
		printMetaNodeInfo(info.Entry, false)
	}

	fmt.Fprintf(w, "Stripe pattern details:\n")
	fmt.Fprintf(w, "+ Type: %s\n", info.Entry.Pattern.Type)
	if viper.GetBool(config.RawKey) {
		fmt.Fprintf(w, "+ Chunksize: %d\n", info.Entry.Pattern.Chunksize)
	} else {
		fmt.Fprintf(w, "+ Chunksize: %s\n", unitconv.FormatPrefix(float64(info.Entry.Pattern.Chunksize), unitconv.Base1024, 0))
	}
	fmt.Fprintf(w, "+ Number of storage targets: ")
	actualNumStorageTgts := len(info.Entry.Pattern.TargetIDs)
	if actualNumStorageTgts == 0 {
		// Expected if this is a directory.
		fmt.Fprintf(w, "desired: %d\n", info.Entry.Pattern.DefaultNumTargets)
	} else {
		fmt.Fprintf(w, "desired: %d; actual: %d\n", info.Entry.Pattern.DefaultNumTargets, actualNumStorageTgts)
	}

	if info.Entry.Type == beegfs.EntryDirectory {
		fmt.Fprintf(w, "+ Storage Pool: %d  (%s)\n", info.Entry.Pattern.StoragePoolID, info.Entry.Pattern.StoragePoolName)
	}

	if actualNumStorageTgts != 0 {
		if info.Entry.Pattern.Type == beegfs.StripePatternBuddyMirror {
			fmt.Fprintf(w, "+ Storage mirror buddy groups:\n")
			for _, tgt := range info.Entry.Pattern.TargetIDs {
				fmt.Fprintf(w, "  + %d\n", tgt)
			}
		} else {
			fmt.Fprintf(w, "+ Storage targets:\n")
			for tgt, node := range info.Entry.Pattern.StorageTargets {
				// Here we differ slightly from the old CTL. The old CTL method of getting target mappings
				// first determined the node ID, then used this to determine the alias. It was possible
				// to get the node ID but not the alias, then it would print out <unknown(nodeNumID)>.
				// The new target mapper works differently, looking up both the node ID and alias at once,
				// so if anything goes wrong we can't even print the node ID.
				if node == nil {
					fmt.Fprintf(w, "+ %d @ <unknown>\n", tgt)
				} else {
					fmt.Fprintf(w, "+ %d @ %s [ID: %d]\n", tgt, node.Alias, node.LegacyId.NumId)
				}

			}
		}
	}

	if len(info.Entry.Remote.Targets) != 0 {
		fmt.Fprintf(w, "Remote Storage Target Details:\n")
		fmt.Fprintf(w, "+ CoolDown: %d\n", info.Entry.Remote.CoolDownPeriod)
		fmt.Fprintf(w, "+ Remote Storage Targets:\n")
		for rstID, rst := range info.Entry.Remote.Targets {
			if rst == nil {
				fmt.Fprintf(w, "  + <not available> [ID: %d]\n", rstID)
			} else {
				fmt.Fprintf(w, "  + %s [ID: %d]\n", rst.Name, rstID)
			}
		}
	}

	if frontendCfg.verbose {
		if info.Entry.Type.IsFile() {
			fmt.Fprintf(w, "Chunk path: %s\n", info.Entry.Verbose.ChunkPath)
		}
		switch info.Entry.FeatureFlags.IsInlined() {
		case true:
			fmt.Fprintf(w, "Inlined inode: yes\n")
		default:
			fmt.Fprintf(w, "Inlined inode: no\n")
		}
		if info.Entry.EntryID != "root" && len(info.Entry.Verbose.DentryPath) != 0 {
			fmt.Fprintf(w, "Dentry info:\n")
			fmt.Fprintf(w, " + Path: %s\n", info.Entry.Verbose.DentryPath[1:])
			printMetaNodeInfo(info.Parent, true)
		}
		if !info.Entry.FeatureFlags.IsInlined() && len(info.Entry.Verbose.HashPath) != 0 {
			fmt.Fprintf(w, "Inode info:\n")
			fmt.Fprintf(w, " + Path: %s\n", info.Entry.Verbose.HashPath[1:])
			printMetaNodeInfo(info.Entry, true)
		}
		if info.Entry.Type == beegfs.EntryRegularFile {
			fmt.Fprintf(w, "Client Sessions\n")
			fmt.Fprintf(w, "+ Reading: %d\n+ Writing: %d\n", info.Entry.NumSessionsRead, info.Entry.NumSessionsWrite)
		}
	}
	fmt.Fprintf(w, "\n")
}

// TODO: https://github.com/ThinkParQ/beegfs-ctl/issues/56
// Finish implementing (mainly just verbose output).
func printTable(w *tabwriter.Writer, info *entry.GetEntryCombinedInfo, printHeader bool, frontendCfg entryInfoCfg) {
	if printHeader {
		fmt.Fprintf(w, "Path\tEntry ID\tType\tMeta Node\tMeta Mirror\tStorage Pool\tStripe Pattern\tStorage Targets\tBuddy Groups\tRemote Targets\tCool Down")
		if frontendCfg.verbose {
			fmt.Fprintf(w, "\tClient Sessions")
		}
		fmt.Fprintf(w, "\n")
	}
	entryRow := fmt.Sprintf("%s\t", info.Path)
	entryRow += fmt.Sprintf("%s\t%s\t", info.Entry.EntryID, info.Entry.Type)
	entryRow += fmt.Sprintf("%s (%d)\t", info.Entry.MetaOwnerNode.Alias, info.Entry.MetaOwnerNode.Id.NumId)

	if info.Entry.FeatureFlags.IsBuddyMirrored() {
		entryRow += fmt.Sprintf("%d\t", info.Entry.MetaBuddyGroup)
	} else {
		entryRow += "(unmirrored)\t"
	}

	entryRow += fmt.Sprintf("%s (%d)\t", info.Entry.Pattern.StoragePoolName, info.Entry.Pattern.StoragePoolID)

	if viper.GetBool(config.RawKey) {
		entryRow += fmt.Sprintf("%s (%dx%d)\t", info.Entry.Pattern.Type, info.Entry.Pattern.DefaultNumTargets, info.Entry.Pattern.Chunksize)
	} else {
		entryRow += fmt.Sprintf("%s (%dx%s)\t", info.Entry.Pattern.Type, info.Entry.Pattern.DefaultNumTargets, unitconv.FormatPrefix(float64(info.Entry.Pattern.Chunksize), unitconv.Base1024, 0))
	}

	if info.Entry.Type != beegfs.EntryDirectory {
		if info.Entry.Pattern.Type == beegfs.StripePatternBuddyMirror {
			entryRow += "(mirrored)\t"
			numCommas := len(info.Entry.Pattern.TargetIDs) - 1
			for i, tgt := range info.Entry.Pattern.TargetIDs {
				entryRow += fmt.Sprintf("%d", tgt)
				if i < numCommas {
					entryRow += ","
				}
			}
			entryRow += "\t"
		} else {
			numCommas := len(info.Entry.Pattern.TargetIDs) - 1
			for i, tgt := range info.Entry.Pattern.TargetIDs {
				entryRow += fmt.Sprintf("%d", tgt)
				if i < numCommas {
					entryRow += ","
				}
			}
			entryRow += "\t(unmirrored)\t"
		}
	} else {
		entryRow += "(directory)\t(directory)\t"
	}

	if len(info.Entry.Remote.Targets) != 0 {
		i := 0
		numCommas := len(info.Entry.Remote.Targets) - 1
		for rstID, rst := range info.Entry.Remote.Targets {
			if rst == nil {
				entryRow += fmt.Sprintf("%d-not-available", rstID)
			} else {
				entryRow += fmt.Sprintf("%d-%s", rstID, rst.Name)
			}
			if i < numCommas {
				entryRow += ","
			}
			i++
		}
		entryRow += fmt.Sprintf("\t%ds", info.Entry.Remote.CoolDownPeriod)
	} else {
		entryRow += "(none)\t(n/a)"
	}

	if frontendCfg.verbose {
		if info.Entry.Type == beegfs.EntryRegularFile {
			entryRow += fmt.Sprintf("\tReading: %d, Writing: %d", info.Entry.NumSessionsRead, info.Entry.NumSessionsWrite)
		} else {
			entryRow += "\tN/A"
		}
	}

	fmt.Fprintf(w, "%s\n", entryRow)
}
