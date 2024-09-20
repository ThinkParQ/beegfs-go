package entry

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/thinkparq/beegfs-go/beegfs-ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/ctl/entry"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/types"
)

type entrySetCfg struct {
	recurse            bool
	stdinDelimiter     string
	confirmBulkUpdates bool
	verbose            bool
}

func newEntrySetCmd() *cobra.Command {

	frontendCfg := entrySetCfg{}
	backendCfg := entry.SetEntriesConfig{
		NewConfig: entry.SetEntryConfig{},
	}

	cmd := &cobra.Command{
		Use:   "set <path> [<path>] ...",
		Short: "Configure stripe patterns, storage pools, remote storage targets, and more.",
		Long: `Configure stripe patterns, storage pools, remote storage targets, and more. 
New configurations will apply only to new files and sub-directories of the specified path(s), with the exception of Remote Storage Targets,
which can be updated for existing files at any time. Enable the --verbose flag to view detailed configuration changes for each entry.

Specifying Paths:
When supported by the current shell, standard wildcards (globbing patterns) can be used in each path to update multiple directories at once.
Alternatively multiple entries can be provided using stdin by specifying '-' as the path (example: 'cat file_list.txt | beegfs entry set -').
WARNING: When updating multiple entries, non-directory entries will be silently ignored.

Required Permissions:
This mode can only be used by non-root users if administrators have enabled the "sysAllowUserSetPattern" option in the metadata server config. 
This enables normal users to change the default number of targets and chunksize for directories they own. All other options can only be changed by root.
				`,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			} else if len(args) > 1 && frontendCfg.recurse {
				return fmt.Errorf("only one path can be specified when recursively updating entries")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {

			return runEntrySetCmd(cmd, args, frontendCfg, backendCfg)
		},
	}

	// IMPORTANT: When adding new flags or updating flag names update the help function below.
	cmd.SetHelpFunc(runEntrySetHelp)

	// Frontend / display configuration options:
	cmd.Flags().BoolVar(&frontendCfg.recurse, "recurse", false, `When  <path> is a single directory recursively update all supported configuration for entries beneath the path.
				CAUTION: this may update many entries, for example if the BeeGFS root is the provided path.`)
	// The same default used for stdin-delimiter to allow the help output to print correctly can't
	// be used directly. If the default changes update where this is set in getDelimiterFromString.
	cmd.Flags().StringVar(&frontendCfg.stdinDelimiter, "stdin-delimiter", "\\n", `Change the string delimiter used to determine individual paths when read from stdin.
				For example use --stdin-delimiter=\"\\x00\" for NULL.`)
	cmd.Flags().BoolVar(&frontendCfg.verbose, "verbose", false, "Print what configuration was updated for each entry.")

	// Entry options
	cmd.Flags().Var(newChunksizeFlag(&backendCfg.NewConfig.Chunksize), "chunksize", "Block size for striping (per storage target). Suffixes 'Ki' (Kibibytes) and 'Mi` (Mebibytes) are allowed.")
	cmd.Flags().Var(newPoolFlag(&backendCfg.NewConfig.Pool), "pool", `Use the specified storage pool for all new files in this directory. 
				Can be specified as the alias, numerical ID, or unique ID of the pool.
				NOTE: This is an enterprise feature. See end-user license agreement for definition and usage.`)
	cmd.Flags().Var(newStripePatternFlag(&backendCfg.NewConfig.StripePattern), "pattern", fmt.Sprintf(`Set the stripe pattern type to use. Valid patterns: %s.
				When the pattern is set to "buddymirror", each target will be mirrored on a corresponding mirror target.
				NOTE: Buddy mirroring is an enterprise feature. See end-user license agreement for definition and usage.`, strings.Join(validStripePatternKeys(), ", ")))
	cmd.Flags().Var(newNumTargetsFlag(&backendCfg.NewConfig.DefaultNumTargets), "num-targets", `Number of targets to stripe each file across.
				If the stripe pattern is 'buddymirror' this is the number of mirror groups.`)
	cmd.Flags().VarP(newRstsFlag(&backendCfg.NewConfig.RemoteTargets), "remote-targets", "r", `Comma-separated list of Remote Storage Target IDs.
				All desired IDs must be specified. Specify 'none' to unset all RSTs.`)
	cmd.Flags().Var(newRstCooldownFlag(&backendCfg.NewConfig.RemoteCooldownSecs), "remote-cooldown", "Time to wait after a file is closed before replication begins. Accepts a duration such as 1s, 1m, or 1h. The max duration is 65,535 seconds.")
	// TODO: https://github.com/ThinkParQ/bee-remote/issues/18
	// Unmark this as hidden once automatic uploads are supported.
	cmd.Flags().MarkHidden("remote-cooldown")
	// Advanced options
	cmd.Flags().BoolVar(&backendCfg.NewConfig.Force, "force", false, "Allow some configuration checks to be overridden.")
	cmd.Flags().BoolVar(&frontendCfg.confirmBulkUpdates, "yes", false, "Use to acknowledge when running this command may update a large number of entries.")
	// IMPORTANT: When adding new flags or updating flag names update the help function below.
	return cmd
}

// Currently Cobra does not have a way to group flags into sections which makes the default help for
// this command hard to read. This custom help function groups flags into logical sections. When
// this PR (https://github.com/spf13/cobra/pull/2117) is merged we could get rid of it and use flag
// groups instead. WARNING: New flags are not automatically printed and must be added below.
func runEntrySetHelp(cmd *cobra.Command, args []string) {
	w := tabwriter.Writer{}
	w.Init(os.Stdout, 0, 0, 2, ' ', 0)
	printFlagsHelp := func(cmd *cobra.Command, flags []string) {
		for _, flagName := range flags {
			flag := cmd.Flags().Lookup(flagName)
			if flag != nil && !flag.Hidden {
				fmt.Fprintf(&w, "\t\t\t--%s: %s\t%s (%s)\n", flag.Name, flag.Value.Type(), flag.Usage, flag.DefValue)
			}
		}
	}

	fmt.Fprintf(&w, "Usage: %s\n\n", cmd.UseLine())
	fmt.Fprintf(&w, cmd.Long)
	fmt.Fprintf(&w, "\nFlags:\n")

	fmt.Fprintf(&w, "\nEntry Options:\n")
	entryFlags := sort.StringSlice{"chunksize", "num-targets", "pattern", "pool", "remote-cooldown", "remote-targets"}
	entryFlags.Sort()
	printFlagsHelp(cmd, entryFlags)

	fmt.Fprintf(&w, "\nInput Options:\n")
	inputFlags := sort.StringSlice{"recurse", "stdin-delimiter"}
	inputFlags.Sort()
	printFlagsHelp(cmd, inputFlags)

	fmt.Fprintf(&w, "\nAdvanced Options:\n")
	advancedFlags := sort.StringSlice{"force", "yes"}
	advancedFlags.Sort()
	printFlagsHelp(cmd, advancedFlags)

	fmt.Fprintf(&w, "\nGlobal options:\n")
	cmd.InheritedFlags().VisitAll(func(flag *pflag.Flag) {
		if !flag.Hidden {
			fmt.Fprintf(&w, "\t\t\t--%s: %s\t%s (%s)\n", flag.Name, flag.Value.Type(), flag.Usage, flag.DefValue)
		}
	})
	w.Flush()
}

func runEntrySetCmd(cmd *cobra.Command, args []string, frontendCfg entrySetCfg, backendCfg entry.SetEntriesConfig) error {

	actorEUID := syscall.Geteuid()
	backendCfg.NewConfig.ActorEUID = &actorEUID

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
		if !frontendCfg.confirmBulkUpdates {
			return fmt.Errorf("the recurse mode updates the specified entry and ALL child entries, if you're sure this is what you want add the --yes flag")
		}
		backendCfg.PathsViaRecursion = args[0]
	} else {
		backendCfg.PathsViaList = args
	}

	entriesChan, errChan, err := entry.SetEntries(cmd.Context(), backendCfg)
	if err != nil {
		return err
	}

	// The table is only used for printing verbose output and tbl.PrintRemaining() is only called at
	// the end when running in verbose mode (to avoid the headers printing out). If this is ever
	// used to print other output adjust how/where tbl.PrintRemaining() is called as needed.
	allColumns := []string{"path", "status", "configuration updates"}
	tbl := cmdfmt.NewTableWrapper(allColumns, allColumns)
	var multiErr types.MultiError
	count := 0

run:
	for {
		// Count is always one more than the number of entries actually processed.
		count++
		select {
		case result, ok := <-entriesChan:
			if !ok {
				break run
			}
			if frontendCfg.verbose {
				configUpdates := "None"
				if result.Status == beegfs.OpsErr_SUCCESS {
					configUpdates = sprintfNewEntryConfig(result.Updates)
				}
				tbl.Row(result.Path, result.Status, configUpdates)
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

	if frontendCfg.verbose {
		tbl.PrintRemaining()
		fmt.Printf("Processed %d entries.\n", count-1)
	} else {
		fmt.Printf("Processed %d entries.\nConfiguration Updates: %s\n", count-1, sprintfNewEntryConfig(backendCfg.NewConfig))
	}
	// We may have still processed some entries so wait to print an error until the end.
	if len(multiErr.Errors) != 0 {
		return &multiErr
	}
	return nil
}

// sprintfNewEntryConfig() combines all non-nil/empty fields from the newConfig into a comma
// separated string. This is meant for printing what configuration updates were applied.
func sprintfNewEntryConfig(newConfig entry.SetEntryConfig) string {
	val := reflect.ValueOf(newConfig)
	typ := reflect.TypeOf(newConfig)
	// We can't specify the size of the slice upfront because we don't know how many fields
	// actually changed until we loop over the updates. The only reason we do it this way is to
	// join the resulting strings with commas. If we didn't care to do that we could just print
	// out each item instead of aggregating them and printing the whole line at once.
	line := make([]string, 0)
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)
		// Only include if the field is a pointer and not nil
		if field.Kind() == reflect.Ptr && !field.IsNil() && fieldType.Name != "ActorEUID" {
			line = append(line, fmt.Sprintf("%s (%v)", fieldType.Name, field.Elem()))
		}
		// Only include non-empty slices:
		if field.Kind() == reflect.Slice && !field.IsNil() && field.Len() > 0 {
			elems := make([]string, field.Len())
			for j := 0; j < field.Len(); j++ {
				elems[j] = fmt.Sprintf("%v", field.Index(j))
			}
			line = append(line, fmt.Sprintf("%s (%v)", fieldType.Name, strings.Join(elems, ", ")))
		} else if field.Kind() == reflect.Slice && !field.IsNil() {
			// A non-nil empty slice means it was explicitly cleared (set to none).
			line = append(line, fmt.Sprintf("%s (none)", fieldType.Name))
		}
	}
	return strings.Join(line, ", ")
}
