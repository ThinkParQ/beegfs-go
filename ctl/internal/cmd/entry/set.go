package entry

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/types"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
)

type entrySetCfg struct {
	recurse            bool
	stdinDelimiter     string
	confirmBulkUpdates bool
	verbose            bool
}

func newEntrySetCmd() *cobra.Command {

	frontendCfg := entrySetCfg{}
	backendCfg := entry.SetEntryCfg{}

	cmd := &cobra.Command{
		Use:   "set <path> [<path>] ...",
		Short: "Configure stripe patterns, storage pools, remote storage targets, and more",
		Long: `Configure stripe patterns, storage pools, remote storage targets, and more. 
New configurations will apply only to new files and sub-directories of the specified path(s), with the exception of Remote Storage Targets,
which can be updated for existing files at any time. Enable the --verbose flag to view detailed configuration changes for each entry.

Specifying Paths:
When supported by the current shell, standard wildcards (globbing patterns) can be used in each path to update multiple directories at once.
Alternatively multiple entries can be provided using stdin by specifying '-' as the path (example: 'cat file_list.txt | beegfs entry set -').
WARNING: When updating multiple entries, non-directory entries will be silently ignored.

Required Permissions:
This mode can only be used by non-root users if administrators have enabled the "sysAllowUserSetPattern" option in the metadata server config. 
This enables normal users to change the default number of targets and chunksize for directories they own. All other options can only be changed by root.`,
		Annotations: map[string]string{"authorization.AllowAllUsers": ""},
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf("missing <path> argument. Usage: %s", cmd.Use)
			}
			return nil
		},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Early return if set-stub is not specified
			if backendCfg.StubStatus == nil {
				return nil
			}

			// Flags that are allowed to be used with set-stub.
			allowedFlags := []string{"set-stub", "verbose", "yes", "recurse"}

			// Initialize a list to track any disallowed flags.
			disallowedFlags := []string{}

			cmd.Flags().VisitAll(func(flag *pflag.Flag) {
				// Only add flags that have been explicitly changed and are not allowed with set-stub flag.
				if flag.Changed && !slices.Contains(allowedFlags, flag.Name) {
					disallowedFlags = append(disallowedFlags, flag.Name)
				}
			})

			// Return an error if any disallowed flags are used with set-stub.
			if len(disallowedFlags) > 0 {
				return fmt.Errorf("--set-stub can't be used with the following flag(s): %s", strings.Join(disallowedFlags, ", "))
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {

			return runEntrySetCmd(cmd.Context(), args, frontendCfg, backendCfg)
		},
	}

	// Frontend / display configuration options:
	cmd.Flags().BoolVar(&frontendCfg.recurse, "recurse", false, `When  <path> is a single directory recursively update all supported configuration for entries beneath the path.
	CAUTION: this may update many entries, for example if the BeeGFS root is the provided path.`)
	// The same default used for stdin-delimiter to allow the help output to print correctly can't
	// be used directly. If the default changes update where this is set in getDelimiterFromString.
	cmd.Flags().StringVar(&frontendCfg.stdinDelimiter, "stdin-delimiter", "\\n", `Change the string delimiter used to determine individual paths when read from stdin.
	For example use --stdin-delimiter=\"\\x00\" for NULL.`)
	cmd.Flags().BoolVar(&frontendCfg.verbose, "verbose", false, "Print what configuration was updated for each entry.")

	// Entry options
	cmd.Flags().Var(newChunksizeFlag(&backendCfg.Chunksize), "chunk-size", "Block size for striping (per storage target). Suffixes 'ki' (Kibibytes) and 'Mi` (Mebibytes) are allowed.")
	cmd.Flags().Var(newPoolFlag(&backendCfg.Pool), "pool", `Use the specified storage pool for all new files in this directory. 
	Can be specified as the alias, numerical ID, or unique ID of the pool.
	NOTE: This is an enterprise feature. See end-user license agreement for definition and usage.`)
	cmd.Flags().Var(newStripePatternFlag(&backendCfg.StripePattern), "pattern", fmt.Sprintf(`Set the stripe pattern type to use. Valid patterns: %s.
	When the pattern is set to "mirrored", each target will be mirrored on a corresponding mirror target.
	NOTE: Buddy mirroring is an enterprise feature. See end-user license agreement for definition and usage.`, strings.Join(validStripePatternKeys(), ", ")))
	cmd.Flags().Var(newNumTargetsFlag(&backendCfg.DefaultNumTargets), "num-targets", `Number of targets to stripe each file across.
	If the stripe pattern is "mirrored" this is the number of mirror groups.`)
	cmd.Flags().VarP(newRstsFlag(&backendCfg.RemoteTargets), "remote-targets", "r", `Comma-separated list of Remote Storage Target IDs.
	All desired IDs must be specified. Specify 'none' to unset all RSTs.`)
	cmd.Flags().Var(newRstCooldownFlag(&backendCfg.RemoteCooldownSecs), "remote-cooldown", "Time to wait after a file is closed before replication begins. Accepts a duration such as 1s, 1m, or 1h. The max duration is 65,535 seconds.")
	// TODO: https://github.com/ThinkParQ/bee-remote/issues/18
	// Unmark this as hidden once automatic uploads are supported.
	cmd.Flags().MarkHidden("remote-cooldown")
	// Advanced options
	cmd.Flags().BoolVar(&backendCfg.Force, "force", false, "Allow some configuration checks to be overridden.")
	cmd.Flags().Var(newStubStatusFlag(&backendCfg.StubStatus), "set-stub", "Set or clear the stub flag for regular files. If not specified, the stub status remains unchanged.")
	cmd.Flags().MarkHidden("set-stub")
	cmd.Flags().BoolVar(&frontendCfg.confirmBulkUpdates, "yes", false, "Use to acknowledge when running this command may update a large number of entries.")
	// IMPORTANT: When adding new flags or updating flag names update the help function below.
	return cmd
}

func runEntrySetCmd(ctx context.Context, args []string, frontendCfg entrySetCfg, backendCfg entry.SetEntryCfg) error {

	// Setup the method for sending paths to the backend:
	if frontendCfg.recurse {
		if !frontendCfg.confirmBulkUpdates {
			return fmt.Errorf("the recurse mode updates the specified entry and ALL child entries, if you're sure this is what you want add the --yes flag")
		}
	}
	method, err := entry.DetermineInputMethod(args, frontendCfg.recurse, frontendCfg.stdinDelimiter)
	if err != nil {
		return err
	}

	entriesChan, errChan, err := entry.SetEntries(ctx, method, backendCfg)
	if err != nil {
		return err
	}

	// The table is only used for printing verbose output and tbl.PrintRemaining() is only called at
	// the end when running in verbose mode (to avoid the headers printing out). If this is ever
	// used to print other output adjust how/where tbl.PrintRemaining() is called as needed.
	allColumns := []string{"path", "status", "configuration updates"}
	tbl := cmdfmt.NewPrintomatic(allColumns, allColumns)
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
				tbl.AddItem(result.Path, result.Status, configUpdates)
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
	}
	cmdfmt.Printf("Summary: processed %d entries | configuration updates: %s\n", count-1, sprintfNewEntryConfig(backendCfg))
	// We may have still processed some entries so wait to print an error until the end.
	if len(multiErr.Errors) != 0 {
		return &multiErr
	}
	return nil
}

// sprintfNewEntryConfig() combines all non-nil/empty fields from the newConfig into a comma
// separated string. This is meant for printing what configuration updates were applied.
func sprintfNewEntryConfig(newConfig entry.SetEntryCfg) string {
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
		if field.Kind() == reflect.Ptr && !field.IsNil() && fieldType.Name != "actorEUID" {
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
