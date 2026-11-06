package rst

import (
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
)

// This file defines "beegfs remote bucket", which manages the buckets in the store of a backend
// (see backend.go). Remote runs each command against the store with the backend's settings, so the
// CLI host needs no route to the store (see common/rst/bucket.go).
//
// "bucket" is the name, because most remote stores in use speak S3. "container" is an alias, as the
// generic word for what other kinds of store call a share, an export, a directory, a volume or a
// disk.

func newBucketCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "bucket",
		Aliases: []string{"container"},
		Short:   "Create, modify, delete and list the buckets in the store of a backend",
		Long: `Create, modify, delete and list the buckets in the store of a backend.

A bucket lives in the store of one backend. Remote runs every command against the store with the backend's settings, such as its endpoint, region and credentials. A target that overrides its backend's endpoint keeps its objects in another store, which these commands do not reach.

"container" is an alias of "bucket", for stores that call the same thing a share, an export, a directory, a volume or a disk.`,
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(newBucketListCmd(), newBucketCreateCmd(), newBucketModifyCmd(), newBucketDeleteCmd())
	return cmd
}

// addBucketBackendFlag adds a required --backend to cmd. A bucket name is unique only within the
// store of one backend, and the backend supplies the connection.
func addBucketBackendFlag(cmd *cobra.Command, backend *string) {
	cmd.Flags().StringVar(backend, "backend", "", "The backend whose store holds the bucket.")
	cmd.MarkFlagRequired("backend")
}

// bucketSettingFlags are the flags that set a bucket's settings, on create and modify.
type bucketSettingFlags struct {
	versioning string
	tags       []string
	removeTags []string
	clearTags  bool
}

// register adds the flags to flags. With removals, it also adds --remove-tag and --clear-tags,
// which only modify has, because a new bucket has no tags to remove.
func (f *bucketSettingFlags) register(flags *pflag.FlagSet, removals bool) {
	flags.StringVar(&f.versioning, "versioning", "", "Keep old versions of objects: enabled or suspended. A store cannot turn versioning off once it was enabled.")
	flags.StringArrayVar(&f.tags, "tag", nil, "Set a tag on the bucket. Format: <key>=<value>. Repeat the flag for more tags.")
	if removals {
		flags.StringArrayVar(&f.removeTags, "remove-tag", nil, "Remove the tag with this key. Repeat the flag to remove more.")
		flags.BoolVar(&f.clearTags, "clear-tags", false, "Remove every tag before any --tag is applied.")
	}
}

// changes returns the changes the flags describe.
func (f *bucketSettingFlags) changes() (cRst.BucketChanges, error) {
	changes := cRst.BucketChanges{RemoveTags: f.removeTags, ClearTags: f.clearTags}
	switch strings.ToLower(f.versioning) {
	case "":
	case "enabled":
		changes.Versioning = beeremote.RSTBucket_ENABLED
	case "suspended":
		changes.Versioning = beeremote.RSTBucket_SUSPENDED
	default:
		return changes, fmt.Errorf("invalid --versioning %q (it must be enabled or suspended)", f.versioning)
	}
	tags, err := parseTags(f.tags)
	if err != nil {
		return changes, err
	}
	changes.SetTags = tags
	return changes, nil
}

// parseTags parses --tag values of the form <key>=<value>. The key is required, and may be given
// only once. The value may be empty, and may contain "=".
func parseTags(values []string) (map[string]string, error) {
	tags := map[string]string{}
	for _, raw := range values {
		key, value, ok := strings.Cut(raw, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid --tag %q (it must be <key>=<value>)", raw)
		}
		if _, ok := tags[key]; ok {
			return nil, fmt.Errorf("--tag %q is given more than once", key)
		}
		tags[key] = value
	}
	return tags, nil
}

func newBucketCreateCmd() *cobra.Command {
	var backend string
	var settings bucketSettingFlags
	var createTarget bool
	var targetID uint32
	var targetName string
	cmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a bucket in the store of a backend",
		Long: `Create a bucket in the store of a backend, in the backend's region.

With --create-target, also create a target for the new bucket. The target names the backend and sets only the bucket, so it inherits every other setting from the backend. If the bucket is created but the target is not, the bucket stays, and the command fails with the reason.`,
		Example: `  # A versioned bucket with a target that uses it
  beegfs remote bucket create beegfs-archive --backend=site --versioning=enabled --create-target --target-name=archive`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if !createTarget && (cmd.Flags().Changed("target-id") || cmd.Flags().Changed("target-name")) {
				return fmt.Errorf("--target-id and --target-name need --create-target")
			}
			changes, err := settings.changes()
			if err != nil {
				return err
			}
			// Build the target before the bucket exists, so a backend that cannot have one fails
			// before anything is created.
			var target *flex.RemoteStorageTarget
			if createTarget {
				if target, err = rst.NewBucketTarget(cmd.Context(), backend, name, targetID, targetName); err != nil {
					return err
				}
			}
			bucket, err := rst.CreateBucket(cmd.Context(), backend, name, changes)
			if err != nil {
				return err
			}
			if target == nil {
				if jsonOutput() {
					return cmdfmt.PrintJSON(map[string]any{"bucket": bucketView(backend, bucket)})
				}
				fmt.Printf("Created bucket %s in the store of remote backend %s.\n", name, backend)
				printBucketDetail(backend, bucket)
				return nil
			}

			resp, targetErr := rst.CreateTarget(cmd.Context(), target, false)
			if jsonOutput() {
				out := map[string]any{"bucket": bucketView(backend, bucket)}
				if targetErr == nil {
					out["target"] = createResult(shownAfterChange(cmd.Context(), resp.GetEffective(), resp.GetRst()), resp)
				}
				if err := cmdfmt.PrintJSON(out); err != nil {
					return err
				}
			} else {
				fmt.Printf("Created bucket %s in the store of remote backend %s.\n", name, backend)
				printBucketDetail(backend, bucket)
			}
			if targetErr != nil {
				return fmt.Errorf("bucket %q was created, but its target was not (hint: create it with 'beegfs remote target create'): %w", name, targetErr)
			}
			if jsonOutput() {
				return applyError(resp.GetApply())
			}
			fmt.Printf("\nCreated remote target %d (%s).\n", resp.GetRst().GetId(), resp.GetRst().GetName())
			printTargetDetail(shownAfterChange(cmd.Context(), resp.GetEffective(), resp.GetRst()), false)
			return printApplyResult(resp.GetApply())
		},
	}
	addBucketBackendFlag(cmd, &backend)
	settings.register(cmd.Flags(), false)
	cmd.Flags().BoolVar(&createTarget, "create-target", false, "Also create a target that uses the new bucket.")
	cmd.Flags().Uint32Var(&targetID, "target-id", 0, "The ID of the target --create-target creates. Omit it to let Remote assign one.")
	cmd.Flags().StringVar(&targetName, "target-name", "", "The name of the target --create-target creates.")
	return cmd
}

func newBucketModifyCmd() *cobra.Command {
	var backend string
	var settings bucketSettingFlags
	cmd := &cobra.Command{
		Use:   "modify <name>",
		Short: "Modify a bucket in the store of a backend",
		Long: `Modify the settings of a bucket in the store of a backend.

Only the settings given change. A store cannot turn versioning off once it was enabled, only suspend it. Suspended versioning keeps the old versions it already holds.`,
		Example: `  # Enable versioning, and replace every tag with one
  beegfs remote bucket modify beegfs-archive --backend=site --versioning=enabled --clear-tags --tag owner=hpc`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			changes, err := settings.changes()
			if err != nil {
				return err
			}
			if changes.IsEmpty() {
				return fmt.Errorf("nothing to change (hint: give --versioning, --tag, --remove-tag or --clear-tags)")
			}
			bucket, err := rst.UpdateBucket(cmd.Context(), backend, name, changes)
			if err != nil {
				return err
			}
			if jsonOutput() {
				return cmdfmt.PrintJSON(map[string]any{"bucket": bucketView(backend, bucket)})
			}
			fmt.Printf("Modified bucket %s in the store of remote backend %s.\n", name, backend)
			printBucketDetail(backend, bucket)
			return nil
		},
	}
	addBucketBackendFlag(cmd, &backend)
	settings.register(cmd.Flags(), true)
	return cmd
}

func newBucketDeleteCmd() *cobra.Command {
	var backend string
	var force bool
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a bucket from the store of a backend",
		Long: `Delete a bucket from the store of a backend.

A bucket that holds objects, old versions of objects, or unfinished multipart uploads is refused unless --force is given. With --force, every one of them is deleted first. That deletes the data in the bucket, and cannot be undone.

A bucket a target uses is refused, even with --force. Delete the target, or point it at another bucket, first. Stubs and jobs of a deleted target may still name the bucket, and are not checked.`,
		Example: `  # Delete an empty bucket
  beegfs remote bucket delete scratch --backend=site

  # Delete a bucket and every object in it
  beegfs remote bucket delete scratch --backend=site --force`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			resp, err := rst.DeleteBucket(cmd.Context(), backend, name, force)
			if err != nil {
				return err
			}
			if jsonOutput() {
				return cmdfmt.PrintJSON(map[string]any{
					"backend":         backend,
					"name":            name,
					"deleted-objects": resp.GetDeletedObjects(),
					"aborted-uploads": resp.GetAbortedUploads(),
				})
			}
			if resp.GetDeletedObjects() > 0 || resp.GetAbortedUploads() > 0 {
				fmt.Printf("Deleted %d objects and aborted %d uploads to empty the bucket.\n", resp.GetDeletedObjects(), resp.GetAbortedUploads())
			}
			fmt.Printf("Deleted bucket %s from the store of remote backend %s.\n", name, backend)
			return nil
		},
	}
	addBucketBackendFlag(cmd, &backend)
	cmd.Flags().BoolVar(&force, "force", false, "Delete every object, old object version and unfinished upload in the bucket, then the bucket.")
	return cmd
}

func newBucketListCmd() *cobra.Command {
	var backend string
	var verbose bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List the buckets in the stores of the backends",
		Long: `List the buckets in the stores of the backends, and the targets that use each one.

Without --backend, every backend whose type can manage buckets is listed. A backend whose store cannot be listed is reported, and the others are still listed. Keys that may only use their own buckets often may not list buckets at all.

--verbose also shows each bucket's versioning and tags, which costs requests per bucket.`,
		Example: `  beegfs remote bucket list --backend=site --verbose`,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			backends, err := rst.ListBuckets(cmd.Context(), backend, verbose)
			if err != nil {
				return err
			}
			columns := []string{"backend", "name", "created", "targets", "versioning", "tags"}
			defaults := columns[:4]
			if verbose {
				defaults = columns
			}
			structured := jsonOutput()
			failed := []string{}
			unread := 0
			tbl := cmdfmt.NewPrintomatic(columns, defaults)
			for _, b := range backends {
				if b.GetError() != "" {
					failed = append(failed, b.GetBackend())
					cmdfmt.Printf("Warning: unable to list the buckets of remote backend %s: %s\n", b.GetBackend(), b.GetError())
					continue
				}
				for _, bucket := range b.GetBuckets() {
					if !bucket.GetDetails() {
						unread++
					}
					tbl.AddItem(bucketRow(b.GetBackend(), bucket, structured)...)
				}
			}
			tbl.PrintRemaining()
			if verbose && unread > 0 {
				cmdfmt.Printf("Note: Remote could not read the versioning and tags of %d buckets, shown as '-'. Its log has the reason.\n", unread)
			}
			if len(failed) > 0 {
				return fmt.Errorf("unable to list the buckets of remote backends %s", strings.Join(failed, ", "))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&backend, "backend", "", "List only the buckets of this backend.")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Also show each bucket's versioning and tags.")
	return cmd
}

// bucketRow returns the values of bucket in the order of the list columns. With structured, for
// JSON output, targets is an array, tags is an object, and a detail that was not read is null.
func bucketRow(backend string, bucket *beeremote.RSTBucket, structured bool) []any {
	view := bucketView(backend, bucket)
	if structured {
		return []any{view["backend"], view["name"], view["created"], view["targets"], view["versioning"], view["tags"]}
	}
	versioning, tags := "-", "-"
	if bucket.GetDetails() {
		versioning = view["versioning"].(string)
		tags = orDash(formatTags(bucket.GetTags()))
	}
	return []any{backend, bucket.GetName(), orDash(formatCreated(bucket)), orDash(formatIDs(bucket.GetTargets())), versioning, tags}
}

// bucketView returns bucket for JSON output. Versioning and tags are null when they were not read.
func bucketView(backend string, bucket *beeremote.RSTBucket) map[string]any {
	targets := bucket.GetTargets()
	if targets == nil {
		targets = []uint32{}
	}
	view := map[string]any{
		"backend":    backend,
		"name":       bucket.GetName(),
		"created":    formatCreated(bucket),
		"targets":    targets,
		"versioning": nil,
		"tags":       nil,
	}
	if bucket.GetDetails() {
		view["versioning"] = strings.ToLower(bucket.GetVersioning().String())
		tags := bucket.GetTags()
		if tags == nil {
			tags = map[string]string{}
		}
		view["tags"] = tags
	}
	return view
}

// printBucketDetail prints one bucket as a block. Versioning and tags show "-" when Remote could
// not read them, and a note says where the reason is.
func printBucketDetail(backend string, bucket *beeremote.RSTBucket) {
	versioning, tags := "-", "-"
	if bucket.GetDetails() {
		versioning = strings.ToLower(bucket.GetVersioning().String())
		tags = orDash(formatTags(bucket.GetTags()))
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  backend:\t%s\n", backend)
	fmt.Fprintf(w, "  versioning:\t%s\n", versioning)
	fmt.Fprintf(w, "  tags:\t%s\n", tags)
	fmt.Fprintf(w, "  targets:\t%s\n", orDash(formatIDs(bucket.GetTargets())))
	w.Flush()
	if !bucket.GetDetails() {
		cmdfmt.Printf("Note: Remote could not read the versioning and tags of the bucket back. Its log has the reason.\n")
	}
}

func formatCreated(bucket *beeremote.RSTBucket) string {
	if !bucket.HasCreated() {
		return ""
	}
	return bucket.GetCreated().AsTime().Format(time.RFC3339)
}

func formatTags(tags map[string]string) string {
	pairs := []string{}
	for _, key := range slices.Sorted(maps.Keys(tags)) {
		pairs = append(pairs, key+"="+tags[key])
	}
	return strings.Join(pairs, ",")
}

func formatIDs(ids []uint32) string {
	text := []string{}
	for _, id := range ids {
		text = append(text, strconv.FormatUint(uint64(id), 10))
	}
	return strings.Join(text, ", ")
}
