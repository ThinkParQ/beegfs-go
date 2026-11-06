package rst

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/beeremote"
)

// This file defines "beegfs remote backend". A backend is the named connection to one remote
// store, such as an S3 endpoint and its credentials, that targets of the same type inherit from
// (see common/rst/backend.go). Each type decides which of its settings a backend must set, and
// which it must not set (see rst.BackendRole). The commands reuse the target flags (targetflags.go)
// and output (targetformat.go), without the settings the type forbids on a backend. A backend
// names no backend, so its commands have no --backend flag.
//
// Two things contact the store, and nothing else does:
//   - The --verify flag of create and modify. Remote checks the new definition from its own host
//     before it stores it, and refuses the change when the check fails.
//   - The verify command. It checks a stored backend from Remote and from every sync node.

// defaultVerifyTimeout is the --timeout of verify. It matches Remote's default, so the flag's help
// shows what an omitted timeout means.
const defaultVerifyTimeout = 30 * time.Second

func newBackendCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backend",
		Short: "Create, modify, delete, verify and list the backends Remote Storage Targets connect through",
		Long: `Create, modify, delete, verify and list the backends Remote Storage Targets connect through.

A backend is the connection to one remote store, such as an S3 endpoint and the credentials to use it. It is not a target: no job uses it, and it has no ID. A target of the same type names it with --backend, and every setting the target does not set itself is taken from the backend. A target may set any setting itself, including one the backend must set, and its own value wins. A backend cannot name a backend.

Each type decides what a backend holds:
` + backendRolesHelp(cRst.TargetTypes()) + `
Every other setting is optional, and is a default for the targets that name the backend.

Remote stores only what each target and backend sets. A change to a backend reaches every target that names it at once, as a modify of each target would. List entries a target sets, such as a storage class with the same name, override the backend's.`,
		Args: cobra.NoArgs,
	}
	cmd.AddCommand(newBackendListCmd(), newBackendCreateCmd(), newBackendModifyCmd(), newBackendDeleteCmd(), newBackendVerifyCmd())
	return cmd
}

// backendRolesHelp describes, for each type, the settings a backend must set and those it cannot
// set, one line per type, each from backendRolesOf.
func backendRolesHelp(types []cRst.TargetType) string {
	var b strings.Builder
	for _, t := range types {
		fmt.Fprintf(&b, "  %s: %s\n", t.Name(), backendRolesOf(t))
	}
	return b.String()
}

// backendRolesOf describes the backend roles of t as one sentence or two, such as
// "must set endpoint-url. It cannot set bucket.".
func backendRolesOf(t cRst.TargetType) string {
	required, forbidden := []string{}, []string{}
	for _, s := range t.Settings() {
		switch s.Backend {
		case cRst.BackendRequired:
			required = append(required, s.Key)
		case cRst.BackendForbidden:
			forbidden = append(forbidden, s.Key)
		}
	}
	for _, l := range t.Lists() {
		if l.BackendForbidden() {
			forbidden = append(forbidden, l.Plural())
		}
	}
	parts := []string{}
	if len(required) > 0 {
		parts = append(parts, "must set "+strings.Join(required, ", ")+".")
	}
	if len(forbidden) > 0 {
		parts = append(parts, "It cannot set "+strings.Join(forbidden, ", ")+".")
	}
	if len(parts) == 0 {
		return "may leave every setting unset."
	}
	return strings.Join(parts, " ")
}

// backendExample returns a "remote backend create" command for t, built from the settings t
// requires of a backend, followed by a target that names it. A secret is given through its file
// flag, which the help recommends.
func backendExample(t cRst.TargetType) string {
	args := []string{"--name=site"}
	for _, s := range t.Settings() {
		if s.Backend != cRst.BackendRequired {
			continue
		}
		if s.Secret {
			args = append(args, fmt.Sprintf("--%s-file=/etc/beegfs/site.%s", s.Key, s.Key))
		} else {
			args = append(args, fmt.Sprintf("--%s=<%s>", s.Key, s.Key))
		}
	}
	targetArgs := []string{"--name=archive", "--backend=site"}
	for _, s := range t.Settings() {
		if s.Backend == cRst.BackendForbidden && s.RequiredOnCreate {
			targetArgs = append(targetArgs, fmt.Sprintf("--%s=<%s>", s.Key, s.Key))
		}
	}
	return fmt.Sprintf(`  # The connection every %[1]s target of one store shares, checked from Remote first
  beegfs remote backend create %[1]s --verify %[2]s

  # Check that every sync node reaches the store too
  beegfs remote backend verify site

  # A target that adds only what each target sets itself
  beegfs remote target create %[1]s %[3]s`, t.Name(), strings.Join(args, " "), strings.Join(targetArgs, " "))
}

// verifyFlagHelp is the help of the --verify flag of create and modify.
const verifyFlagHelp = `Before storing the change, Remote checks that the backend reaches its store from Remote, and refuses the change when it does not. Sync nodes are not checked. Run "remote backend verify" for them.`

// verifyNoteView returns the note of a --verify that passed for JSON output, so a script can tell a
// verified change from one that was not verified. It is nil without --verify.
func verifyNoteView(verified bool, note string) any {
	if !verified {
		return nil
	}
	return map[string]any{"remote": "ok", "message": note}
}

// printVerifyNote prints the outcome of a --verify that passed, in the table mode.
func printVerifyNote(verified bool, note string) {
	if !verified {
		return
	}
	if note == "" {
		fmt.Println("Verified from Remote.")
		return
	}
	fmt.Printf("Verified from Remote: %s.\n", note)
}

func newBackendCreateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "create <type>",
		Short: "Create a backend",
		Long: `Create a backend of the given type.

--name is required, and must not belong to another backend. The settings the type requires of a backend are required too. Every other setting is optional.

A list entry, such as a storage class, must be complete, with every field a target would need. Targets inherit an entry whole, and never merge its fields with their own.`,
		Args: cobra.NoArgs,
	}
	for _, t := range cRst.TargetTypes() {
		cmd.AddCommand(newBackendCreateTypeCmd(t))
	}
	return cmd
}

func newBackendCreateTypeCmd(t cRst.TargetType) *cobra.Command {
	flags := newTargetFlags()
	var verify bool
	cmd := &cobra.Command{
		Use:     t.Name(),
		Short:   fmt.Sprintf("Create an %s backend", t.Name()),
		Long:    fmt.Sprintf("Create an %s backend.\n\nAn %s backend %s", t.Name(), t.Name(), backendRolesOf(t)),
		Example: backendExample(t),
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := checkRequiredFlags(cmd, t, isBackendRequired, ""); err != nil {
				return err
			}
			changes, err := flags.changes(cmd)
			if err != nil {
				return err
			}
			backend, err := rst.NewBackend(t.Name())
			if err != nil {
				return err
			}
			backend, err = rst.ApplyTargetChanges(backend, changes)
			if err != nil {
				return err
			}
			resp, err := rst.CreateBackend(cmd.Context(), backend, verify)
			if err != nil {
				return err
			}
			shown, err := shownBackendAfterChange(cmd.Context(), resp.GetBackend().GetName())
			if err != nil {
				return err
			}
			if jsonOutput() {
				out := map[string]any{"backend": targetView(shown, false)}
				if v := verifyNoteView(verify, resp.GetVerifyNote()); v != nil {
					out["verify"] = v
				}
				return cmdfmt.PrintJSON(out)
			}
			printVerifyNote(verify, resp.GetVerifyNote())
			fmt.Printf("Created remote backend %s.\n", shown.effective.GetName())
			printTargetDetail(shown, false)
			return nil
		},
	}
	flags.registerCommon(cmd.Flags(), "backend")
	cmd.MarkFlagRequired("name")
	flags.registerTypes(cmd.Flags(), []cRst.TargetType{t}, false, true)
	cmd.Flags().BoolVar(&verify, "verify", false, verifyFlagHelp)
	return cmd
}

func isBackendRequired(s cRst.Setting) bool { return s.Backend == cRst.BackendRequired }

func newBackendModifyCmd() *cobra.Command {
	flags := newTargetFlags()
	var verify bool
	var force bool
	cmd := &cobra.Command{
		Use:   "modify <name>",
		Short: "Modify a backend",
		Long: `Modify a backend.

Settings change as they do for "remote target modify". A setting the type requires of a backend cannot be cleared. --name renames the backend, and every target that names it is changed to the new name.

The change reaches every target that names the backend. It is checked for each of them as a modify of that target would be. Changing where a target keeps its objects, such as its endpoint, is refused while unfinished jobs use the target, unless --force is given.`,
		Example: `  # Rotate the credentials of every target that inherits them, after checking the new ones
  beegfs remote backend modify site --verify --access-key=AKIANEW --secret-key-file=/etc/beegfs/site.secret.new`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			changes, err := flags.changes(cmd)
			if err != nil {
				return err
			}
			updated, resp, err := rst.UpdateBackend(cmd.Context(), args[0], changes, force, verify)
			if err != nil {
				return err
			}
			shown, err := shownBackendAfterChange(cmd.Context(), updated.GetName())
			if err != nil {
				return err
			}
			if jsonOutput() {
				out := backendChangeResult(targetView(shown, false), resp.GetAffectedTargets(), resp.GetApply())
				if v := verifyNoteView(verify, resp.GetVerifyNote()); v != nil {
					out["verify"] = v
				}
				if err := cmdfmt.PrintJSON(out); err != nil {
					return err
				}
				return applyError(resp.GetApply())
			}
			printVerifyNote(verify, resp.GetVerifyNote())
			fmt.Printf("Modified remote backend %s. %s\n", updated.GetName(), describeAffected(resp.GetAffectedTargets()))
			printTargetDetail(shown, false)
			return printApplyResult(resp.GetApply())
		},
	}
	flags.registerCommon(cmd.Flags(), "backend")
	flags.registerTypes(cmd.Flags(), cRst.TargetTypes(), true, true)
	cmd.Flags().BoolVar(&force, "force", false, "Apply the change even when it moves where a target that names the backend keeps its objects while unfinished jobs use it.")
	cmd.Flags().BoolVar(&verify, "verify", false, verifyFlagHelp)
	return cmd
}

func newBackendDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a backend",
		Long: `Delete a backend.

The delete is refused while any target names the backend, and the error lists those targets. First give each one another backend with "remote target modify <id> --backend=<name>", or remove its backend with "remote target modify <id> --backend=", which keeps the values it inherited as its own. So a delete never changes a target.`,
		Example: `  # Keep the values target 3 inherits, then delete the backend it named
  beegfs remote target modify 3 --backend=
  beegfs remote backend delete site`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if err := rst.DeleteBackend(cmd.Context(), name); err != nil {
				return err
			}
			if jsonOutput() {
				return cmdfmt.PrintJSON(map[string]any{"name": name})
			}
			fmt.Printf("Deleted remote backend %s.\n", name)
			return nil
		},
	}
	return cmd
}

func newBackendVerifyCmd() *cobra.Command {
	var timeout time.Duration
	cmd := &cobra.Command{
		Use:   "verify <name>",
		Short: "Check that a backend reaches its store from Remote and every sync node",
		Long: `Check that a backend reaches its store from Remote and every sync node.

Each host sends one read-only request to the store with the backend's settings. For S3, that lists at most one bucket. A host passes when the store answers and accepts the credentials. Keys that may not list buckets still pass, with a note, because the store checked them before it refused.

Run it after creating or changing a backend, and when a sync node is added, to check the hosts that move data. The check says nothing about any bucket or target. It exits non-zero when Remote or a connected sync node fails. A node that is not connected, or too old to verify, is listed and does not fail the check.`,
		Example: `  beegfs remote backend verify site`,
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			resp, err := rst.VerifyBackend(cmd.Context(), args[0], timeout)
			if err != nil {
				return err
			}
			if jsonOutput() {
				if err := cmdfmt.PrintJSON(verifyView(resp)); err != nil {
					return err
				}
				return verifyError(resp)
			}
			return printVerifyResult(resp)
		},
	}
	cmd.Flags().DurationVar(&timeout, "timeout", defaultVerifyTimeout, "How long each host may take.")
	return cmd
}

// verifyView returns a verify result for JSON output: one entry per host, with the outcome in lower
// case, as applyView does for a change.
func verifyView(resp *beeremote.VerifyRSTBackendResponse) map[string]any {
	hosts := []map[string]string{}
	for _, h := range resp.GetHosts() {
		hosts = append(hosts, map[string]string{
			"host":    h.GetHost(),
			"outcome": strings.ToLower(h.GetOutcome().String()),
			"message": h.GetMessage(),
		})
	}
	return map[string]any{"hosts": hosts}
}

// verifyError returns an error when Remote or a connected node failed the verify, so the command
// exits non-zero in every output mode. Offline and unsupported nodes are not failures, the same as
// for a change (see applyError).
func verifyError(resp *beeremote.VerifyRSTBackendResponse) error {
	failed := []string{}
	for _, h := range resp.GetHosts() {
		if h.GetOutcome() == beeremote.VerifyRSTBackendResponse_FAILED {
			failed = append(failed, h.GetHost())
		}
	}
	if len(failed) > 0 {
		return fmt.Errorf("the backend failed the check on %s", strings.Join(failed, ", "))
	}
	return nil
}

// printVerifyResult prints one row per host, then a summary line. It returns verifyError.
func printVerifyResult(resp *beeremote.VerifyRSTBackendResponse) error {
	ok := 0
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "HOST\tOUTCOME\tMESSAGE")
	for _, h := range resp.GetHosts() {
		if h.GetOutcome() == beeremote.VerifyRSTBackendResponse_OK {
			ok++
		}
		fmt.Fprintf(w, "%s\t%s\t%s\n", h.GetHost(), strings.ToLower(h.GetOutcome().String()), orDash(h.GetMessage()))
	}
	w.Flush()
	fmt.Printf("Verified on %d of %d hosts.\n", ok, len(resp.GetHosts()))
	return verifyError(resp)
}

func newBackendListCmd() *cobra.Command {
	var showSecrets bool
	var verbose bool
	cmd := &cobra.Command{
		Use:   "list [<name>...]",
		Short: "List backends and their settings",
		Long: `List backends and their settings.

By default one row is printed per backend. Each row shows the backend's settings and the targets that name it. Use --verbose, or name one or more backends, to print every setting of each backend. With --output json, --verbose or names print every column instead of the detail view.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			targets, err := rst.GetTargets(cmd.Context())
			if err != nil {
				return err
			}
			names := args
			if len(names) == 0 {
				names = slices.Sorted(maps.Keys(targets.Backends))
			}
			shown := []shownTarget{}
			for _, name := range names {
				s, err := newShownBackend(targets, name)
				if err != nil {
					return err
				}
				shown = append(shown, s)
			}
			printShownTargets(shown, newTargetColumns(true), verbose || len(args) > 0, showSecrets)
			return nil
		},
	}
	cmd.Flags().BoolVar(&showSecrets, "show-secrets", false, "Print secret keys in cleartext instead of masked.")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Print every setting of each backend.")
	return cmd
}

// shownBackendAfterChange reads the backends again and returns the backend called name for output,
// with the targets that name it.
func shownBackendAfterChange(ctx context.Context, name string) (shownTarget, error) {
	targets, err := rst.GetTargets(ctx)
	if err != nil {
		return shownTarget{}, fmt.Errorf("the change was made, but the backend could not be read back: %w", err)
	}
	return newShownBackend(targets, name)
}

// backendChangeResult returns the JSON output of a backend modify: the backend, the targets whose
// effective configuration changed, and how the change reached each sync node.
func backendChangeResult(backend any, affected []uint32, apply *beeremote.ApplyResult) map[string]any {
	if affected == nil {
		affected = []uint32{}
	}
	return map[string]any{
		"backend":          backend,
		"affected-targets": affected,
		"apply":            applyView(apply),
	}
}

// describeAffected says which targets a backend change reached, as a sentence.
func describeAffected(affected []uint32) string {
	if len(affected) == 0 {
		return "No target changed."
	}
	ids := []string{}
	for _, id := range affected {
		ids = append(ids, strconv.FormatUint(uint64(id), 10))
	}
	return fmt.Sprintf("Changed remote targets %s.", strings.Join(ids, ", "))
}
