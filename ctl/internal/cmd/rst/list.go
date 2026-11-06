package rst

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type listCfg struct {
	showSecrets bool
	verbose     bool
	deleted     bool
}

// newListCmd is registered as "beegfs remote target list". "beegfs remote list" is
// newLegacyListCmd, which keeps the output of earlier releases.
func newListCmd() *cobra.Command {
	cfg := listCfg{}
	cmd := &cobra.Command{
		Use:   "list [<id>...]",
		Short: "List Remote Storage Targets and their configuration",
		Long: `List Remote Storage Targets and their configuration.

By default one row is printed per target. A value a target inherits from its backend is shown, and listed in the inherited column. Use --verbose, or name one or more target IDs, to print every setting of each target. With --output json, --verbose or target IDs print every column instead of the detail view. Use --columns all to get every column in the table.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ids := []uint32{}
			for _, arg := range args {
				id, err := strconv.ParseUint(arg, 10, 32)
				if err != nil {
					return fmt.Errorf("invalid remote target ID %q: %w", arg, err)
				}
				ids = append(ids, uint32(id))
			}
			return runListCmd(cmd, cfg, ids)
		},
	}
	cmd.Flags().BoolVar(&cfg.showSecrets, "show-secrets", false, "Print secret keys in cleartext instead of masked.")
	cmd.Flags().BoolVarP(&cfg.verbose, "verbose", "v", false, "Print every setting of each target.")
	cmd.Flags().BoolVar(&cfg.deleted, "deleted", false, "List the targets that were deleted, and why.")
	return cmd
}

func runListCmd(cmd *cobra.Command, cfg listCfg, ids []uint32) error {
	if cfg.deleted {
		return runListDeletedCmd(cmd)
	}

	targets, err := rst.GetTargets(cmd.Context())
	if err != nil {
		return err
	}
	for _, id := range ids {
		if _, ok := targets.EffectiveOf(id); !ok {
			return fmt.Errorf("remote target %d does not exist", id)
		}
	}
	shown := []shownTarget{}
	for _, effective := range targets.Effective {
		if len(ids) > 0 && !slices.Contains(ids, effective.GetId()) {
			continue
		}
		s, err := newShownTarget(effective, targets.Definitions[effective.GetId()], targets.Backends)
		if err != nil {
			return fmt.Errorf("remote target %d: %w", effective.GetId(), err)
		}
		shown = append(shown, s)
	}
	printShownTargets(shown, newTargetColumns(false), cfg.verbose || len(ids) > 0, cfg.showSecrets)
	return nil
}

// printShownTargets prints targets or backends in the output mode. With detail, the table mode
// prints the detail view of each one, and the JSON modes print every column.
func printShownTargets(shown []shownTarget, columns targetColumns, detail bool, showSecrets bool) {
	// The detail view is text. JSON output prints rows instead, with every column when the detail
	// view was asked for, so a script gets the same settings.
	structured := jsonOutput()
	if detail && !structured {
		for i, s := range shown {
			if i > 0 {
				fmt.Println()
			}
			printTargetDetail(s, showSecrets)
		}
		return
	}

	defaults := columns.defaults
	if detail {
		defaults = columns.all
	}
	tbl := cmdfmt.NewPrintomatic(columns.all, defaults)
	defer tbl.PrintRemaining()
	for _, s := range shown {
		tbl.AddItem(columns.row(s, showSecrets, structured)...)
	}
}

func runListDeletedCmd(cmd *cobra.Command) error {
	deleted, err := rst.GetDeletedTargets(cmd.Context())
	if err != nil {
		return err
	}
	columns := []string{"id", "name", "type", "deleted-at", "reason"}
	tbl := cmdfmt.NewPrintomatic(columns, columns)
	defer tbl.PrintRemaining()
	for _, d := range deleted {
		tbl.AddItem(d.GetId(), d.GetName(), d.GetType(), d.GetDeletedAt().AsTime().Format("2006-01-02 15:04:05 MST"), orDash(d.GetReason()))
	}
	return nil
}

// targetColumns are the columns of the target and backend tables:
//   - First the columns that say what the row is. For a target: id, name, type and backend. For a
//     backend: name, type, and the targets that name it.
//   - Then every setting and every keyed list any registered type declares (see rst.TargetType in
//     common/rst). The backend table leaves out what every type forbids on a backend, such as the
//     bucket. A row whose type lacks a column shows "-" in it.
//   - Then policies, and, for a target, what it inherits from its backend.
//
// The default columns are the leading ones, and the settings and lists their types mark as Summary.
type targetColumns struct {
	all        []string
	defaults   []string
	forBackend bool
	settings   []string
	lists      []string
}

func newTargetColumns(forBackend bool) targetColumns {
	c := targetColumns{forBackend: forBackend}
	settingsOf := cRst.TargetType.Settings
	listsOf := cRst.TargetType.Lists
	if forBackend {
		settingsOf = backendSettings
		listsOf = backendLists
	}
	for _, t := range cRst.TargetTypes() {
		for _, s := range settingsOf(t) {
			if !slices.Contains(c.settings, s.Key) {
				c.settings = append(c.settings, s.Key)
				if s.Summary {
					c.defaults = append(c.defaults, s.Key)
				}
			}
		}
	}
	defaultLists := []string{}
	for _, t := range cRst.TargetTypes() {
		for _, l := range listsOf(t) {
			if !slices.Contains(c.lists, l.Plural()) {
				c.lists = append(c.lists, l.Plural())
				if l.Summary() {
					defaultLists = append(defaultLists, l.Plural())
				}
			}
		}
	}
	// A backend names no backend and inherits nothing, so it has neither column.
	lead := []string{"id", "name", "type", "backend"}
	trail := []string{"policies", "inherited"}
	if forBackend {
		lead = []string{"name", "type", "targets"}
		trail = []string{"policies"}
	}
	c.all = slices.Concat(lead, c.settings, c.lists, trail)
	c.defaults = slices.Concat(lead, c.defaults, defaultLists)
	return c
}

// row returns the values of shown in the order of c.all. With structured, for JSON output, a keyed
// list is an array of entries (see listView), policies and inherited are objects, targets is an
// array of IDs, and a column the type lacks is null. Without it, every value is text for the table: a
// keyed list shows its keys, and a column the type lacks shows "-".
func (c targetColumns) row(shown shownTarget, showSecrets bool, structured bool) []any {
	target := shown.effective
	var row []any
	switch {
	case c.forBackend && structured:
		row = []any{target.GetName(), cRst.TypeName(target), shown.usedBy}
	case c.forBackend:
		row = []any{target.GetName(), cRst.TypeName(target), shown.usedByText()}
	case structured:
		row = []any{target.GetId(), target.GetName(), cRst.TypeName(target), shown.backend}
	default:
		row = []any{target.GetId(), target.GetName(), cRst.TypeName(target), orDash(shown.backend)}
	}

	t, typed := cRst.TargetTypeOf(target)
	var missing any = "-"
	if structured {
		missing = nil
	}
	for _, key := range c.settings {
		value := missing
		if typed {
			if i := slices.IndexFunc(t.Settings(), func(s cRst.Setting) bool { return s.Key == key }); i >= 0 {
				v := settingValue(t.Settings()[i], target, showSecrets)
				if structured {
					value = v
				} else {
					value = orDash(v)
				}
			}
		}
		row = append(row, value)
	}
	for _, plural := range c.lists {
		value := missing
		if typed {
			if i := slices.IndexFunc(t.Lists(), func(l cRst.KeyedList) bool { return l.Plural() == plural }); i >= 0 {
				if structured {
					value = listView(t.Lists()[i], target)
				} else {
					value = listKeys(t.Lists()[i], target)
				}
			}
		}
		row = append(row, value)
	}
	switch {
	case c.forBackend && structured:
		return append(row, policiesView(target.GetPolicies()))
	case c.forBackend:
		return append(row, formatPolicies(target.GetPolicies()))
	case structured:
		return append(row, policiesView(target.GetPolicies()), shown.inherited)
	default:
		return append(row, formatPolicies(target.GetPolicies()), shown.inheritedKeys())
	}
}

// newLegacyListCmd is "beegfs remote list". Scripts written for earlier releases parse its output,
// so it keeps their columns and values, and only those:
//   - The columns id, name, policies, type and configuration, all shown by default.
//   - policies is the text form of the policies message, also in JSON.
//   - configuration is every set field of the S3 part, with the secret key masked.
//
// Each row shows the effective configuration, so a value a target inherits from its backend is
// filled in, as Remote sends it to sync nodes. "beegfs remote target list" shows each setting in
// its own column and says what is inherited.
func newLegacyListCmd() *cobra.Command {
	cfg := rst.GetRSTCfg{}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List Remote Storage Targets and their configuration",
		Long: `List Remote Storage Targets and their configuration.

The output keeps the format of earlier releases, for scripts that parse it. Use 'beegfs remote target list' to show each setting in its own column, and what each target inherits from its backend.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runLegacyListCmd(cmd, cfg)
		},
	}
	cmd.Flags().BoolVar(&cfg.ShowSecrets, "show-secrets", false, "If secret keys should be printed in cleartext or masked (the default).")
	return cmd
}

func runLegacyListCmd(cmd *cobra.Command, cfg rst.GetRSTCfg) error {
	response, err := rst.GetRSTConfig(cmd.Context())
	if err != nil {
		return err
	}

	defaultColumns := []string{"id", "name", "policies", "type", "configuration"}
	tbl := cmdfmt.NewPrintomatic(defaultColumns, defaultColumns)
	defer tbl.PrintRemaining()
	sort.Slice(response.Rsts, func(i, j int) bool {
		return response.Rsts[i].Id < response.Rsts[j].Id
	})

	for _, target := range response.Rsts {
		if target.GetId() == cRst.JobBuilderRstId {
			continue
		}
		rstType, rstConfiguration := legacyTypeAndConfiguration(target, cfg.ShowSecrets)
		tbl.AddItem(
			target.GetId(),
			target.GetName(),
			target.GetPolicies().String(),
			rstType,
			rstConfiguration,
		)
	}
	return nil
}

// legacyTypeAndConfiguration returns the type and configuration columns of target, as earlier
// releases printed them.
func legacyTypeAndConfiguration(target *flex.RemoteStorageTarget, showSecrets bool) (string, string) {
	switch target.WhichType() {
	case flex.RemoteStorageTarget_S3_case:
		return "s3", formatLegacyS3Configuration(target.GetS3(), showSecrets)
	case flex.RemoteStorageTarget_Xtreemstore_case:
		xtreemstoreConfig := target.GetXtreemstore()
		if xtreemstoreConfig == nil {
			return "xtreemstore", "xtreemstore configuration is not set"
		}
		return "xtreemstore", formatLegacyS3Configuration(xtreemstoreConfig.GetS3(), showSecrets)
	default:
		if !showSecrets {
			return "unknown", "unknown configuration masked by default"
		}
		return "unknown", fmt.Sprintf("%v", target.GetType())
	}
}

// formatLegacyS3Configuration returns every set field of s3Config as "name: value", joined by ", ".
// The secret key is masked unless showSecrets is set.
func formatLegacyS3Configuration(s3Config *flex.RemoteStorageTarget_S3, showSecrets bool) string {
	if s3Config == nil {
		return "s3 configuration is not set"
	}
	fields := []string{}
	s3Config.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if string(fd.Name()) == "secret_key" && !showSecrets {
			fields = append(fields, fmt.Sprintf("%s: *****", fd.Name()))
		} else {
			fields = append(fields, fmt.Sprintf("%s: %s", fd.Name(), v))
		}
		return true
	})
	return strings.Join(fields, ", ")
}
