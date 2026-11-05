package rst

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/spf13/cobra"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
)

type listCfg struct {
	showSecrets bool
	verbose     bool
	deleted     bool
}

// newListCmd is registered as "beegfs remote target list" and, for compatibility, as "beegfs
// remote list".
func newListCmd() *cobra.Command {
	cfg := listCfg{}
	cmd := &cobra.Command{
		Use:   "list [<id>...]",
		Short: "List Remote Storage Targets and their configuration",
		Long: `List Remote Storage Targets and their configuration.

By default one row is printed per target. A value a target inherits from a parent is shown, and listed in the inherited column. Use --verbose, or name one or more target IDs, to print every setting of each target. With --output json, --verbose or target IDs print every column instead of the detail view. Use --columns all to get every column in the table.`,
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
		s, err := newShownTarget(effective, targets.Definitions[effective.GetId()], targets.Parents)
		if err != nil {
			return fmt.Errorf("remote target %d: %w", effective.GetId(), err)
		}
		shown = append(shown, s)
	}
	printShownTargets(shown, newTargetColumns(false), cfg.verbose || len(ids) > 0, cfg.showSecrets)
	return nil
}

// printShownTargets prints targets or parents in the output mode. With detail, the table mode
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

// targetColumns are the columns of the target and parent tables:
//   - First the columns that say what the row is. For a target: id, name, type and parent. For a
//     parent: name, type, parent and children.
//   - Then every setting and every keyed list any registered type declares (see rst.TargetType in
//     common/rst). A row whose type lacks a column shows "-" in it.
//   - Then policies, and what the row inherits from its parents.
//
// The default columns are the leading ones, and the settings and lists their types mark as Summary.
type targetColumns struct {
	all       []string
	defaults  []string
	forParent bool
	settings  []string
	lists     []string
}

func newTargetColumns(forParent bool) targetColumns {
	c := targetColumns{forParent: forParent}
	for _, t := range cRst.TargetTypes() {
		for _, s := range t.Settings() {
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
		for _, l := range t.Lists() {
			if !slices.Contains(c.lists, l.Plural()) {
				c.lists = append(c.lists, l.Plural())
				if l.Summary() {
					defaultLists = append(defaultLists, l.Plural())
				}
			}
		}
	}
	// A parent names no parent and inherits nothing, so it has neither column.
	lead := []string{"id", "name", "type", "parent"}
	trail := []string{"policies", "inherited"}
	if forParent {
		lead = []string{"name", "type", "children"}
		trail = []string{"policies"}
	}
	c.all = slices.Concat(lead, c.settings, c.lists, trail)
	c.defaults = slices.Concat(lead, c.defaults, defaultLists)
	return c
}

// row returns the values of shown in the order of c.all. With structured, for JSON output, a keyed
// list is an array of entries (see listView), policies and inherited are objects, children is an
// object, and a column the type lacks is null. Without it, every value is text for the table: a
// keyed list shows its keys, and a column the type lacks shows "-".
func (c targetColumns) row(shown shownTarget, showSecrets bool, structured bool) []any {
	target := shown.effective
	var row []any
	switch {
	case c.forParent && structured:
		row = []any{target.GetName(), cRst.TypeName(target), map[string]any{"targets": shown.childTargets}}
	case c.forParent:
		row = []any{target.GetName(), cRst.TypeName(target), shown.children()}
	case structured:
		row = []any{target.GetId(), target.GetName(), cRst.TypeName(target), shown.parent}
	default:
		row = []any{target.GetId(), target.GetName(), cRst.TypeName(target), orDash(shown.parent)}
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
	case c.forParent && structured:
		return append(row, policiesView(target.GetPolicies()))
	case c.forParent:
		return append(row, formatPolicies(target.GetPolicies()))
	case structured:
		return append(row, policiesView(target.GetPolicies()), shown.inherited)
	default:
		return append(row, formatPolicies(target.GetPolicies()), shown.inheritedKeys())
	}
}
