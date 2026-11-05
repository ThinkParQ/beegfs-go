package rst

import (
	"fmt"
	"math"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
)

// targetFlags holds the raw values of the flags that describe an RST. Flags fall in two groups:
//   - Common flags (--name, --policies) set the fields every RST type has. Targets also have
//     --parent (registerParent). Parents do not, because a parent names no parent.
//   - Type flags set the settings and keyed lists an RST type declares (see rst.TargetType in
//     common/rst). They are built from the declaration, so a new type needs no code here.
//
// A type flag is registered once per name, however many types have it. Types that share a name
// share its meaning, which the registry tests in common/rst check.
//
// changes turns the flags the admin set into a backend.TargetChanges. A flag the admin did not set
// changes nothing, which is what lets modify keep every other setting of the target.
type targetFlags struct {
	name     string
	parent   string
	policies string
	// settings maps a setting key to its flag value.
	settings map[string]*string
	// secretFiles maps the key of a secret setting to the value of its --<key>-file flag.
	secretFiles map[string]*string
	// lists maps a keyed list's flag to its declaration and the values of its flags.
	lists map[string]*listFlags
}

type listFlags struct {
	list    cRst.KeyedList
	entries []string
	removes []string
	clear   bool
}

const policiesHelp = "Set policies that apply to any target type. Format: <key>=<value>[,...]. Keys: fast-start-max-size=<size> (for example 1GiB)."

func newTargetFlags() *targetFlags {
	return &targetFlags{
		settings:    map[string]*string{},
		secretFiles: map[string]*string{},
		lists:       map[string]*listFlags{},
	}
}

func (f *targetFlags) registerCommon(flags *pflag.FlagSet) {
	flags.StringVar(&f.name, "name", "", "A name for the target, shown in logs and output.")
	flags.StringVar(&f.policies, "policies", "", policiesHelp)
}

// registerParent registers --parent. Only target commands call it.
func (f *targetFlags) registerParent(flags *pflag.FlagSet) {
	flags.StringVar(&f.parent, "parent", "", "The parent to inherit every unset setting from. It must have the same type. On modify, --parent= removes the parent and keeps the inherited values as the target's own.")
}

// registerTypes registers the settings and lists of every type in types. With removals, it also
// registers the --remove and --clear flags of each list, which only modify has, because a new target
// has nothing to remove. When types holds more than one type, a flag only some of them have says so
// in its help, such as "(xtreemstore only)".
func (f *targetFlags) registerTypes(flags *pflag.FlagSet, types []cRst.TargetType, removals bool) {
	onlyFor := func(name string) string {
		with := backend.TargetTypesWith(name)
		if len(types) < 2 || len(with) == len(types) {
			return ""
		}
		return " (" + strings.Join(with, " and ") + " only)"
	}

	for _, t := range types {
		for _, s := range t.Settings() {
			if _, ok := f.settings[s.Key]; ok {
				continue
			}
			value := new(string)
			f.settings[s.Key] = value
			help := s.Help
			if s.Secret {
				help += fmt.Sprintf(" It is visible to other users in the process list and in shell history, so prefer --%s-file.", s.Key)
			}
			flags.StringVar(value, s.Key, "", help+onlyFor(s.Key))
			if s.Secret {
				path := new(string)
				f.secretFiles[s.Key] = path
				flags.StringVar(path, s.Key+"-file", "", fmt.Sprintf("Read --%s from this file. A trailing newline is ignored.%s", s.Key, onlyFor(s.Key)))
				cobra.MarkFlagFilename(flags, s.Key+"-file")
			}
		}
		for _, l := range t.Lists() {
			if _, ok := f.lists[l.Flag()]; ok {
				continue
			}
			lf := &listFlags{list: l}
			f.lists[l.Flag()] = lf
			only := onlyFor(l.Flag())
			flags.StringArrayVar(&lf.entries, l.Flag(), nil, fmt.Sprintf("%s Format: %s.%s", l.Help(), cRst.ListFormat(l), only))
			if removals {
				flags.StringArrayVar(&lf.removes, "remove-"+l.Flag(), nil, fmt.Sprintf("Remove the %s with this %s. Repeat the flag to remove more.%s", l.Flag(), l.Key().Key, only))
				flags.BoolVar(&lf.clear, "clear-"+l.Plural(), false, fmt.Sprintf("Remove every %s before any --%s is applied.%s", l.Flag(), l.Flag(), only))
			}
		}
	}
}

// changes returns the changes the flags set on cmd describe.
func (f *targetFlags) changes(cmd *cobra.Command) (backend.TargetChanges, error) {
	changed := cmd.Flags().Changed
	changes := backend.TargetChanges{
		Settings: map[string]string{},
		Lists:    map[string]backend.ListChanges{},
	}
	if changed("name") {
		changes.Name = &f.name
	}
	if changed("parent") {
		changes.Parent = &f.parent
	}
	if changed("policies") {
		policies, err := parsePolicies(f.policies)
		if err != nil {
			return changes, err
		}
		changes.Policies = policies
	}

	for key, value := range f.settings {
		if changed(key) {
			changes.Settings[key] = *value
		}
	}
	for key, path := range f.secretFiles {
		if !changed(key + "-file") {
			continue
		}
		if changed(key) {
			return changes, fmt.Errorf("--%s and --%s-file cannot both be given", key, key)
		}
		b, err := os.ReadFile(*path)
		if err != nil {
			return changes, fmt.Errorf("unable to read the --%s-file: %w", key, err)
		}
		changes.Settings[key] = strings.TrimRight(string(b), "\r\n")
	}

	for flag, lf := range f.lists {
		listChanges := backend.ListChanges{Clear: lf.clear, Remove: lf.removes}
		for _, raw := range lf.entries {
			entry, err := parseListEntry(lf.list, raw)
			if err != nil {
				return changes, err
			}
			listChanges.Upsert = append(listChanges.Upsert, entry)
		}
		if listChanges.Clear || len(listChanges.Remove) > 0 || len(listChanges.Upsert) > 0 {
			changes.Lists[flag] = listChanges
		}
	}
	return changes, nil
}

// parseListEntry parses one value of a keyed list flag, such as "name=GLACIER,recheck-time=30m". The
// list's key field is required. The other fields are kept as text for the type to parse.
func parseListEntry(list cRst.KeyedList, raw string) (backend.ListEntryChange, error) {
	allowed := []string{list.Key().Key}
	for _, field := range list.Fields() {
		allowed = append(allowed, field.Key)
	}
	values, err := parseKeyValues(raw, allowed...)
	if err != nil {
		return backend.ListEntryChange{}, fmt.Errorf("invalid --%s %q: %w", list.Flag(), raw, err)
	}
	key, ok := values[list.Key().Key]
	if !ok || key == "" {
		return backend.ListEntryChange{}, fmt.Errorf("invalid --%s %q: %s is required", list.Flag(), raw, list.Key().Key)
	}
	delete(values, list.Key().Key)
	return backend.ListEntryChange{Key: key, Values: values}, nil
}

// parseKeyValues splits "k1=v1,k2=v2" into a map. It refuses a key that is not in allowed, a key
// given twice, and an entry without "=". Values cannot contain commas.
func parseKeyValues(input string, allowed ...string) (map[string]string, error) {
	values := map[string]string{}
	for _, pair := range strings.Split(input, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, value, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("%q is not in the form <key>=<value>", pair)
		}
		key = strings.TrimSpace(key)
		if !slices.Contains(allowed, key) {
			return nil, fmt.Errorf("unknown key %q (allowed keys: %s)", key, strings.Join(allowed, ", "))
		}
		if _, ok := values[key]; ok {
			return nil, fmt.Errorf("key %q is given more than once", key)
		}
		values[key] = strings.TrimSpace(value)
	}
	return values, nil
}

func parsePolicies(input string) (backend.PolicyChanges, error) {
	policies := backend.PolicyChanges{}
	values, err := parseKeyValues(input, "fast-start-max-size")
	if err != nil {
		return policies, fmt.Errorf("invalid --policies: %w", err)
	}
	if v, ok := values["fast-start-max-size"]; ok {
		size, err := util.ParseIntFromStr(v)
		if err != nil {
			return policies, fmt.Errorf("invalid --policies fast-start-max-size: %w", err)
		}
		if size > math.MaxInt64 {
			return policies, fmt.Errorf("invalid --policies fast-start-max-size: %s is too large", v)
		}
		s := int64(size)
		policies.FastStartMaxSize = &s
	}
	return policies, nil
}
