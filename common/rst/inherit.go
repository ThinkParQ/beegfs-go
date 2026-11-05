package rst

import (
	"fmt"
	"slices"
	"strings"

	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

// This file resolves parents. A parent is a named, partial configuration of one RST type that
// targets inherit from. It is not an RST: it has no ID and no provider, and no stub or job names it.
// A target names its parent in its parent field. A parent names no parent, so there is one level:
// a value comes from the target or from its parent, and no rule for precedence or loops is needed.
//
// Terms used here:
//   - The definition of a target or parent is what is stored for it: only the values it sets itself.
//   - The effective configuration of a target is its definition with every value it leaves unset
//     taken from its parent. Providers are built from it.
//
// Rules for what a configuration inherits. "Unset" is the zero value, because the generated proto3
// fields cannot tell an unset value from a zero one:
//   - A setting is inherited when the configuration's value is "".
//   - fast-start-max-size is inherited when it is 0.
//   - A keyed list is inherited per entry. Every entry whose key the configuration lacks is copied
//     whole from the parent. An entry with the same key replaces the parent's entry whole: its
//     fields are never merged with the parent's.
//   - id, name and parent are never inherited.
//
// So a configuration cannot clear a value its parent sets, or remove an entry its parent has.

// Overlay returns over with every value it leaves unset taken from base. base is usually over's
// parent. Neither argument is modified. Both must have the same type, and the type must be
// registered, because the type's declaration says what its settings and lists are.
func Overlay(base, over *flex.RemoteStorageTarget) (*flex.RemoteStorageTarget, error) {
	if base.WhichType() != over.WhichType() {
		return nil, fmt.Errorf("a %s configuration cannot inherit from a %s parent", TypeName(over), TypeName(base))
	}
	t, ok := TargetTypeOf(over)
	if !ok {
		return nil, fmt.Errorf("%s configurations cannot have parents", TypeName(over))
	}
	result := proto.Clone(over).(*flex.RemoteStorageTarget)

	if result.GetPolicies().GetFastStartMaxSize() == 0 && base.GetPolicies().GetFastStartMaxSize() != 0 {
		if !result.HasPolicies() {
			result.SetPolicies(&flex.RemoteStorageTarget_Policies{})
		}
		result.GetPolicies().SetFastStartMaxSize(base.GetPolicies().GetFastStartMaxSize())
	}
	for _, s := range t.Settings() {
		if s.Get(result) != "" {
			continue
		}
		if v := s.Get(base); v != "" {
			if err := s.Set(result, v); err != nil {
				return nil, fmt.Errorf("unable to inherit %s: %w", s.Key, err)
			}
		}
	}
	for _, list := range t.Lists() {
		own := listEntryKeys(list, result)
		for _, entry := range list.Entries(base) {
			if slices.Contains(own, entry.Key) {
				continue
			}
			if err := list.CopyEntry(result, base, entry.Key); err != nil {
				return nil, fmt.Errorf("unable to inherit %s %q: %w", list.Flag(), entry.Key, err)
			}
		}
	}
	return result, nil
}

// parentOf returns the parent config names, or nil when it names none. parents maps each parent's
// name to its definition. It returns an error when the parent does not exist, or names a parent of
// its own. Remote never stores such a parent (ValidateParent), so the second error means the
// parents were not read from Remote.
func parentOf(config *flex.RemoteStorageTarget, parents map[string]*flex.RemoteStorageTarget) (*flex.RemoteStorageTarget, error) {
	name := config.GetParent()
	if name == "" {
		return nil, nil
	}
	parent, ok := parents[name]
	if !ok {
		return nil, fmt.Errorf("parent %q does not exist", name)
	}
	if parent.GetParent() != "" {
		return nil, fmt.Errorf("parent %q names parent %q, but a parent cannot have a parent", name, parent.GetParent())
	}
	return parent, nil
}

// Resolve returns the effective configuration of config: its definition with every unset value
// taken from its parent (see Overlay). parents maps each parent's name to its definition. The result
// has no parent, because it no longer depends on one. So renaming a parent, which changes only the
// names its children hold, changes no effective configuration, and rebuilds no provider. A config
// without a parent is returned as a copy.
func Resolve(config *flex.RemoteStorageTarget, parents map[string]*flex.RemoteStorageTarget) (*flex.RemoteStorageTarget, error) {
	parent, err := parentOf(config, parents)
	if err != nil {
		return nil, err
	}
	effective := proto.Clone(config).(*flex.RemoteStorageTarget)
	if parent != nil {
		if effective, err = Overlay(parent, effective); err != nil {
			return nil, err
		}
	}
	effective.SetParent("")
	return effective, nil
}

// Inherited returns the sorted keys of what config takes from its parent, config.GetParent(). A key
// is named the way the CLI names it:
//   - a setting by its key, such as "bucket";
//   - fast-start-max-size by that name;
//   - a list entry as <flag>=<key>, such as "storage-class=GLACIER".
func Inherited(config *flex.RemoteStorageTarget, parents map[string]*flex.RemoteStorageTarget) ([]string, error) {
	parent, err := parentOf(config, parents)
	if err != nil {
		return nil, err
	}
	inherited := []string{}
	t, ok := TargetTypeOf(config)
	if !ok || parent == nil {
		return inherited, nil
	}
	effective, err := Overlay(parent, config)
	if err != nil {
		return nil, err
	}
	own := ownKeys(t, config)
	for _, key := range ownKeys(t, effective) {
		if !slices.Contains(own, key) {
			inherited = append(inherited, key)
		}
	}
	slices.Sort(inherited)
	return inherited, nil
}

// ownKeys returns every value config sets, named as Inherited names them.
func ownKeys(t TargetType, config *flex.RemoteStorageTarget) []string {
	keys := []string{}
	if config.GetPolicies().GetFastStartMaxSize() != 0 {
		keys = append(keys, "fast-start-max-size")
	}
	for _, s := range t.Settings() {
		if s.Get(config) != "" {
			keys = append(keys, s.Key)
		}
	}
	for _, list := range t.Lists() {
		for _, key := range listEntryKeys(list, config) {
			keys = append(keys, list.Flag()+"="+key)
		}
	}
	return keys
}

func listEntryKeys(list KeyedList, config *flex.RemoteStorageTarget) []string {
	keys := []string{}
	for _, entry := range list.Entries(config) {
		keys = append(keys, entry.Key)
	}
	return keys
}

// CheckRequired returns an error when effective lacks a setting its type requires of every target,
// such as the bucket. Remote calls it on each effective configuration it stores, because the
// providers do not check these settings, and a target may get them from a parent rather than from
// the flags it was created with. Parents are partial, so this does not apply to them.
func CheckRequired(effective *flex.RemoteStorageTarget) error {
	t, ok := TargetTypeOf(effective)
	if !ok {
		return nil
	}
	missing := []string{}
	for _, s := range t.Settings() {
		if s.RequiredOnCreate && s.Get(effective) == "" {
			missing = append(missing, s.Key)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("no %s is set, by the target or by its parent", strings.Join(missing, " or "))
	}
	return nil
}

// ValidateParent returns an error when parent is not a valid parent on its own:
//   - Its type is registered, because the type's declaration says what can be inherited.
//   - It names no parent. Parents have one level (see the top of this file).
//   - Each list entry is complete, such as a storage class with a check-time. A parent is otherwise
//     allowed to be partial, and is never built into a provider, so nothing else would check its
//     entries until a target inherited them. Entries are inherited whole, so an incomplete entry in
//     a parent would make every target below it invalid.
func ValidateParent(parent *flex.RemoteStorageTarget) error {
	t, ok := TargetTypeOf(parent)
	if !ok {
		return fmt.Errorf("%s configurations cannot be parents", TypeName(parent))
	}
	if parent.GetParent() != "" {
		return fmt.Errorf("a parent cannot name a parent (it names %q)", parent.GetParent())
	}
	for _, list := range t.Lists() {
		if err := list.ValidateEntries(parent); err != nil {
			return err
		}
	}
	return nil
}

// copyListEntry replaces the entry of dst that matches, or appends one, with a copy of the entry of
// src that matches. KeyedList.CopyEntry implementations call it with their own entry type. It
// returns the new dst slice, or an error when src has no matching entry.
func copyListEntry[T proto.Message](dst, src []T, matches func(T) bool, describe string) ([]T, error) {
	i := slices.IndexFunc(src, matches)
	if i < 0 {
		return nil, fmt.Errorf("there is no %s to copy", describe)
	}
	entry := proto.Clone(src[i]).(T)
	if j := slices.IndexFunc(dst, matches); j >= 0 {
		dst[j] = entry
		return dst, nil
	}
	return append(dst, entry), nil
}
