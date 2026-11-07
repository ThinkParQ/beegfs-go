package rst

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

// This file resolves backends and checks them. A backend is the named connection to one remote
// store, such as an S3 endpoint and its credentials, of one RST type. Targets of that type name it
// in their backend field and inherit from it. A backend is not an RST: it has no ID and no
// provider, and no stub or job names it. A backend names no backend, so there is one level: a
// value comes from the target or from its backend, and no rule for precedence or loops is needed.
//
// Terms used here:
//   - The definition of a target or backend is what is stored for it: only the values it sets
//     itself.
//   - The effective configuration of a target is its definition with every value it leaves unset
//     taken from its backend. Providers are built from it.
//
// What a backend holds is decided by its type, through each setting's backend role (see
// targettype.go). A backend must set its required settings and must not set its forbidden ones.
// Everything else is a default for its targets. A target may override any value, required ones
// included, so the roles constrain the backend, never the target.
//
// Rules for what a configuration inherits. "Unset" is the zero value, because the generated proto3
// fields cannot tell an unset value from a zero one:
//   - A setting is inherited when the configuration's value is "".
//   - fast-start-max-size is inherited when it is 0.
//   - A keyed list is inherited per entry. Every entry whose key the configuration lacks is copied
//     whole from the backend. An entry with the same key replaces the backend's entry whole: its
//     fields are never merged with the backend's.
//   - id, name and backend are never inherited.
//
// So a configuration cannot clear a value its backend sets, or remove an entry its backend has.

// Overlay returns over with every value it leaves unset taken from base. base is usually over's
// backend. Neither argument is modified. Both must have the same type, and the type must be
// registered, because the type's declaration says what its settings and lists are.
func Overlay(base, over *flex.RemoteStorageTarget) (*flex.RemoteStorageTarget, error) {
	if err := CheckBackendType(base, over); err != nil {
		return nil, err
	}
	t, ok := TargetTypeOf(over)
	if !ok {
		return nil, fmt.Errorf("%s configurations cannot have backends", TypeName(over))
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

// CheckBackendType returns an error when target, which names backend, has another type than the
// backend. A backend describes one kind of store, so only targets of its type can use it. Remote
// and the CLI both call it, so the two refuse a mismatch with the same message.
func CheckBackendType(backend, target *flex.RemoteStorageTarget) error {
	if backend.WhichType() != target.WhichType() {
		return fmt.Errorf("backend %q is of type %s, but the target is of type %s", backend.GetName(), TypeName(backend), TypeName(target))
	}
	return nil
}

// backendOf returns the backend config names, or nil when it names none. backends maps each
// backend's name to its definition. It returns an error when the backend does not exist, or names a
// backend of its own. Remote never stores such a backend (ValidateBackend), so the second error
// means the backends were not read from Remote.
func backendOf(config *flex.RemoteStorageTarget, backends map[string]*flex.RemoteStorageTarget) (*flex.RemoteStorageTarget, error) {
	name := config.GetBackend()
	if name == "" {
		return nil, nil
	}
	backend, ok := backends[name]
	if !ok {
		return nil, fmt.Errorf("backend %q does not exist", name)
	}
	if backend.GetBackend() != "" {
		return nil, fmt.Errorf("backend %q names backend %q, but a backend cannot have a backend", name, backend.GetBackend())
	}
	return backend, nil
}

// Resolve returns the effective configuration of config: its definition with every unset value
// taken from its backend (see Overlay). backends maps each backend's name to its definition. The
// result names no backend, because it no longer depends on one. So renaming a backend, which
// changes only the names its targets hold, changes no effective configuration, and rebuilds no
// provider. A config without a backend is returned as a copy.
func Resolve(config *flex.RemoteStorageTarget, backends map[string]*flex.RemoteStorageTarget) (*flex.RemoteStorageTarget, error) {
	backend, err := backendOf(config, backends)
	if err != nil {
		return nil, err
	}
	effective := proto.Clone(config).(*flex.RemoteStorageTarget)
	if backend != nil {
		if effective, err = Overlay(backend, effective); err != nil {
			return nil, err
		}
	}
	effective.SetBackend("")
	return effective, nil
}

// Inherited returns the sorted keys of what config takes from its backend, config.GetBackend(). A
// key is named the way the CLI names it:
//   - a setting by its key, such as "endpoint-url";
//   - fast-start-max-size by that name;
//   - a list entry as <flag>=<key>, such as "storage-class=GLACIER".
func Inherited(config *flex.RemoteStorageTarget, backends map[string]*flex.RemoteStorageTarget) ([]string, error) {
	backend, err := backendOf(config, backends)
	if err != nil {
		return nil, err
	}
	inherited := []string{}
	t, ok := TargetTypeOf(config)
	if !ok || backend == nil {
		return inherited, nil
	}
	effective, err := Overlay(backend, config)
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
// providers do not check these settings, and a target may get them from a backend rather than from
// the flags it was created with. A backend has its own requirements (ValidateBackend).
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
		return fmt.Errorf("no %s is set, by the target or by its backend", strings.Join(missing, " or "))
	}
	return nil
}

// ValidateBackend returns an error when backend is not a valid backend on its own. It never
// contacts the store, so Remote runs it on every backend change. The checks, in order:
//   - Its type is registered, because the type's declaration says what a backend holds.
//   - It names no backend. Backends have one level (see the top of this file).
//   - It sets every setting its type requires of a backend, and none its type forbids, and has no
//     entry in a list its type forbids. The error names every such setting at once.
//   - Each list entry is complete, such as a storage class with a check-time. Nothing else would
//     check a backend's entries until a target inherited them. Entries are inherited whole, so an
//     incomplete entry in a backend would make every target that names it invalid.
//
// It does not check that a value is well formed, such as the endpoint URL. A type may reach its
// store over another protocol, and only contacting the store proves a value works. VerifyBackend
// does that.
func ValidateBackend(backend *flex.RemoteStorageTarget) error {
	t, ok := TargetTypeOf(backend)
	if !ok {
		return fmt.Errorf("%s configurations cannot be backends", TypeName(backend))
	}
	if backend.GetBackend() != "" {
		return fmt.Errorf("a backend cannot name a backend (it names %q)", backend.GetBackend())
	}
	missing := []string{}
	forbidden := []string{}
	for _, s := range t.Settings() {
		set := s.Get(backend) != ""
		switch {
		case s.Backend == BackendRequired && !set:
			missing = append(missing, s.Key)
		case s.Backend == BackendForbidden && set:
			forbidden = append(forbidden, s.Key)
		}
	}
	for _, list := range t.Lists() {
		if list.BackendForbidden() && len(list.Entries(backend)) > 0 {
			forbidden = append(forbidden, list.Plural())
		}
	}
	problems := []string{}
	if len(missing) > 0 {
		problems = append(problems, fmt.Sprintf("a %s backend must set %s", t.Name(), strings.Join(missing, ", ")))
	}
	if len(forbidden) > 0 {
		problems = append(problems, fmt.Sprintf("a %s backend cannot set %s, because each target sets its own", t.Name(), strings.Join(forbidden, ", ")))
	}
	if len(problems) > 0 {
		return fmt.Errorf("%s", strings.Join(problems, "; "))
	}
	for _, list := range t.Lists() {
		if err := list.ValidateEntries(backend); err != nil {
			return err
		}
	}
	return nil
}

// VerifyBackend checks that backend reaches its store, and that the store accepts its credentials.
// Unlike every other function here, it contacts the store, through the type's VerifyBackend. It
// runs ValidateBackend first, so the type's VerifyBackend only ever sees a valid backend. note is
// a detail about a check that passed, or "".
func VerifyBackend(ctx context.Context, backend *flex.RemoteStorageTarget) (note string, err error) {
	if err := ValidateBackend(backend); err != nil {
		return "", err
	}
	t, _ := TargetTypeOf(backend)
	return t.VerifyBackend(ctx, backend)
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
