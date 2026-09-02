package rst

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/protobuf/go/flex"
)

const (
	AllowRestoreFlag   = "allow-restore"
	PriorityFlag       = "priority"
	RemoteCooldownFlag = "remote-cooldown"
	RemotePathFlag     = "remote-path"
	RemoteTargetFlag   = "remote-target"
	RestorePolicyFlag  = "restore-policy"
	StorageClassFlag   = "storage-class"
	UpdateFlag         = "update"
	StubLocalFlag      = "stub-local"
)

const (
	RemoteCooldownFlagHelp = "Time to wait after a file is closed before replication begins (set to 0s to disable). " +
		"Accepts a duration such as 1s, 1m, or 1h. The max duration is 65,535 seconds."
	RestorePolicyFlagHelp = "Set the restore policy for stub files (manual-restore, auto-restore, delayed-restore; " +
		"the short forms manual, auto and delayed are also accepted). Requires --" + StubLocalFlag +
		" to also be set, and can be used to change the policy on existing stub files."
)

// restorePolicyTarget constrains the two value types that restorePolicyFlag can write to
// via a double pointer (**T).
type restorePolicyTarget interface {
	flex.RestorePolicy | beegfs.DataState
}

// NewRestorePolicyFlag returns a pflag.Value that parses a restore policy string and writes the result
// to *p. p may be **flex.RestorePolicy or **beegfs.DataState. See Set for the accepted spellings.
// *p is only set when the user explicitly provides the flag, preserving the optional-field
// semantics required by cfg.HasRestorePolicy() downstream.
func NewRestorePolicyFlag[T restorePolicyTarget](p **T) *restorePolicyFlag[T] {
	return &restorePolicyFlag[T]{p: p}
}

type restorePolicyFlag[T restorePolicyTarget] struct {
	p **T
}

// restorePolicies are the restore policies a user can set. Each is keyed by the beegfs.DataState it
// corresponds to so its display name (and any separator spelling of it) is accepted as input, with
// the original short token kept as a legacy alias so existing scripts don't break.
var restorePolicies = []struct {
	state       beegfs.DataState
	policy      flex.RestorePolicy
	legacyAlias string
}{
	{state: beegfs.DataStateManualRestore, policy: flex.RestorePolicy_RESTORE_POLICY_MANUAL, legacyAlias: "manual"},
	{state: beegfs.DataStateAutoRestore, policy: flex.RestorePolicy_RESTORE_POLICY_AUTO, legacyAlias: "auto"},
	{state: beegfs.DataStateDelayedRestore, policy: flex.RestorePolicy_RESTORE_POLICY_DELAYED, legacyAlias: "delayed"},
}

// restorePolicyNames returns the policies to advertise in help and error messages, as the data state
// display names so what the CLI prints can be typed straight back in.
func restorePolicyNames() []string {
	names := make([]string, 0, len(restorePolicies))
	for _, p := range restorePolicies {
		names = append(names, p.state.String())
	}
	return names
}

func (f *restorePolicyFlag[T]) String() string {
	if f.p == nil || *f.p == nil {
		return "unchanged"
	}
	for _, p := range restorePolicies {
		switch v := any(*f.p).(type) {
		case *flex.RestorePolicy:
			if *v == p.policy {
				return p.state.String()
			}
		case *beegfs.DataState:
			if *v == p.state {
				return p.state.String()
			}
		}
	}
	return "unchanged"
}

func (f *restorePolicyFlag[T]) Type() string {
	return "string"
}

// Set accepts a restore policy by the display name of the data state it maps to
// (manual-restore/auto-restore/delayed-restore), any separator spelling of it, or the original
// short manual/auto/delayed token.
func (f *restorePolicyFlag[T]) Set(value string) error {
	for _, policy := range restorePolicies {
		if !beegfs.EnumInputMatches(value, policy.state, policy.legacyAlias) {
			continue
		}
		switch p := any(f.p).(type) {
		case **flex.RestorePolicy:
			*p = policy.policy.Enum()
		case **beegfs.DataState:
			state := policy.state
			*p = &state
		}
		return nil
	}
	return fmt.Errorf("invalid value %q: must be one of %s", value, strings.Join(restorePolicyNames(), ", "))
}

// cooldownTarget constrains the integer types that cooldownFlag can write to via a double pointer.
// uint16 is used by the entry set backend (BeeMsg RST field); uint32 is used by the proto field.
type cooldownTarget interface {
	uint16 | uint32
}

// NewCooldownFlag returns a pflag.Value that parses a duration string (e.g. "30s", "5m", "1h")
// and stores the result as seconds in *p. The max accepted value is math.MaxUint16 (65535s).
// *p is only set when the user explicitly provides the flag.
func NewCooldownFlag[T cooldownTarget](p **T) *cooldownFlag[T] {
	return &cooldownFlag[T]{p: p}
}

type cooldownFlag[T cooldownTarget] struct {
	p **T
}

func (f *cooldownFlag[T]) String() string {
	if f.p == nil || *f.p == nil {
		return "unchanged"
	}
	return fmt.Sprintf("%d", **f.p)
}

func (f *cooldownFlag[T]) Type() string {
	return "<duration>"
}

func (f *cooldownFlag[T]) Set(value string) error {
	d, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("invalid duration %s: %w", value, err)
	}
	secs := d.Seconds()
	if secs > math.MaxUint16 {
		return fmt.Errorf("cooldown cannot be greater than %d seconds", math.MaxUint16)
	}
	if secs < 0 {
		return fmt.Errorf("cooldown cannot be a negative number")
	}
	v := T(secs)
	*f.p = &v
	return nil
}
