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
	RestorePolicyFlagHelp = "Set the restore policy for stub files (manual, auto, delayed). Requires --" + StubLocalFlag +
		" to also be set, and can be used to change the policy on existing stub files."
)

// restorePolicyTarget constrains the two value types that restorePolicyFlag can write to
// via a double pointer (**T).
type restorePolicyTarget interface {
	flex.RestorePolicy | beegfs.DataState
}

// NewRestorePolicyFlag returns a pflag.Value that parses a restore policy string (manual, auto,
// delayed) and writes the result to *p. p may be **flex.RestorePolicy or **beegfs.DataState.
// *p is only set when the user explicitly provides the flag, preserving the optional-field
// semantics required by cfg.HasRestorePolicy() downstream.
func NewRestorePolicyFlag[T restorePolicyTarget](p **T) *restorePolicyFlag[T] {
	return &restorePolicyFlag[T]{p: p}
}

type restorePolicyFlag[T restorePolicyTarget] struct {
	p **T
}

func (f *restorePolicyFlag[T]) String() string {
	if f.p == nil || *f.p == nil {
		return "unchanged"
	}
	switch v := any(*f.p).(type) {
	case *flex.RestorePolicy:
		switch *v {
		case flex.RestorePolicy_RESTORE_POLICY_AUTO:
			return "auto"
		case flex.RestorePolicy_RESTORE_POLICY_DELAYED:
			return "delayed"
		}
	case *beegfs.DataState:
		switch *v {
		case beegfs.DataStateAutoRestore:
			return "auto"
		case beegfs.DataStateDelayedRestore:
			return "delayed"
		}
	}
	return "unchanged"
}

func (f *restorePolicyFlag[T]) Type() string {
	return "string"
}

func (f *restorePolicyFlag[T]) Set(value string) error {
	switch p := any(f.p).(type) {
	case **flex.RestorePolicy:
		switch strings.ToLower(value) {
		case "manual":
			*p = flex.RestorePolicy_RESTORE_POLICY_MANUAL.Enum()
		case "auto":
			*p = flex.RestorePolicy_RESTORE_POLICY_AUTO.Enum()
		case "delayed":
			*p = flex.RestorePolicy_RESTORE_POLICY_DELAYED.Enum()
		default:
			return fmt.Errorf("invalid value %q: must be manual, auto, or delayed", value)
		}
	case **beegfs.DataState:
		var state beegfs.DataState
		switch strings.ToLower(value) {
		case "manual":
			state = beegfs.DataStateManualRestore
		case "auto":
			state = beegfs.DataStateAutoRestore
		case "delayed":
			state = beegfs.DataStateDelayedRestore
		default:
			return fmt.Errorf("invalid value %q: must be manual, auto, or delayed", value)
		}
		*p = &state
	}
	return nil
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
