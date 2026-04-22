package rst

import (
	"fmt"
	"strings"

	"github.com/thinkparq/protobuf/go/flex"
)

const (
	AllowRestoreFlag  = "allow-restore"
	PriorityFlag      = "priority"
	RemotePathFlag    = "remote-path"
	RemoteTargetFlag  = "remote-target"
	RestorePolicyFlag = "restore-policy"
	StorageClassFlag  = "storage-class"
	UpdateFlag        = "update"
	StubLocalFlag     = "stub-local"
)

const (
	RestorePolicyFlagHelp = "Set the restore policy for stub files (manual, auto, delayed). Requires --" + StubLocalFlag +
		" to also be set, and can be used to change the policy on existing stub files."
)

// NewRestorePolicyFlag returns a pflag.Value that parses a restore policy string (manual, auto,
// delayed) and writes the corresponding proto enum value to *p. *p is only set when the user
// explicitly provides the flag, preserving the optional-field semantics required by
// cfg.HasRestorePolicy() downstream.
func NewRestorePolicyFlag(p **flex.RestorePolicy) *restorePolicyFlag {
	return &restorePolicyFlag{p: p}
}

type restorePolicyFlag struct {
	p **flex.RestorePolicy
}

func (f *restorePolicyFlag) String() string {
	if f.p == nil || *f.p == nil {
		return "manual"
	}
	switch **f.p {
	case flex.RestorePolicy_RESTORE_POLICY_AUTO:
		return "auto"
	case flex.RestorePolicy_RESTORE_POLICY_DELAYED:
		return "delayed"
	default:
		return "manual"
	}
}

func (f *restorePolicyFlag) Type() string {
	return "string"
}

func (f *restorePolicyFlag) Set(value string) error {
	switch strings.ToLower(value) {
	case "manual":
		*f.p = flex.RestorePolicy_RESTORE_POLICY_MANUAL.Enum()
	case "auto":
		*f.p = flex.RestorePolicy_RESTORE_POLICY_AUTO.Enum()
	case "delayed":
		*f.p = flex.RestorePolicy_RESTORE_POLICY_DELAYED.Enum()
	default:
		return fmt.Errorf("invalid value %q: must be manual, auto, or delayed", value)
	}
	return nil
}
