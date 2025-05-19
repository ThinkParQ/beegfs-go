package manifest

import (
	"fmt"
	"path"
	"strings"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	pb "github.com/thinkparq/protobuf/go/agent"
)

type Target struct {
	// shortUUID is set by InheritGlobalConfig and used internally to generate globally unique names
	// and identifiers in case resources for multiple file systems exist on the same machine.
	shortUUID string
	nodeType  beegfs.NodeType
	ID        beegfs.NumId  `yaml:"id"`
	Path      string        `yaml:"path"`
	ULFS      *UnderlyingFS `yaml:"ulfs"`
}

func (t Target) GetPath() string {
	return path.Join(t.Path, t.shortUUID, fmt.Sprintf("%s_%d", t.nodeType, t.ID))
}

type UnderlyingFS struct {
	Device      string           `yaml:"device"`
	Type        UnderlyingFSType `yaml:"type"`
	FormatFlags string           `yaml:"format_flags"`
	MountFlags  string           `yaml:"mount_flags"`
}

type UnderlyingFSType int

const (
	UnknownUnderlyingFS UnderlyingFSType = iota
	EXT4UnderlyingFS
)

func (t UnderlyingFSType) String() string {
	switch t {
	case EXT4UnderlyingFS:
		return "ext4"
	default:
		return "unknown"
	}
}

func (t *UnderlyingFSType) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	switch strings.ToLower(s) {
	case "ext4":
		*t = EXT4UnderlyingFS
	default:
		return fmt.Errorf("invalid underlying fs type: %s", s)
	}
	return nil
}

func (t UnderlyingFSType) MarshalYAML() (any, error) {
	switch t {
	case EXT4UnderlyingFS:
		return "ext4", nil
	default:
		return nil, fmt.Errorf("unknown fs type: %d", t)
	}
}

func ulfsTypeFromProto(fs pb.Target_UnderlyingFSOpts_FsType) UnderlyingFSType {
	switch fs {
	case pb.Target_UnderlyingFSOpts_EXT4:
		return EXT4UnderlyingFS
	default:
		return UnknownUnderlyingFS
	}
}

func (fs UnderlyingFSType) toProto() pb.Target_UnderlyingFSOpts_FsType {
	switch fs {
	case EXT4UnderlyingFS:
		return pb.Target_UnderlyingFSOpts_EXT4
	default:
		return pb.Target_UnderlyingFSOpts_UNSPECIFIED
	}
}
