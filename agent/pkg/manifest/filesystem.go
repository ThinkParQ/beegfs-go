// Package manifest defines Go-native structs for defining a BeeGFS instance. This includes
// functions for converting to/from protobuf messages and loading/unloading from YAML files.
// Protobuf structs are not used directly (as is done in other BeeGFS Go projects) to provide a more
// user-friendly YAML manifest than what protobuf generated structs allow.
package manifest

import (
	"fmt"
	"os"
	"strings"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	pb "github.com/thinkparq/protobuf/go/beegfs"
	"gopkg.in/yaml.v3"
)

func New() Filesystem {
	return Filesystem{
		Agents: make(map[string]Agent),
		Common: Common{
			MetaConfig:    make(map[string]string),
			StorageConfig: make(map[string]string),
			ClientConfig:  make(map[string]string),
			Source:        Source{},
		},
	}
}

type Filesystem struct {
	Agents map[string]Agent `yaml:"agents"`
	Common Common           `yaml:"common"`
}

type SourceType int

const (
	UnknownSource SourceType = iota
	LocalSource
	PackageSource
)

func (s SourceType) ToProto() pb.SourceType {
	switch s {
	case LocalSource:
		return pb.SourceType_LOCAL
	case PackageSource:
		return pb.SourceType_PACKAGE
	default:
		return pb.SourceType_UNKNOWN
	}
}

func SourceTypeFromProto(st pb.SourceType) SourceType {
	switch st {
	case pb.SourceType_LOCAL:
		return LocalSource
	case pb.SourceType_PACKAGE:
		return PackageSource
	default:
		return UnknownSource
	}
}

func (s *SourceType) UnmarshalYAML(unmarshal func(any) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	switch str {
	case "local":
		*s = LocalSource
	case "package":
		*s = PackageSource
	default:
		*s = UnknownSource
	}
	return nil
}

func (s SourceType) MarshalYAML() (any, error) {
	switch s {
	case LocalSource:
		return "local", nil
	case PackageSource:
		return "package", nil
	default:
		return "unknown", nil
	}
}

type Source struct {
	Type       SourceType `yaml:"type"`
	Repo       string     `yaml:"repo"`
	Management string     `yaml:"management"`
	Meta       string     `yaml:"meta"`
	Storage    string     `yaml:"storage"`
	Remote     string     `yaml:"remote"`
	Sync       string     `yaml:"sync"`
}

func (s Source) refForNodeType(t beegfs.NodeType) string {
	switch t {
	case beegfs.Meta:
		return s.Meta
	case beegfs.Storage:
		return s.Storage
	case beegfs.Management:
		return s.Management
	case beegfs.Remote:
		return s.Remote
	case beegfs.Sync:
		return s.Sync
	default:
		return ""
	}
}

func (f *Filesystem) InheritGlobalConfig() {
	for agentID, agent := range f.Agents {
		for i := range agent.Nodes {
			node := &agent.Nodes[i]
			// Inherit global interface configuration if there are no node specific interfaces.
			if len(node.Interfaces) == 0 {
				node.Interfaces = agent.Interfaces
			}
			// Inherit global node configuration based on the node type.
			switch agent.Nodes[i].Type {
			case beegfs.Meta:
				node.Config = inheritMapDefaults(f.Common.MetaConfig, node.Config)
			case beegfs.Storage:
				node.Config = inheritMapDefaults(f.Common.StorageConfig, node.Config)
			case beegfs.Client:
				node.Config = inheritMapDefaults(f.Common.ClientConfig, node.Config)
			}
			// Inherit global source configuration based on the node type.
			if node.Source == nil || node.Source.Ref == "" {
				node.Source = &NodeSource{
					Type: f.Common.Source.Type,
					Ref:  f.Common.Source.refForNodeType(node.Type),
				}
			}
		}
		f.Agents[agentID] = agent
	}
}

func inheritMapDefaults(defaults, target map[string]string) map[string]string {
	if target == nil {
		target = make(map[string]string, 0)
	}
	for k, v := range defaults {
		if _, ok := target[k]; !ok {
			target[k] = v
		}
	}
	return target
}

type Common struct {
	Auth          string            `yaml:"auth"`
	MetaConfig    map[string]string `yaml:"meta_config"`
	StorageConfig map[string]string `yaml:"storage_config"`
	ClientConfig  map[string]string `yaml:"client_config"`
	Source        Source            `yaml:"source"`
}

type Agent struct {
	Nodes []Node `yaml:"nodes"`
	// Global interfaces potentially reused by multiple nodes.
	Interfaces []Nic `yaml:"interfaces"`
}

type Node struct {
	ID         beegfs.NumId      `yaml:"id"`
	Type       beegfs.NodeType   `yaml:"type"`
	Config     map[string]string `yaml:"config"`
	Interfaces []Nic             `yaml:"interfaces"`
	Targets    []Target          `yaml:"targets"`
	Source     *NodeSource       `yaml:"source,omitempty"`
}

type NodeSource struct {
	Type SourceType `yaml:"type"`
	Ref  string     `yaml:"ref"`
}

type Nic struct {
	Name string `yaml:"name"`
	Addr string `yaml:"address"`
}

type Target struct {
	ID      beegfs.NumId  `yaml:"id"`
	RootDir string        `yaml:"root_dir"`
	ULFS    *UnderlyingFS `yaml:"ulfs"`
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

func fsTypeFromProto(fs pb.Target_UnderlyingFSOpts_FsType) UnderlyingFSType {
	switch fs {
	case pb.Target_UnderlyingFSOpts_EXT4:
		return EXT4UnderlyingFS
	default:
		return UnknownUnderlyingFS
	}
}

func fsTypeToProto(fs UnderlyingFSType) pb.Target_UnderlyingFSOpts_FsType {
	switch fs {
	case EXT4UnderlyingFS:
		return pb.Target_UnderlyingFSOpts_EXT4
	default:
		return pb.Target_UnderlyingFSOpts_UNSPECIFIED
	}
}

func FromProto(protoFS *pb.Filesystem) Filesystem {
	fs := New()
	if protoFS == nil {
		return fs
	}

	pSrc := protoFS.GetCommon().GetSource()
	fs.Common = Common{
		Auth:          protoFS.GetCommon().GetAuth(),
		MetaConfig:    protoFS.GetCommon().GetMetaConfig(),
		StorageConfig: protoFS.GetCommon().GetStorageConfig(),
		ClientConfig:  protoFS.GetCommon().GetClientConfig(),
		Source: Source{
			Type:       SourceTypeFromProto(pSrc.Type),
			Repo:       pSrc.Repo,
			Management: pSrc.Management,
			Meta:       pSrc.Meta,
			Storage:    pSrc.Storage,
			Remote:     pSrc.Remote,
			Sync:       pSrc.Sync,
		},
	}

	for id, a := range protoFS.GetAgent() {
		agent := Agent{
			Nodes:      make([]Node, 0),
			Interfaces: make([]Nic, 0),
		}
		for _, i := range a.GetInterfaces() {
			agent.Interfaces = append(agent.Interfaces, Nic{
				Name: i.Name,
				Addr: i.Addr,
			})
		}
		for _, n := range a.GetNodes() {
			node := Node{
				ID:         beegfs.NumId(n.GetNumId()),
				Type:       beegfs.NodeTypeFromProto(n.NodeType),
				Config:     n.GetConfig(),
				Interfaces: make([]Nic, 0),
				Targets:    make([]Target, 0),
			}

			if n.Source != nil {
				node.Source = &NodeSource{
					Type: SourceTypeFromProto(n.GetSource().GetType()),
					Ref:  n.GetSource().GetRef(),
				}
			}

			for _, i := range n.GetInterfaces() {
				node.Interfaces = append(node.Interfaces, Nic{
					Name: i.Name,
					Addr: i.Addr,
				})
			}

			for _, t := range n.GetTargets() {
				target := Target{
					ID:      beegfs.NumId(t.GetNumId()),
					RootDir: t.GetRootDir(),
				}
				if t.GetUlfs() != nil {
					target.ULFS = &UnderlyingFS{
						Device:      t.GetUlfs().GetDevice(),
						Type:        fsTypeFromProto(t.GetUlfs().GetType()),
						FormatFlags: t.GetUlfs().GetFormatFlags(),
						MountFlags:  t.GetUlfs().GetMountFlags(),
					}

				}
				node.Targets = append(node.Targets, target)
			}
			agent.Nodes = append(agent.Nodes, node)
		}
		fs.Agents[id] = agent
	}
	return fs
}

func ToProto(fs *Filesystem) *pb.Filesystem {
	pbFS := &pb.Filesystem{
		Common: &pb.Filesystem_Common{
			Auth:          fs.Common.Auth,
			MetaConfig:    fs.Common.MetaConfig,
			StorageConfig: fs.Common.StorageConfig,
			ClientConfig:  fs.Common.ClientConfig,
			Source: &pb.Filesystem_Common_Source{
				Type:       fs.Common.Source.Type.ToProto(),
				Repo:       fs.Common.Source.Repo,
				Management: fs.Common.Source.Management,
				Meta:       fs.Common.Source.Meta,
				Storage:    fs.Common.Source.Storage,
				Remote:     fs.Common.Source.Remote,
				Sync:       fs.Common.Source.Sync,
			},
		},

		Agent: make(map[string]*pb.Agent),
	}

	for agentID, agent := range fs.Agents {
		pbAgent := &pb.Agent{
			Nodes:      make([]*pb.Node, 0, len(agent.Nodes)),
			Interfaces: make([]*pb.Nic, 0, len(agent.Interfaces)),
		}
		for _, i := range agent.Interfaces {
			pbAgent.Interfaces = append(pbAgent.Interfaces, &pb.Nic{
				Name: i.Name,
				Addr: i.Addr,
			})
		}
		for _, node := range agent.Nodes {
			pbNode := &pb.Node{
				NumId:      uint32(node.ID),
				NodeType:   *node.Type.ToProto(),
				Config:     node.Config,
				Interfaces: make([]*pb.Nic, 0, len(node.Interfaces)),
				Targets:    make([]*pb.Target, 0, len(node.Targets)),
			}

			if node.Source != nil {
				pbNode.Source = &pb.Node_Source{
					Type: node.Source.Type.ToProto(),
					Ref:  node.Source.Ref,
				}
			}

			for _, nic := range node.Interfaces {
				pbNode.Interfaces = append(pbNode.Interfaces, &pb.Nic{
					Name: nic.Name,
					Addr: nic.Addr,
				})
			}
			for _, tgt := range node.Targets {
				pbTarget := &pb.Target{
					NumId:   uint32(tgt.ID),
					RootDir: tgt.RootDir,
				}
				if tgt.ULFS != nil {
					pbTarget.Ulfs = &pb.Target_UnderlyingFSOpts{
						Device:      tgt.ULFS.Device,
						Type:        fsTypeToProto(tgt.ULFS.Type),
						FormatFlags: tgt.ULFS.FormatFlags,
						MountFlags:  tgt.ULFS.MountFlags,
					}
				}
				pbNode.Targets = append(pbNode.Targets, pbTarget)
			}
			pbAgent.Nodes = append(pbAgent.Nodes, pbNode)
		}
		pbFS.Agent[agentID] = pbAgent
	}
	return pbFS
}

func FromDisk(path string) (Filesystem, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Filesystem{}, err
	}
	var fs Filesystem
	if err := yaml.Unmarshal(data, &fs); err != nil {
		return fs, err
	}
	return fs, nil
}

func ToDisk(fs Filesystem, path string) error {
	data, err := yaml.Marshal(&fs)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
