package manifest

import (
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	pb "github.com/thinkparq/protobuf/go/agent"
)

type Common struct {
	Auth          *Auth         `yaml:"auth"`
	TLS           *TLS          `yaml:"tls"`
	GlobalConfig  NodeConfigs   `yaml:"config"`
	InstallSource InstallSource `yaml:"install-source"`
}

type Auth struct {
	Secret string `yaml:"secret"`
}

type TLS struct {
	Key  string `yaml:"key"`
	Cert string `yaml:"cert"`
}

type NodeConfigs map[beegfs.NodeType]map[string]string

func (s *NodeConfigs) UnmarshalYAML(unmarshal func(any) error) error {
	// We cannot directly apply validation to map[beegfs.NodeType]... during unmarshal because the
	// YAML input uses string keys and as a result things blow up (spectacularly).
	intermediate := map[string]map[string]string{}
	if err := unmarshal(&intermediate); err != nil {
		return err
	}

	result := make(NodeConfigs, len(intermediate))
	for key, val := range intermediate {
		nodeType := beegfs.NodeTypeFromString(key)
		if nodeType == beegfs.InvalidNodeType {
			return fmt.Errorf("invalid node type '%s' in config", key)
		}
		result[nodeType] = val
	}

	*s = result
	return nil
}

func (c NodeConfigs) toProto() []*pb.NodeConfig {
	pbNodeConfigs := make([]*pb.NodeConfig, 0, len(c))
	for nodeType, nodeMap := range c {
		pbNodeConfigs = append(pbNodeConfigs, &pb.NodeConfig{
			NodeType:  *nodeType.ToProto(),
			StringMap: nodeMap,
		})
	}
	return pbNodeConfigs
}

func nodeConfigsFromProto(m []*pb.NodeConfig) NodeConfigs {
	nsm := make(NodeConfigs, len(m))
	for _, node := range m {
		if node != nil && node.GetStringMap() != nil {
			nsm[beegfs.NodeTypeFromProto(node.NodeType)] = node.GetStringMap()
		}
	}
	return nsm
}

type InstallSource struct {
	Type InstallType `yaml:"type"`
	Repo string      `yaml:"repo"`
	Refs SourceRefs  `yaml:"refs"`
}

type SourceRefs map[beegfs.NodeType]string

func (s *SourceRefs) UnmarshalYAML(unmarshal func(any) error) error {
	// We cannot directly apply validation to map[beegfs.NodeType]... during unmarshal because the
	// YAML input uses string keys and as a result things blow up (spectacularly).
	intermediate := map[string]string{}
	if err := unmarshal(&intermediate); err != nil {
		return err
	}

	result := make(SourceRefs, len(intermediate))
	for key, val := range intermediate {
		nodeType := beegfs.NodeTypeFromString(key)
		if nodeType == beegfs.InvalidNodeType {
			return fmt.Errorf("invalid node type '%s' in source refs", key)
		}
		result[nodeType] = val
	}

	*s = result
	return nil
}

func (c SourceRefs) toProto() []*pb.SourceRef {
	pbSourceRefs := make([]*pb.SourceRef, 0, len(c))
	for nodeType, ref := range c {
		pbSourceRefs = append(pbSourceRefs, &pb.SourceRef{
			NodeType: *nodeType.ToProto(),
			Ref:      ref,
		})
	}
	return pbSourceRefs
}

func sourceRefsFromProto(r []*pb.SourceRef) SourceRefs {
	srs := make(SourceRefs, len(r))
	for _, ref := range r {
		if ref != nil {
			srs[beegfs.NodeTypeFromProto(ref.NodeType)] = ref.GetRef()
		}
	}
	return srs
}

type InstallType int

const (
	UnknownInstall InstallType = iota
	LocalInstall
	PackageInstall
)

func (s InstallType) ToProto() pb.InstallType {
	switch s {
	case LocalInstall:
		return pb.InstallType_LOCAL
	case PackageInstall:
		return pb.InstallType_PACKAGE
	default:
		return pb.InstallType_UNKNOWN
	}
}

func sourceTypeFromProto(st pb.InstallType) InstallType {
	switch st {
	case pb.InstallType_LOCAL:
		return LocalInstall
	case pb.InstallType_PACKAGE:
		return PackageInstall
	default:
		return UnknownInstall
	}
}

func (s *InstallType) UnmarshalYAML(unmarshal func(any) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	switch str {
	case "local":
		*s = LocalInstall
	case "package":
		*s = PackageInstall
	default:
		*s = UnknownInstall
	}
	return nil
}

func (s InstallType) MarshalYAML() (any, error) {
	switch s {
	case LocalInstall:
		return "local", nil
	case PackageInstall:
		return "package", nil
	default:
		return "unknown", nil
	}
}
