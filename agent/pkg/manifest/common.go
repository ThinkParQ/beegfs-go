package manifest

import (
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	pb "github.com/thinkparq/protobuf/go/agent"
)

type Common struct {
	Auth         *Auth       `yaml:"auth"`
	TLS          *TLS        `yaml:"tls"`
	GlobalConfig NodeConfigs `yaml:"config"`
	Source       Source      `yaml:"source"`
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

type Source struct {
	Type SourceType `yaml:"type"`
	Repo string     `yaml:"repo"`
	Refs SourceRefs `yaml:"refs"`
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

func sourceTypeFromProto(st pb.SourceType) SourceType {
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
