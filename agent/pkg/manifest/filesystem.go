// Package manifest defines Go-native structs for defining a BeeGFS instance. This includes
// functions for converting to/from protobuf messages and loading/unloading from YAML files.
// Protobuf structs are not used directly (as is done in other BeeGFS Go projects) to provide a more
// user-friendly YAML manifest than what protobuf generated structs allow.
package manifest

import (
	"os"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	pb "github.com/thinkparq/protobuf/go/agent"
	"gopkg.in/yaml.v3"
)

// Filesystems is a map of FsUUIDs to file systems.
type Filesystems map[string]Filesystem

type Filesystem struct {
	Agents map[string]Agent `yaml:"agents"`
	Common Common           `yaml:"common"`
}

type Agent struct {
	Nodes []Node `yaml:"nodes"`
	// Global agent interfaces potentially reused by multiple nodes.
	Interfaces []Nic `yaml:"interfaces"`
}

type Nic struct {
	Name string `yaml:"name"`
	Addr string `yaml:"address"`
}

func (f *Filesystem) InheritGlobalConfig(fsUUID string) {
	for agentID, agent := range f.Agents {
		for i := range agent.Nodes {
			node := &agent.Nodes[i]
			node.fsUUID = fsUUID
			// Inherit global interface configuration if there are no node specific interfaces.
			if len(node.Interfaces) == 0 {
				node.Interfaces = agent.Interfaces
			}
			// Inherit global node configuration based on the node type.
			if commonNodeConfig, ok := f.Common.GlobalConfig[agent.Nodes[i].Type]; ok {
				node.Config = inheritMapDefaults(commonNodeConfig, node.Config)
			}
			// Inherit global source configuration based on the node type.
			if node.Source == nil || node.Source.Ref == "" {
				node.Source = &NodeSource{
					Type: f.Common.Source.Type,
				}
				if ref, ok := f.Common.Source.Refs[node.Type]; ok {
					node.Source.Ref = ref
				}
			}
			// Inherit target configuration from the FS and node:
			for t := range node.Targets {
				agent.Nodes[i].Targets[t].fsUUID = fsUUID
				agent.Nodes[i].Targets[t].nodeType = node.Type
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

func FromProto(protoFS *pb.Filesystem) Filesystem {
	var fs Filesystem
	if protoFS == nil {
		return fs
	}

	pSrc := protoFS.GetCommon().GetSource()
	fs.Common = Common{
		GlobalConfig: nodeConfigsFromProto(protoFS.Common.GetGlobalConfig()),
		Source: Source{
			Type: sourceTypeFromProto(pSrc.Type),
			Repo: pSrc.Repo,
			Refs: sourceRefsFromProto(pSrc.Refs),
		},
	}

	if protoFS.GetCommon().GetAuth() != nil {
		fs.Common.Auth = &Auth{
			Secret: protoFS.GetCommon().GetAuth().GetSecret(),
		}
	}

	if protoFS.GetCommon().GetTls() != nil {
		fs.Common.TLS = &TLS{
			Key:  protoFS.GetCommon().GetTls().GetKey(),
			Cert: protoFS.GetCommon().GetTls().GetCert(),
		}
	}

	fs.Agents = make(map[string]Agent, len(protoFS.GetAgent()))
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
					Type: sourceTypeFromProto(n.GetSource().GetType()),
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
						Type:        ulfsTypeFromProto(t.GetUlfs().GetType()),
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
			GlobalConfig: fs.Common.GlobalConfig.toProto(),
			Source: &pb.Source{
				Type: fs.Common.Source.Type.ToProto(),
				Repo: fs.Common.Source.Repo,
				Refs: fs.Common.Source.Refs.toProto(),
			},
		},
		Agent: make(map[string]*pb.Agent),
	}

	if fs.Common.Auth != nil {
		pbFS.Common.Auth = &pb.Auth{
			Secret: fs.Common.Auth.Secret,
		}
	}

	if fs.Common.TLS != nil {
		pbFS.Common.Tls = &pb.TLS{
			Key:  fs.Common.TLS.Key,
			Cert: fs.Common.TLS.Cert,
		}
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
						Type:        tgt.ULFS.Type.toProto(),
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

func FromDisk(path string) (Filesystems, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var filesystems Filesystems
	if err := yaml.Unmarshal(data, &filesystems); err != nil {
		return nil, err
	}
	return filesystems, nil
}

func ToDisk(filesystems Filesystems, path string) error {
	data, err := yaml.Marshal(&filesystems)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
