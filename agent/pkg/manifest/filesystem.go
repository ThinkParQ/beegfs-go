// Package manifest defines Go-native structs for defining a BeeGFS instance. This includes
// functions for converting to/from protobuf messages and loading/unloading from YAML files.
// Protobuf structs are not used directly (as is done in other BeeGFS Go projects) to provide a more
// user-friendly YAML manifest than what protobuf generated structs allow.
package manifest

import (
	"fmt"
	"os"

	"github.com/google/uuid"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	pb "github.com/thinkparq/protobuf/go/agent"
	"gopkg.in/yaml.v3"
)

const ShortUUIDLen = 8

func ShortUUID(u uuid.UUID) string {
	return u.String()[:ShortUUIDLen]
}

type Filesystem struct {
	Agents map[string]Agent `yaml:"agents"`
	Common Common           `yaml:"common"`
}

type Agent struct {
	Services []Service `yaml:"services"`
	// Global agent interfaces potentially reused by multiple services.
	Interfaces []Nic `yaml:"interfaces"`
}

type Nic struct {
	Name string `yaml:"name"`
	Addr string `yaml:"address"`
}

// InheritGlobalConfig accepts a shortUUID used internally to generate globally unique names and
// identifiers in case resources for multiple file systems exist on the same machine. Derived by
// taking the first ShortUUIDLen hex digits of the full 128-bit v4 UUID. The caller is responsible
// for validating the shortUUID including verifying no collisions are possible in this manifest.
func (f *Filesystem) InheritGlobalConfig(shortUUID string, longUUID string) error {
	for agentID, agent := range f.Agents {
		for i := range agent.Services {
			service := &agent.Services[i]
			service.shortUUID = shortUUID
			service.longUUID = longUUID
			// Inherit global interface configuration if there are no service specific interfaces.
			if len(service.Interfaces) == 0 {
				service.Interfaces = agent.Interfaces
			}
			// Inherit global service configuration based on the service type.
			if commonServiceConfig, ok := f.Common.GlobalConfig[agent.Services[i].Type]; ok {
				service.Config = inheritMapDefaults(commonServiceConfig, service.Config)
			}
			// Inherit global source configuration based on the service type.
			if service.Executable == "" {
				if ref, ok := f.Common.InstallSource.Refs[service.Type]; ok {
					// If there is a global install reference for this service type use this to
					// derive the executable path.
					service.Executable = f.Common.InstallSource.refToExecutablePath(ref)
				} else {
					// Otherwise get the default executable path for this service type.
					service.Executable = f.Common.InstallSource.nodeTypeToExecutablePath(service.Type)
				}
			}
			// Inherit target configuration from the FS and service:
			for t := range service.Targets {
				agent.Services[i].Targets[t].longUUID = longUUID
				agent.Services[i].Targets[t].shortUUID = shortUUID
				agent.Services[i].Targets[t].nodeType = service.Type
				// TODO: May be different for each service type.
				agent.Services[i].Targets[t].initCmd = service.Executable
			}

			if targetConfig, err := service.GetTargetsConfig(); err != nil {
				return err
			} else {
				for k, v := range targetConfig {
					if user, ok := service.Config[k]; ok {
						return fmt.Errorf("auto-generated target config for %s=%s would overwrite user config %s (user config must be removed)", k, v, user)
					}
					service.Config[k] = v
				}
			}

			// TODO: Inherit global conn.auth and TLS config based on service type. Return an error
			// if users try to manually specify this in the global or per-service config somehow.
			// Maybe this is implemented as methods on the Auth and TLS structs?
		}
		f.Agents[agentID] = agent
	}
	return nil
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

	pSrc := protoFS.GetCommon().GetInstallSource()
	fs.Common = Common{
		GlobalConfig: serviceConfigsFromProto(protoFS.Common.GetGlobalConfig()),
		InstallSource: InstallSource{
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
			Services:   make([]Service, 0),
			Interfaces: make([]Nic, 0),
		}
		for _, i := range a.GetInterfaces() {
			agent.Interfaces = append(agent.Interfaces, Nic{
				Name: i.Name,
				Addr: i.Addr,
			})
		}
		for _, s := range a.GetServices() {
			service := Service{
				ID:         beegfs.NumId(s.GetNumId()),
				Type:       beegfs.NodeTypeFromProto(s.ServiceType),
				Config:     s.GetConfig(),
				Interfaces: make([]Nic, 0),
				Targets:    make([]Target, 0),
				Executable: s.GetExecutable(),
			}

			for _, i := range s.GetInterfaces() {
				service.Interfaces = append(service.Interfaces, Nic{
					Name: i.Name,
					Addr: i.Addr,
				})
			}

			for _, t := range s.GetTargets() {
				target := Target{
					ID:   beegfs.NumId(t.GetNumId()),
					Path: t.GetPath(),
				}
				if t.GetUlfs() != nil {
					target.ULFS = &UnderlyingFS{
						Device:      t.GetUlfs().GetDevice(),
						Type:        ulfsTypeFromProto(t.GetUlfs().GetType()),
						FormatFlags: t.GetUlfs().GetFormatFlags(),
						MountFlags:  t.GetUlfs().GetMountFlags(),
					}

				}
				service.Targets = append(service.Targets, target)
			}
			agent.Services = append(agent.Services, service)
		}
		fs.Agents[id] = agent
	}
	return fs
}

func ToProto(fs *Filesystem) *pb.Filesystem {
	pbFS := &pb.Filesystem{
		Common: &pb.Filesystem_Common{
			GlobalConfig: fs.Common.GlobalConfig.toProto(),
			InstallSource: &pb.InstallSource{
				Type: fs.Common.InstallSource.Type.ToProto(),
				Repo: fs.Common.InstallSource.Repo,
				Refs: fs.Common.InstallSource.Refs.toProto(),
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
			Services:   make([]*pb.Service, 0, len(agent.Services)),
			Interfaces: make([]*pb.Nic, 0, len(agent.Interfaces)),
		}
		for _, i := range agent.Interfaces {
			pbAgent.Interfaces = append(pbAgent.Interfaces, &pb.Nic{
				Name: i.Name,
				Addr: i.Addr,
			})
		}
		for _, service := range agent.Services {
			pbService := &pb.Service{
				NumId:       uint32(service.ID),
				ServiceType: *service.Type.ToProto(),
				Config:      service.Config,
				Interfaces:  make([]*pb.Nic, 0, len(service.Interfaces)),
				Targets:     make([]*pb.Target, 0, len(service.Targets)),
				Executable:  service.Executable,
			}

			for _, nic := range service.Interfaces {
				pbService.Interfaces = append(pbService.Interfaces, &pb.Nic{
					Name: nic.Name,
					Addr: nic.Addr,
				})
			}
			for _, tgt := range service.Targets {
				pbTarget := &pb.Target{
					NumId: uint32(tgt.ID),
					Path:  tgt.Path,
				}
				if tgt.ULFS != nil {
					pbTarget.Ulfs = &pb.Target_UnderlyingFSOpts{
						Device:      tgt.ULFS.Device,
						Type:        tgt.ULFS.Type.toProto(),
						FormatFlags: tgt.ULFS.FormatFlags,
						MountFlags:  tgt.ULFS.MountFlags,
					}
				}
				pbService.Targets = append(pbService.Targets, pbTarget)
			}
			pbAgent.Services = append(pbAgent.Services, pbService)
		}
		pbFS.Agent[agentID] = pbAgent
	}
	return pbFS
}

func FromDisk(path string) (Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func ToDisk(manifest Manifest, path string) error {
	data, err := yaml.Marshal(&manifest)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
