package manifest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	pb "github.com/thinkparq/protobuf/go/agent"
	pbb "github.com/thinkparq/protobuf/go/beegfs"
)

func TestFromToProto_RoundTrip(t *testing.T) {
	original := &pb.Filesystem{
		Common: &pb.Filesystem_Common{
			Auth: &pb.Auth{
				Secret: "secret",
			},
			Tls: &pb.TLS{
				Key:  "tlsKey",
				Cert: "tlsCert",
			},
			GlobalConfig: []*pb.ServiceConfig{
				{
					ServiceType: pbb.NodeType_META,
					StringMap:   map[string]string{"key": "val"},
				},
			},
			InstallSource: &pb.InstallSource{
				Type: pb.InstallType_PACKAGE,
				Refs: []*pb.SourceRef{
					{
						ServiceType: pbb.NodeType_META,
						Ref:         "ref",
					},
				},
			},
		},

		Agent: map[string]*pb.Agent{
			"agent1": {
				Interfaces: []*pb.Nic{
					{Name: "eth0", Addr: "11.0.0.1/16"},
				},
				Services: []*pb.Service{
					{
						NumId:       1,
						ServiceType: pbb.NodeType_META,
						Config:      map[string]string{"nkey": "nval"},
						Interfaces: []*pb.Nic{
							{Name: "ib0", Addr: "10.0.0.1/16"},
						},
						InstallSource: &pb.Service_InstallSource{
							Type: pb.InstallType_LOCAL,
							Ref:  "12345",
						},
						Targets: []*pb.Target{
							{
								NumId: 101,
								Path:  "/mnt",
								Ulfs: &pb.Target_UnderlyingFSOpts{
									Device:      "/dev/sda1",
									Type:        pb.Target_UnderlyingFSOpts_EXT4,
									FormatFlags: "force",
									MountFlags:  "ro",
								},
							},
						},
					},
				},
			},
		},
	}

	goStruct := FromProto(original)
	roundTripped := ToProto(&goStruct)

	assert.Equal(t, original, roundTripped, "round-trip protobuf -> go -> protobuf did not match original")
}

func TestInheritGlobalConfig(t *testing.T) {
	tests := []struct {
		name        string
		input       Filesystem
		expectedNIC string // Expected NIC name in service if inherited
		expectedCfg map[string]string
		expectedSrc ServiceInstallSource
	}{
		{
			name: "inherit source, NIC and meta config",
			input: Filesystem{
				Common: Common{
					GlobalConfig: ServiceConfigs{beegfs.Meta: map[string]string{
						"foo": "bar",              // inherited
						"baz": "service-specific", // overridden
					}},
					InstallSource: InstallSource{
						Refs: SourceRefs{beegfs.Meta: "beegfs-meta=8.0.1"},
						Type: PackageInstall,
						Repo: "repoURL",
					},
				},
				Agents: map[string]Agent{
					"agent1": {
						Interfaces: []Nic{
							{Name: "ib0", Addr: "10.0.0.1/16"},
						},
						Services: []Service{
							{
								Type:   beegfs.Meta,
								ID:     1,
								Config: map[string]string{"baz": "service-specific"},
								Targets: []Target{
									{
										ID:   beegfs.NumId(1),
										Path: "/beegfs/",
									},
								},
							},
						},
					},
				},
			},
			expectedNIC: "ib0",
			expectedCfg: map[string]string{
				"foo": "bar",              // inherited
				"baz": "service-specific", // overridden
			},
			expectedSrc: ServiceInstallSource{
				Type: PackageInstall,
				Ref:  "beegfs-meta=8.0.1",
			},
		},
		{
			name: "no inheritance if NICs or source are present",
			input: Filesystem{
				Common: Common{
					GlobalConfig: ServiceConfigs{
						beegfs.Meta: map[string]string{
							"quota": "enabled",
						},
					},
					InstallSource: InstallSource{
						Type: PackageInstall,
						Refs: SourceRefs{beegfs.Meta: "beegfs-meta=8.0.1"},
						Repo: "repoURL",
					},
				},
				Agents: map[string]Agent{
					"agent1": {
						Interfaces: []Nic{
							{Name: "ib0", Addr: "10.0.0.1/16"},
						},
						Services: []Service{
							{
								Type: beegfs.Meta,
								ID:   2,
								Interfaces: []Nic{
									{Name: "eth0", Addr: "192.168.0.1/24"},
								},
								Config: map[string]string{"quota": "override"},
								InstallSource: &ServiceInstallSource{
									Type: LocalInstall,
									Ref:  "/home/tux/beegfs-meta",
								},
							},
						},
					},
				},
			},
			expectedNIC: "eth0",
			expectedCfg: map[string]string{
				"quota": "override",
			},
			expectedSrc: ServiceInstallSource{
				Type: LocalInstall,
				Ref:  "/home/tux/beegfs-meta",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := tt.input
			fs.InheritGlobalConfig("testFS")
			agent := fs.Agents["agent1"]
			service := agent.Services[0]
			assert.Equal(t, tt.expectedNIC, service.Interfaces[0].Name)
			assert.Equal(t, tt.expectedCfg, service.Config)
			assert.Equal(t, "testFS", service.fsUUID)
			for _, target := range service.Targets {
				assert.Equal(t, "/beegfs/testFS/meta_1", target.GetPath(), "generated target path did not match")
			}

		})
	}
}

func TestInheritMapDefaults(t *testing.T) {
	tests := []struct {
		name     string
		defaults map[string]string
		target   map[string]string
		expected map[string]string
	}{
		{
			name: "adds missing keys",
			defaults: map[string]string{
				"a": "1",
				"b": "2",
			},
			target: map[string]string{
				"a": "1-overridden",
			},
			expected: map[string]string{
				"a": "1-overridden", // should NOT be overridden
				"b": "2",            // should be added
			},
		},
		{
			name:     "target already has all keys",
			defaults: map[string]string{"a": "1"},
			target:   map[string]string{"a": "custom"},
			expected: map[string]string{"a": "custom"},
		},
		{
			name:     "empty defaults",
			defaults: map[string]string{},
			target:   map[string]string{"a": "existing"},
			expected: map[string]string{"a": "existing"},
		},
		{
			name:     "empty target",
			defaults: map[string]string{"a": "1"},
			target:   map[string]string{},
			expected: map[string]string{"a": "1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := inheritMapDefaults(tt.defaults, tt.target)
			assert.Equal(t, tt.expected, result)
		})
	}
}
