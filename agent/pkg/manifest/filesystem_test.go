package manifest

import (
	"testing"

	"github.com/stretchr/testify/assert"
	pb "github.com/thinkparq/protobuf/go/beegfs"
)

func TestFromToProto_RoundTrip(t *testing.T) {
	original := &pb.Filesystem{
		Common:        &pb.Filesystem_Common{Auth: "secret"},
		MetaConfig:    map[string]string{"key1": "val1"},
		StorageConfig: map[string]string{"key2": "val2"},
		ClientConfig:  map[string]string{"key3": "val3"},
		Agent: map[string]*pb.Agent{
			"agent1": {
				Nodes: []*pb.Node{
					{
						NumId:    1,
						NodeType: pb.NodeType_META,
						Config:   map[string]string{"nkey": "nval"},
						Interfaces: []*pb.Nic{
							{Name: "ib0", Addr: "10.0.0.1/16"},
						},
						Targets: []*pb.Target{
							{
								NumId:   101,
								RootDir: "/mnt",
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
