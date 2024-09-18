package procfs

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/common/beegfs"
)

func TestParseClientConfigFile(t *testing.T) {
	tests := []struct {
		input    string
		expected map[string]string
	}{
		{
			input:    `key=value`,
			expected: map[string]string{"key": "value"},
		},
		{
			input: `
cfgFile = /etc/beegfs/beegfs-client.conf			
sysXAttrsCheckCapabilities = never
sysSessionCheckOnClose = 0
sysSessionChecksEnabled = 1
sysFileEventLogMask = link-op,close,setattr,trunc,open-read
sysRenameEbusyAsXdev = 0
tunePageCacheValidityMS = 2000000000		
			`,
			expected: map[string]string{
				"cfgFile":                    "/etc/beegfs/beegfs-client.conf",
				"sysXAttrsCheckCapabilities": "never",
				"sysSessionCheckOnClose":     "0",
				"sysSessionChecksEnabled":    "1",
				"sysFileEventLogMask":        "link-op,close,setattr,trunc,open-read",
				"sysRenameEbusyAsXdev":       "0",
				"tunePageCacheValidityMS":    "2000000000",
			},
		},
		{
			input:    ``,
			expected: map[string]string{},
		},
	}
	for _, test := range tests {
		clientConfig, err := parseClientConfigFile(strings.NewReader(test.input))
		assert.NoError(t, err)
		assert.Equal(t, test.expected, clientConfig)
	}
}

func TestParseNodes(t *testing.T) {

	tests := []struct {
		input    string
		expected []Node
	}{
		{
			input: `
alias-mds1 [ID: 1]
   Root: <yes>
   Connections: TCP: 1 (192.168.64.2:8005 [fallback route]); RDMA: 63 (192.168.64.2:8005); 
some-other-arbitrary-alias [ID: 2]
   Connections: <none>
mds3 [ID: 1]
   Connections: SDP: 64 (192.168.64.4:8005);   				
			`,
			expected: []Node{
				{
					Alias: "alias-mds1",
					NumID: 1,
					Root:  true,
					Peers: []Peer{
						{Type: beegfs.Ethernet, IP: "192.168.64.2:8005", Connections: 1, Fallback: true},
						{Type: beegfs.Rdma, IP: "192.168.64.2:8005", Connections: 63, Fallback: false},
					},
				},
				{
					Alias: "some-other-arbitrary-alias",
					NumID: 2,
					Root:  false,
					Peers: []Peer{},
				},
				{
					Alias: "mds3",
					NumID: 1,
					Root:  false,
					Peers: []Peer{
						{Type: beegfs.Sdp, IP: "192.168.64.4:8005", Connections: 64, Fallback: false},
					},
				},
			},
		}, {
			input: "storage_node_1024 [ID: 1024]",
			expected: []Node{
				{
					Alias: "storage_node_1024",
					NumID: 1024,
					Peers: []Peer{},
				},
			},
		}, {
			input:    "",
			expected: []Node{},
		},
	}

	for _, test := range tests {
		nodes, err := parseNodes(strings.NewReader(test.input))
		assert.NoError(t, err)
		assert.Equal(t, test.expected, nodes)
	}
}

func TestParseMounts(t *testing.T) {

	tests := []struct {
		input    string
		expected map[string]MountPoint
	}{
		{
			input: `
binfmt_misc /proc/sys/fs/binfmt_misc binfmt_misc rw,nosuid,nodev,noexec,relatime 0 0
tmpfs /run/snapd/ns tmpfs rw,nosuid,nodev,noexec,relatime,size=1016744k,mode=755,inode64 0 0
beegfs_nodev /mnt/beegfs beegfs rw,relatime,cfgFile=/etc/beegfs/beegfs-client.conf 0 0
nsfs /run/snapd/ns/lxd.mnt nsfs rw 0 0
tmpfs /run/user/1000 tmpfs rw,nosuid,nodev,relatime,size=1016740k,nr_inodes=254185,mode=700,uid=1000,gid=1000,inode64 0 0		
beegfs_nodev /mnt/2beegfs beegfs ro,cfgFile=/etc/beegfs/2beegfs-client.conf 0 0
			`,
			expected: map[string]MountPoint{
				"/etc/beegfs/beegfs-client.conf": {
					Path: "/mnt/beegfs",
					Opts: map[string]string{
						"rw":       "",
						"relatime": "",
						"cfgFile":  "/etc/beegfs/beegfs-client.conf",
					},
				},
				"/etc/beegfs/2beegfs-client.conf": {
					Path: "/mnt/2beegfs",
					Opts: map[string]string{
						"ro":      "",
						"cfgFile": "/etc/beegfs/2beegfs-client.conf",
					},
				},
			},
		},
	}

	for _, test := range tests {
		nodes, err := parseMounts(strings.NewReader(test.input))
		assert.NoError(t, err)
		assert.Equal(t, test.expected, nodes)
	}
}
