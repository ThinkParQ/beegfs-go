package procfs

import (
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/logger"
	"go.uber.org/zap"
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
						{Type: beegfs.Tcp, IP: "192.168.64.2:8005", Connections: 1, Fallback: true},
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
		input      string
		expected   []MountPoint
		expFsPaths []string
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
			expected: []MountPoint{
				{
					Path: "/mnt/beegfs",
					Opts: map[string]string{
						"rw":       "",
						"relatime": "",
						"cfgFile":  "/etc/beegfs/beegfs-client.conf",
					},
				},
				{
					Path: "/mnt/2beegfs",
					Opts: map[string]string{
						"ro":      "",
						"cfgFile": "/etc/beegfs/2beegfs-client.conf",
					},
				},
			},
			expFsPaths: []string{"beegfs"},
		},
		{
			// Two mounts that share a config file are both kept, and so is a mount that has no
			// config file.
			input: `
	beegfs_nodev /mnt/beegfs1 beegfs rw,cfgFile=/etc/beegfs/beegfs-client.conf 0 0
	beegfs_nodev /mnt/beegfs2/ beegfs rw,cfgFile=/etc/beegfs/beegfs-client.conf 0 0
	beegfs_nodev /mnt/beegfs3 beegfs rw,sysMgmtdHost=192.168.1.100 0 0
			`,
			expected: []MountPoint{
				{Path: "/mnt/beegfs1", Opts: map[string]string{"rw": "", "cfgFile": "/etc/beegfs/beegfs-client.conf"}},
				{Path: "/mnt/beegfs2", Opts: map[string]string{"rw": "", "cfgFile": "/etc/beegfs/beegfs-client.conf"}},
				{Path: "/mnt/beegfs3", Opts: map[string]string{"rw": "", "sysMgmtdHost": "192.168.1.100"}},
			},
			expFsPaths: []string{"beegfs"},
		},
		{
			// The kernel escapes a space, tab, newline and backslash in a mount point.
			input: `
	beegfs_nodev /mnt/my\040beegfs beegfs rw 0 0
	beegfs_nodev /mnt/tab\011new\012line\134slash beegfs rw 0 0
	beegfs_nodev /mnt/literal\134040 beegfs rw 0 0
			`,
			expected: []MountPoint{
				{Path: "/mnt/my beegfs", Opts: map[string]string{"rw": ""}},
				{Path: "/mnt/tab\tnew\nline\\slash", Opts: map[string]string{"rw": ""}},
				// A backslash followed by "040" in the name. The decoded backslash is not decoded
				// again together with the digits after it.
				{Path: `/mnt/literal\040`, Opts: map[string]string{"rw": ""}},
			},
			expFsPaths: []string{"beegfs"},
		},
	}

	for _, test := range tests {
		nodes, fsPaths, err := parseMounts(strings.NewReader(test.input))
		assert.NoError(t, err)
		assert.Equal(t, test.expected, nodes)
		assert.ElementsMatch(t, test.expFsPaths, fsPaths)
	}
}

// fakeMountIDs returns a stand-in for ioctl.GetMountID() that answers from ids. A path missing
// from ids fails, like a mount point the caller cannot open.
func fakeMountIDs(ids map[string]string) func(string) (string, error) {
	return func(dirPath string) (string, error) {
		if id, ok := ids[dirPath]; ok {
			return id, nil
		}
		return "", syscall.EACCES
	}
}

func TestMountIndex(t *testing.T) {
	sharedCfg := map[string]string{"cfgFile": "/etc/beegfs/beegfs-client.conf"}
	otherCfg := map[string]string{"cfgFile": "/etc/beegfs/other-client.conf"}

	type lookup struct {
		mountID   string
		cfgFile   string
		wantPath  string
		wantFound bool
	}
	tests := []struct {
		name     string
		mounts   []MountPoint
		mountIDs map[string]string
		lookups  []lookup
	}{
		{
			name: "mounts that share a config file are told apart by mount ID",
			mounts: []MountPoint{
				{Path: "/mnt/beegfs1", Opts: sharedCfg},
				{Path: "/mnt/beegfs2", Opts: sharedCfg},
			},
			mountIDs: map[string]string{"/mnt/beegfs1": "c1", "/mnt/beegfs2": "c2"},
			lookups: []lookup{
				{mountID: "c1", cfgFile: sharedCfg["cfgFile"], wantPath: "/mnt/beegfs1", wantFound: true},
				{mountID: "c2", cfgFile: sharedCfg["cfgFile"], wantPath: "/mnt/beegfs2", wantFound: true},
			},
		},
		{
			name: "a mount without a mount ID is matched by a config file no other such mount uses",
			mounts: []MountPoint{
				{Path: "/mnt/beegfs1", Opts: sharedCfg},
				{Path: "/mnt/beegfs2", Opts: sharedCfg},
			},
			mountIDs: map[string]string{"/mnt/beegfs1": "c1"},
			lookups: []lookup{
				{mountID: "c1", cfgFile: sharedCfg["cfgFile"], wantPath: "/mnt/beegfs1", wantFound: true},
				{mountID: "c2", cfgFile: sharedCfg["cfgFile"], wantPath: "/mnt/beegfs2", wantFound: true},
			},
		},
		{
			name: "mounts without a mount ID that share a config file are not guessed",
			mounts: []MountPoint{
				{Path: "/mnt/beegfs1", Opts: sharedCfg},
				{Path: "/mnt/beegfs2", Opts: sharedCfg},
				{Path: "/mnt/other", Opts: otherCfg},
			},
			mountIDs: map[string]string{},
			lookups: []lookup{
				{mountID: "c1", cfgFile: sharedCfg["cfgFile"], wantFound: false},
				{mountID: "c3", cfgFile: otherCfg["cfgFile"], wantPath: "/mnt/other", wantFound: true},
			},
		},
		{
			name: "a bind mount keeps the original mount point",
			mounts: []MountPoint{
				{Path: "/mnt/beegfs", Opts: sharedCfg},
				{Path: "/srv/bind", Opts: sharedCfg},
			},
			mountIDs: map[string]string{"/mnt/beegfs": "c1", "/srv/bind": "c1"},
			lookups: []lookup{
				{mountID: "c1", cfgFile: sharedCfg["cfgFile"], wantPath: "/mnt/beegfs", wantFound: true},
			},
		},
		{
			name:     "a client without a mount is not matched",
			mounts:   []MountPoint{{Path: "/mnt/beegfs", Opts: sharedCfg}},
			mountIDs: map[string]string{"/mnt/beegfs": "c1"},
			lookups: []lookup{
				{mountID: "c2", cfgFile: sharedCfg["cfgFile"], wantFound: false},
			},
		},
	}

	log := &logger.Logger{Logger: zap.NewNop()}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index := newMountIndex(test.mounts, fakeMountIDs(test.mountIDs), log)
			for _, l := range test.lookups {
				m, found := index.mountFor(l.mountID, l.cfgFile)
				assert.Equal(t, l.wantFound, found, "mount ID %s", l.mountID)
				assert.Equal(t, l.wantPath, m.Path, "mount ID %s", l.mountID)
			}
		})
	}
}

func TestFilterClients(t *testing.T) {
	c1 := Client{ID: "c1", FsUUID: "fs-a", Mount: MountPoint{Path: "/mnt/beegfs1"}}
	// c2 could not be linked to its mount, for example because its mount point was not readable
	// while the procfs directory was parsed.
	c2 := Client{ID: "c2", FsUUID: "fs-a"}
	c3 := Client{ID: "c3", FsUUID: "fs-b", Mount: MountPoint{Path: "/mnt/other"}}
	clients := []Client{c1, c2, c3}
	mountIDs := map[string]string{
		"/mnt/beegfs1":        "c1",
		"/mnt/beegfs1/subdir": "c1",
		"/mnt/beegfs2":        "c2",
	}

	tests := []struct {
		name string
		cfg  GetBeeGFSClientsConfig
		want []Client
	}{
		{
			name: "no filters",
			cfg:  GetBeeGFSClientsConfig{},
			want: clients,
		},
		{
			name: "a mount path matches its client by mount ID",
			cfg:  GetBeeGFSClientsConfig{FilterByMounts: []string{"/mnt/beegfs2"}},
			want: []Client{c2},
		},
		{
			name: "a directory inside a mount matches the client of that mount",
			cfg:  GetBeeGFSClientsConfig{FilterByMounts: []string{"/mnt/beegfs1/subdir"}},
			want: []Client{c1},
		},
		{
			name: "a path without a mount ID must equal a client's mount point",
			cfg:  GetBeeGFSClientsConfig{FilterByMounts: []string{"/mnt/other/", "/mnt/unknown"}},
			want: []Client{c3},
		},
		{
			name: "the UUID filter applies before the mount filter",
			cfg:  GetBeeGFSClientsConfig{FilterByUUID: "fs-a", FilterByMounts: []string{"/mnt/other"}},
			want: []Client{},
		},
		{
			name: "the UUID filter alone",
			cfg:  GetBeeGFSClientsConfig{FilterByUUID: "fs-a"},
			want: []Client{c1, c2},
		},
	}

	log := &logger.Logger{Logger: zap.NewNop()}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := filterClients(clients, test.cfg, fakeMountIDs(mountIDs), log)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestParseClientFsUUID(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{input: "0b8a6f1e-3c2d-4e5f-8a9b-1c2d3e4f5a6b\n", want: "0b8a6f1e-3c2d-4e5f-8a9b-1c2d3e4f5a6b"},
		{input: "(null)\n", want: ""},
		{input: "", want: ""},
	}
	for _, test := range tests {
		got, err := parseClientFsUUID(strings.NewReader(test.input))
		assert.NoError(t, err)
		assert.Equal(t, test.want, got)
	}
}

func TestParseMountInfo(t *testing.T) {
	input := `
43 1 0:36 /root / rw,relatime shared:1 - btrfs /dev/mapper/root rw,seclabel
32 43 0:69 / /mnt/beegfs rw,relatime shared:617 master:2 - beegfs beegfs_nodev rw,cfgFile=/etc/beegfs/beegfs-client.conf

1097 43 0:81 / /mnt/tmp\040beegfs rw,relatime - beegfs beegfs_nodev rw
`
	table, err := parseMountInfo(strings.NewReader(input))
	require.NoError(t, err)
	assert.Equal(t, []mountInfo{
		{id: 43, dev: "0:36", mountPoint: "/", fsType: "btrfs"},
		{id: 32, dev: "0:69", mountPoint: "/mnt/beegfs", fsType: "beegfs"},
		{id: 1097, dev: "0:81", mountPoint: "/mnt/tmp beegfs", fsType: "beegfs"},
	}, table)

	for _, malformed := range []string{
		"43 1 0:36 / / rw shared:1 btrfs /dev/root rw",
		"43 1 0:36 / / rw -",
		"x 1 0:36 / / rw - btrfs /dev/root rw",
	} {
		_, err := parseMountInfo(strings.NewReader(malformed))
		assert.Error(t, err, malformed)
	}
}

func TestParseFdinfoMountID(t *testing.T) {
	id, err := parseFdinfoMountID(strings.NewReader("pos:\t0\nflags:\t012200000\nmnt_id:\t42\nino:\t1\n"))
	require.NoError(t, err)
	assert.Equal(t, 42, id)

	_, err = parseFdinfoMountID(strings.NewReader("pos:\t0\nflags:\t012200000\n"))
	assert.ErrorContains(t, err, "no mnt_id")

	_, err = parseFdinfoMountID(strings.NewReader("mnt_id:\tx\n"))
	assert.Error(t, err)
}

func TestClientOfMount(t *testing.T) {
	// A root filesystem, two BeeGFS mounts, and bind mounts:
	//   - /mnt/beegfs/scratch bound at /scratch.
	//   - The local /opt/sbin bound at /mnt/beegfs/shared-software.
	//   - /mnt/old-beegfs bound at /mnt/beegfs/archived.
	table := []mountInfo{
		{id: 10, dev: "253:0", mountPoint: "/", fsType: "xfs"},
		{id: 20, dev: "0:69", mountPoint: "/mnt/beegfs", fsType: "beegfs"},
		{id: 21, dev: "0:70", mountPoint: "/mnt/old-beegfs", fsType: "beegfs"},
		{id: 30, dev: "0:69", mountPoint: "/scratch", fsType: "beegfs"},
		{id: 31, dev: "253:0", mountPoint: "/mnt/beegfs/shared-software", fsType: "xfs"},
		{id: 32, dev: "0:70", mountPoint: "/mnt/beegfs/archived", fsType: "beegfs"},
	}
	current := Client{ID: "c-cur", Mount: MountPoint{Path: "/mnt/beegfs"}}
	old := Client{ID: "c-old", Mount: MountPoint{Path: "/mnt/old-beegfs"}}
	clients := []Client{current, old}

	tests := []struct {
		name    string
		mountID int
		wantID  string
	}{
		{name: "the BeeGFS mount", mountID: 20, wantID: "c-cur"},
		{name: "a bind mount of BeeGFS", mountID: 30, wantID: "c-cur"},
		{name: "a local filesystem bound inside BeeGFS", mountID: 31, wantID: ""},
		{name: "another BeeGFS bound inside BeeGFS", mountID: 32, wantID: "c-old"},
		{name: "the root filesystem", mountID: 10, wantID: ""},
		{name: "a mount ID not in the table", mountID: 99, wantID: ""},
	}
	for _, test := range tests {
		c, ok := clientOfMount(clients, table, test.mountID)
		assert.Equal(t, test.wantID != "", ok, test.name)
		assert.Equal(t, test.wantID, c.ID, test.name)
	}

	// NFS stacked on /mnt/beegfs hides it. A directory on the NFS mount selects nothing. A shell
	// that was in /mnt/beegfs before NFS was mounted still has a directory on BeeGFS.
	nfsOnTop := append(slices.Clone(table), mountInfo{id: 41, dev: "0:50", mountPoint: "/mnt/beegfs", fsType: "nfs4"})
	_, ok := clientOfMount(clients, nfsOnTop, 41)
	assert.False(t, ok, "NFS on top of BeeGFS")
	c, ok := clientOfMount(clients, nfsOnTop, 20)
	assert.True(t, ok, "BeeGFS below NFS")
	assert.Equal(t, "c-cur", c.ID)

	// BeeGFS mounted on top of NFS at the same mount point. The NFS mount does not count.
	beegfsOnTop := []mountInfo{
		{id: 10, dev: "253:0", mountPoint: "/", fsType: "xfs"},
		{id: 50, dev: "0:50", mountPoint: "/mnt/beegfs", fsType: "nfs4"},
		{id: 51, dev: "0:69", mountPoint: "/mnt/beegfs", fsType: "beegfs"},
	}
	c, ok = clientOfMount(clients, beegfsOnTop, 51)
	assert.True(t, ok, "BeeGFS on top of NFS")
	assert.Equal(t, "c-cur", c.ID)

	// fs-top is stacked on /mnt/beegfs. The mount point cannot tell the two clients apart.
	stacked := append(slices.Clone(table), mountInfo{id: 40, dev: "0:71", mountPoint: "/mnt/beegfs", fsType: "beegfs"})
	top := Client{ID: "c-top", Mount: MountPoint{Path: "/mnt/beegfs"}}
	_, ok = clientOfMount([]Client{current, top}, stacked, 40)
	assert.False(t, ok, "stacked BeeGFS mounts match nothing")
}

func TestBeeGFSDevsAt(t *testing.T) {
	table := []mountInfo{
		{id: 20, dev: "0:69", mountPoint: "/mnt/beegfs", fsType: "beegfs"},
		{id: 21, dev: "0:69", mountPoint: "/mnt/beegfs", fsType: "beegfs"},
		{id: 22, dev: "0:90", mountPoint: "/mnt/beegfs", fsType: "tmpfs"},
		{id: 23, dev: "0:70", mountPoint: "/mnt/stacked", fsType: "beegfs"},
		{id: 24, dev: "0:71", mountPoint: "/mnt/stacked", fsType: "beegfs"},
	}
	assert.Equal(t, []string{"0:69"}, beegfsDevsAt(table, "/mnt/beegfs"), "duplicates and other filesystems are left out")
	assert.Equal(t, []string{"0:70", "0:71"}, beegfsDevsAt(table, "/mnt/stacked"))
	assert.Empty(t, beegfsDevsAt(table, "/mnt/none"))
}

// TestClientOfWorkingDir runs against the real kernel files. A temporary directory is not in a
// BeeGFS mount, so no client matches, but the fdinfo and mountinfo of this kernel must parse.
func TestClientOfWorkingDir(t *testing.T) {
	t.Chdir(t.TempDir())
	_, ok, err := ClientOfWorkingDir([]Client{{ID: "c", Mount: MountPoint{Path: "/mnt/beegfs"}}})
	require.NoError(t, err)
	assert.False(t, ok)
}
