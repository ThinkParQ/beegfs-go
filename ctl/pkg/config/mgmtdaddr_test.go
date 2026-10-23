package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
	"go.uber.org/zap"
)

var testLog = &logger.Logger{Logger: zap.NewNop()}

func TestMgmtdHostFromCfgFile(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "hostname", content: "sysMgmtdHost = mgmt01\n", want: "mgmt01"},
		{name: "no spaces", content: "sysMgmtdHost=mgmt01\n", want: "mgmt01"},
		{name: "comments are ignored", content: "# sysMgmtdHost = old\nsysMgmtdHost = mgmt01\n", want: "mgmt01"},
		{name: "the last definition wins", content: "sysMgmtdHost = mgmt01\nsysMgmtdHost = mgmt02\n", want: "mgmt02"},
		{name: "unset like in the shipped config file", content: "sysMgmtdHost =\nconnMgmtdPort = 8008\n", want: ""},
		{name: "other keys are ignored", content: "connMgmtdPort = 8008\n", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "beegfs-client.conf")
			require.NoError(t, os.WriteFile(path, []byte(test.content), 0644))
			assert.Equal(t, test.want, mgmtdHostFromCfgFile(path, testLog))
		})
	}
	t.Run("missing file", func(t *testing.T) {
		assert.Equal(t, "", mgmtdHostFromCfgFile(filepath.Join(t.TempDir(), "missing.conf"), testLog))
	})
	t.Run("no config file", func(t *testing.T) {
		assert.Equal(t, "", mgmtdHostFromCfgFile("", testLog))
	})
	t.Run("read error", func(t *testing.T) {
		// A directory opens fine but fails on the first read.
		assert.Equal(t, "", mgmtdHostFromCfgFile(t.TempDir(), testLog))
	})
}

func TestSelectedMount(t *testing.T) {
	t.Cleanup(viper.Reset)
	t.Cleanup(func() { globalMount = nil })

	mount, selectedBy, err := selectedMount()
	require.NoError(t, err, "an unset --mount selects nothing")
	assert.Equal(t, "", mount)
	assert.Equal(t, "", selectedBy)

	// Without --mount, the mount that BeeGFSClient() resolved from a path selects the filesystem.
	globalMount = filesystem.BeeGFS{MountPoint: "/mnt/fs2"}
	mount, selectedBy, err = selectedMount()
	require.NoError(t, err)
	assert.Equal(t, "/mnt/fs2", mount)
	assert.Equal(t, "the mount of the given path (/mnt/fs2)", selectedBy)

	// --mount wins over a resolved mount.
	viper.Set(BeeGFSMountPointKey, "/mnt/beegfs")
	mount, selectedBy, err = selectedMount()
	require.NoError(t, err)
	assert.Equal(t, "/mnt/beegfs", mount)
	assert.Equal(t, "--mount /mnt/beegfs", selectedBy)

	viper.Set(BeeGFSMountPointKey, BeeGFSMountPointNone)
	mount, _, err = selectedMount()
	require.NoError(t, err, "--mount none selects nothing")
	assert.Equal(t, "", mount)

	for _, notAbsolute := range []string{"mnt/beegfs", "auto", ""} {
		viper.Set(BeeGFSMountPointKey, notAbsolute)
		_, _, err = selectedMount()
		assert.ErrorIs(t, err, errMountNotAbsolute, "--mount %q", notAbsolute)
	}
}

func TestFsUUIDsOf(t *testing.T) {
	tests := []struct {
		name      string
		fsUUIDs   []string
		wantUUIDs []string
	}{
		{name: "no clients", fsUUIDs: nil, wantUUIDs: nil},
		{name: "one file system mounted twice", fsUUIDs: []string{"fs-a", "fs-a"}, wantUUIDs: []string{"fs-a"}},
		{name: "two file systems", fsUUIDs: []string{"fs-a", "fs-b", "fs-b"}, wantUUIDs: []string{"fs-a", "fs-b"}},
		{name: "an unregistered client is skipped", fsUUIDs: []string{"", "fs-a"}, wantUUIDs: []string{"fs-a"}},
		{name: "only unregistered clients", fsUUIDs: []string{""}, wantUUIDs: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clients := []procfs.Client{}
			for _, uuid := range test.fsUUIDs {
				clients = append(clients, procfs.Client{FsUUID: uuid})
			}
			assert.Equal(t, test.wantUUIDs, fsUUIDsOf(clients))
		})
	}
}

func TestRegisteredClientOf(t *testing.T) {
	registered := procfs.Client{ID: "c1", FsUUID: "fs-a", Mount: procfs.MountPoint{Path: "/mnt/beegfs"}}
	unregistered := procfs.Client{ID: "c2", Mount: procfs.MountPoint{Path: "/mnt/beegfs"}}
	otherFs := procfs.Client{ID: "c3", FsUUID: "fs-b", Mount: procfs.MountPoint{Path: "/mnt/other"}}

	c, err := registeredClientOf([]procfs.Client{unregistered, registered})
	require.NoError(t, err, "the first registered client selects the filesystem")
	assert.Equal(t, "c1", c.ID)

	_, err = registeredClientOf(nil)
	assert.ErrorIs(t, err, errNoClient)

	_, err = registeredClientOf([]procfs.Client{unregistered})
	assert.ErrorIs(t, err, errNotRegistered)

	_, err = registeredClientOf([]procfs.Client{registered, otherFs})
	assert.ErrorIs(t, err, errSeveralFs)
}

func TestWhyNoClientSelected(t *testing.T) {
	clients := []procfs.Client{{ID: "c1", FsUUID: "fs-a", Mount: procfs.MountPoint{Path: "/mnt/beegfs"}}}
	tests := []struct {
		err        error
		selectedBy string
		want       string
	}{
		{err: errNoClient, selectedBy: "", want: "BeeGFS does not appear to be mounted"},
		{err: errSeveralFs, selectedBy: "", want: "more than one BeeGFS filesystem is mounted (beegfs://<unknown> (FsUUID: fs-a) @ /mnt/beegfs (Client ID: c1)), specify --mount <path>"},
		{err: errNotRegistered, selectedBy: "", want: "no BeeGFS client on this machine has registered"},
		{err: errNoClient, selectedBy: "--mount /tmp", want: "no BeeGFS client serves --mount /tmp"},
		{err: errSeveralFs, selectedBy: "--mount /mnt/beegfs", want: "more than one BeeGFS filesystem matches --mount /mnt/beegfs"},
		{err: errNotRegistered, selectedBy: "the mount of the given path (/mnt/beegfs)", want: "the BeeGFS client that serves the mount of the given path (/mnt/beegfs) has not registered"},
	}
	for _, test := range tests {
		assert.Contains(t, whyNoClientSelected(test.err, test.selectedBy, clients, testLog), test.want, "%v with selectedBy %q", test.err, test.selectedBy)
	}
}

func TestFirstRegisteredClient(t *testing.T) {
	unregistered := procfs.Client{ID: "c0"}
	registered := procfs.Client{ID: "c1", FsUUID: "fs-a"}
	alsoRegistered := procfs.Client{ID: "c2", FsUUID: "fs-a"}

	c, ok := firstRegisteredClient([]procfs.Client{unregistered, registered, alsoRegistered})
	assert.True(t, ok)
	assert.Equal(t, "c1", c.ID)

	_, ok = firstRegisteredClient([]procfs.Client{unregistered})
	assert.False(t, ok)

	_, ok = firstRegisteredClient(nil)
	assert.False(t, ok)
}

func TestKernelMgmtdAddr(t *testing.T) {
	c := procfs.Client{
		ProcDir: "/proc/fs/beegfs/c1",
		Config:  map[string]string{procfsMgmtdHost: "192.167.1.101", procfsMgmtdGrpc: "8010"},
	}
	host, port, err := kernelMgmtdAddr(c)
	require.NoError(t, err)
	assert.Equal(t, "192.167.1.101", host)
	assert.Equal(t, "8010", port)

	delete(c.Config, procfsMgmtdGrpc)
	_, _, err = kernelMgmtdAddr(c)
	assert.ErrorContains(t, err, "does not appear to contain a connMgmtdGrpcPort")

	delete(c.Config, procfsMgmtdHost)
	_, _, err = kernelMgmtdAddr(c)
	assert.ErrorContains(t, err, "does not appear to contain a sysMgmtdHost")
}

func TestDescribeMounts(t *testing.T) {
	cfgFile := filepath.Join(t.TempDir(), "beegfs-client.conf")
	require.NoError(t, os.WriteFile(cfgFile, []byte("sysMgmtdHost = localhost\n"), 0644))
	procfsConfig := map[string]string{procfsMgmtdHost: "127.0.0.1", procfsMgmtdGrpc: "8010"}

	clients := []procfs.Client{
		// The config file's host is shown, because --mgmtd-addr would need it for TLS.
		{
			ID:     "c0",
			FsUUID: "fs-a",
			Mount:  procfs.MountPoint{Path: "/mnt/beegfs"},
			Config: map[string]string{procfsMgmtdHost: "127.0.0.1", procfsMgmtdGrpc: "8010", procfsCfgFile: cfgFile},
		},
		// Without a config file host the procfs IP is shown, and an unknown mount point is marked.
		{ID: "c1", FsUUID: "fs-b", Config: procfsConfig},
		// An unregistered client, and one whose procfs config lacks the management keys.
		{ID: "c2", Mount: procfs.MountPoint{Path: "/mnt/new"}, Config: procfsConfig},
		{ID: "c3", FsUUID: "fs-c", Mount: procfs.MountPoint{Path: "/mnt/odd"}, Config: map[string]string{}},
	}
	assert.Equal(t,
		"beegfs://localhost:8010 (FsUUID: fs-a) @ /mnt/beegfs (Client ID: c0), "+
			"beegfs://127.0.0.1:8010 (FsUUID: fs-b) @ <unknown> (Client ID: c1), "+
			"beegfs://127.0.0.1:8010 (FsUUID: <not registered>) @ /mnt/new (Client ID: c2), "+
			"beegfs://<unknown> (FsUUID: fs-c) @ /mnt/odd (Client ID: c3)",
		describeMounts(clients, testLog))
}
