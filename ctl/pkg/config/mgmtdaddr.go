package config

// This file holds the helpers ManagementClient() uses to pick the management node. Each helper
// fetches or checks one fact. ManagementClient() applies the rules.
//
// The user can set the address with --mgmtd-addr, or leave it at "auto" to take it from the BeeGFS
// clients mounted on this machine. --mount selects which client to take it from. Without --mount,
// the mount of a path argument selects it, or else the mount that holds the current directory.
//
// Facts about the BeeGFS client that the rules rely on:
//
//   - The mount helper (mount.beegfs) resolves sysMgmtdHost to an IP. It passes the IP to the
//     kernel as a mount option, and a mount option overrides the config file. So the
//     sysMgmtdHost that procfs shows is the IP the kernel actually uses.
//   - The sysMgmtdHost in the config file is what the admin wrote, usually a hostname. TLS needs
//     that hostname when the management's certificate does not list the IP. But the kernel may
//     not use it. A sysMgmtdHost mount option wins over the file, and the file may have changed
//     since the mount.
//   - The client learns its filesystem UUID and the management's gRPC port when it registers
//     with the management node. procfs shows both. The GetNodes RPC returns the same UUID.
//
// So an auto-configured address uses the host from the config file. After connecting, the file
// system UUID shows whether that host serves the filesystem of the mount. ManagementClient()
// dials the procfs IP only when it does not.
//
// Where each value comes from, and what ManagementClient() uses it for:
//
//	Value                        Set by                                Used for
//	sysMgmtdHost in config file  the admin                             first address dialed
//	sysMgmtdHost in procfs       mount.beegfs, as a resolved IP        address dialed on a mismatch
//	connMgmtdGrpcPort in procfs  the management node, at registration  port of both addresses
//	fs_uuid in procfs            the management node, at registration  UUID the node must serve
//	fs_uuid from GetNodes        the management node                   UUID the node does serve
//
// Terms used here and in ManagementClient():
//
//   - A "client" is one BeeGFS client instance in procfs. Each mount runs its own instance.
//   - The "kernel address" of a client is its procfs sysMgmtdHost and connMgmtdGrpcPort. Only the
//     IP is what the kernel module uses: it reaches the management node there over BeeMsg on
//     connMgmtdPort. The kernel module never connects to the gRPC port. It only stores the port
//     the management node reports at registration.
//   - A client is "registered" once it has its filesystem UUID from the management node.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
	"go.uber.org/zap"
)

// BeeGFS procfs configuration keys.
const (
	procfsMgmtdHost = "sysMgmtdHost"
	procfsMgmtdGrpc = "connMgmtdGrpcPort"
	procfsCfgFile   = "cfgFile"
	procfsAuthFile  = "connAuthFile"
)

// Formats for selectedBy, which names in messages the input that selected a mount.
const (
	selectedByMountFlag = "--" + BeeGFSMountPointKey + " %s"
	selectedByPathArg   = "the mount of the given path (%s)"
)

// errMountNotAbsolute is returned for a --mount that is set, but is neither "none" nor an absolute
// path. ManagementClient() and BeeGFSClient() both reject such a value with this error.
var errMountNotAbsolute = fmt.Errorf("the specified value for %s does not appear to be an absolute path", BeeGFSMountPointKey)

// registeredClientOf returns one of these when the clients do not select a filesystem.
var (
	errNoClient      = errors.New("no BeeGFS client")
	errSeveralFs     = errors.New("clients of more than one BeeGFS filesystem")
	errNotRegistered = errors.New("no registered BeeGFS client")
)

// selectedMount returns the mount that selects the filesystem to manage, and selectedBy, which
// names that mount's source in messages. It runs when ManagementClient() sets up the management
// client. The mount is one of:
//   - --mount, when it is an absolute path.
//   - Else the mount that BeeGFSClient() already resolved from a path argument.
//   - Else "", when no mount selects a filesystem. That includes --mount "none".
//
// Any other --mount value is an error, so a mistyped --mount is never silently ignored.
//
// It reads globalMount without a lock. That is safe because globalMount is set once, by
// BeeGFSClient(), before any command starts parallel work, and is read-only afterwards.
func selectedMount() (mount string, selectedBy string, err error) {
	if viper.IsSet(BeeGFSMountPointKey) {
		mount = viper.GetString(BeeGFSMountPointKey)
		if mount == BeeGFSMountPointNone {
			return "", "", nil
		}
		if !filepath.IsAbs(mount) {
			return "", "", errMountNotAbsolute
		}
		return mount, fmt.Sprintf(selectedByMountFlag, mount), nil
	}
	if resolved, ok := globalMount.(filesystem.BeeGFS); ok {
		return resolved.GetMountPath(), fmt.Sprintf(selectedByPathArg, resolved.GetMountPath()), nil
	}
	// No --mount was given, and BeeGFSClient() has not resolved a mount yet. The flag's default
	// "auto" counts as not given, because viper.IsSet() ignores flag defaults. An explicit
	// --mount auto is not absolute, so it was rejected above.
	return "", "", nil
}

// fsUUIDsOf returns the distinct filesystem UUIDs of the registered clients, in the order found.
// Clients that are not registered have no UUID and are skipped.
func fsUUIDsOf(clients []procfs.Client) []string {
	var uuids []string
	seen := make(map[string]struct{})
	for _, c := range clients {
		if c.FsUUID == "" {
			continue
		}
		if _, ok := seen[c.FsUUID]; !ok {
			seen[c.FsUUID] = struct{}{}
			uuids = append(uuids, c.FsUUID)
		}
	}
	return uuids
}

// registeredClientOf returns the client that selects the filesystem among clients. clients are
// either every client in procfs, or only those of one mount. It returns a sentinel error when the
// clients do not select one filesystem:
//   - errNoClient: there are no clients. Either nothing is mounted, the path is not inside a
//     mounted BeeGFS, or CTL cannot read /proc/fs/beegfs where it runs.
//   - errSeveralFs: the registered clients belong to more than one filesystem. The UUIDs decide,
//     so mounts of one filesystem may reach the management node through different addresses.
//   - errNotRegistered: no client has registered with its management node yet. Only a registered
//     client has a UUID that can be checked against the management node.
//
// Otherwise it returns the first registered client, in procfs order.
func registeredClientOf(clients []procfs.Client) (procfs.Client, error) {
	if len(clients) == 0 {
		return procfs.Client{}, errNoClient
	}
	if len(fsUUIDsOf(clients)) > 1 {
		return procfs.Client{}, errSeveralFs
	}
	c, ok := firstRegisteredClient(clients)
	if !ok {
		return procfs.Client{}, errNotRegistered
	}
	return c, nil
}

// whyNoClientSelected explains a registeredClientOf() error for messages, with advice on what to
// do. selectedBy names the mount the clients were filtered by, or is "" when no mount was.
func whyNoClientSelected(err error, selectedBy string, clients []procfs.Client, log *logger.Logger) string {
	switch {
	case selectedBy == "" && errors.Is(err, errNoClient):
		return fmt.Sprintf("BeeGFS does not appear to be mounted, manually specify --%s <hostname|ip>:<grpc-port> for the filesystem to manage", ManagementAddrKey)
	case selectedBy == "" && errors.Is(err, errSeveralFs):
		return fmt.Sprintf("more than one BeeGFS filesystem is mounted (%s), specify --%s <path> to select one or manually specify --%s <hostname|ip>:<grpc-port> for the filesystem to manage", describeMounts(clients, log), BeeGFSMountPointKey, ManagementAddrKey)
	case selectedBy == "" && errors.Is(err, errNotRegistered):
		return fmt.Sprintf("no BeeGFS client on this machine has registered with the management node yet, check the management node is responding or manually specify --%s <hostname|ip>:<grpc-port> for the filesystem to manage", ManagementAddrKey)
	case errors.Is(err, errNoClient):
		return fmt.Sprintf("no BeeGFS client serves %s, specify a path inside a mounted BeeGFS", selectedBy)
	case errors.Is(err, errSeveralFs):
		return fmt.Sprintf("more than one BeeGFS filesystem matches %s (%s), specify the mount point of the filesystem to manage", selectedBy, describeMounts(clients, log))
	case errors.Is(err, errNotRegistered):
		return fmt.Sprintf("the BeeGFS client that serves %s has not registered with the management node yet, check the management node is responding", selectedBy)
	}
	return err.Error()
}

// firstRegisteredClient returns the first registered client in clients, in the order given. It
// returns false when no client is registered.
func firstRegisteredClient(clients []procfs.Client) (procfs.Client, bool) {
	for _, c := range clients {
		if c.FsUUID != "" {
			return c, true
		}
	}
	return procfs.Client{}, false
}

// registeredClientOfWorkingDir runs in ManagementClient() when the address is automatic and
// nothing selects a mount. It returns the registered client whose filesystem holds the current
// directory, so a user can pick a filesystem by working inside it. procfs.ClientOfWorkingDir()
// finds the client without calling into the filesystem. It is best effort. It returns false when
// the directory is not in a mount of a registered client, or on any error, and the caller then
// uses all clients.
func registeredClientOfWorkingDir(clients []procfs.Client, log *logger.Logger) (procfs.Client, bool) {
	c, ok, err := procfs.ClientOfWorkingDir(clients)
	switch {
	case err != nil:
		log.Debug("unable to find the mount of the current directory, so it does not select a filesystem", zap.Error(err))
		return procfs.Client{}, false
	case !ok:
		log.Debug("the current directory is not in a mount of a BeeGFS client, so it does not select a filesystem")
		return procfs.Client{}, false
	case c.FsUUID == "":
		log.Debug("the BeeGFS client of the current directory has not registered, so it does not select a filesystem", zap.String("mountPoint", c.Mount.Path))
		return procfs.Client{}, false
	}
	log.Debug("the current directory selects the filesystem", zap.String("mountPoint", c.Mount.Path), zap.String("fsUUID", c.FsUUID))
	return c, true
}

// kernelMgmtdAddr returns the host and gRPC port of the kernel address of a client. procfs always
// shows both keys, so a missing key is an error.
func kernelMgmtdAddr(c procfs.Client) (host string, port string, err error) {
	host, ok := c.Config[procfsMgmtdHost]
	if !ok {
		return "", "", fmt.Errorf("unable to auto-configure the management address: configuration at %s/config does not appear to contain a %s, manually specify --%s <hostname|ip>:<grpc-port> for the filesystem to manage (this is likely a bug)", c.ProcDir, procfsMgmtdHost, ManagementAddrKey)
	}
	port, ok = c.Config[procfsMgmtdGrpc]
	if !ok {
		return "", "", fmt.Errorf("unable to auto-configure the management address: configuration at %s/config does not appear to contain a %s, manually specify --%s <hostname|ip>:<grpc-port> for the filesystem to manage (this is likely a bug)", c.ProcDir, procfsMgmtdGrpc, ManagementAddrKey)
	}
	return host, port, nil
}

// mgmtdHostFromCfgFile returns the sysMgmtdHost set in a client config file. It returns "" when
// the client has no config file, the file cannot be read, or it does not set sysMgmtdHost. When
// the key appears more than once, the last one wins.
func mgmtdHostFromCfgFile(cfgFilePath string, log *logger.Logger) string {
	if cfgFilePath == "" {
		log.Debug("client mount does not use a config file, falling back to use the management IP address from procfs")
		return ""
	}
	f, err := os.Open(cfgFilePath)
	if err != nil {
		log.Debug("unable to open client config file from procfs, ignoring and falling back to use the management IP address from procfs", zap.String(procfsCfgFile, cfgFilePath), zap.Error(err))
		return ""
	}
	defer f.Close()

	host := ""
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Ignore comments and empty lines
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if found && strings.TrimSpace(key) == procfsMgmtdHost {
			host = strings.TrimSpace(value)
		}
	}
	if err := scanner.Err(); err != nil {
		log.Debug("unable to read client config file from procfs, ignoring and falling back to use the management IP address from procfs", zap.String(procfsCfgFile, cfgFilePath), zap.Error(err))
		return ""
	}
	if host == "" {
		log.Debug(fmt.Sprintf("client config file does not appear to set %s, ignoring and falling back to use the management IP address from procfs", procfsMgmtdHost), zap.String(procfsCfgFile, cfgFilePath))
	}
	return host
}

// mountLocation returns the mount point of a client for messages, or "<unknown>" when the client
// could not be linked to its mount point.
func mountLocation(c procfs.Client) string {
	if c.Mount.Path != "" {
		return c.Mount.Path
	}
	return "<unknown>"
}

// describeMount names a client for messages as "<mount point> (Client ID: <id>)". The client ID
// is the name of the client's procfs directory, which identifies it when the mount point is
// unknown.
func describeMount(c procfs.Client) string {
	return fmt.Sprintf("%s (Client ID: %s)", mountLocation(c), c.ID)
}

// describeMounts lists clients for messages, one entry per client in the form
// "beegfs://<mgmtd-addr> (FsUUID: <uuid>) @ <mount point> (Client ID: <id>)".
//
// The address is the one --mgmtd-addr needs to manage that filesystem. It is the config file's
// sysMgmtdHost when the file sets one, otherwise the procfs IP, with the gRPC port from procfs.
// That is the same preference ManagementClient() applies when it auto-configures the address.
func describeMounts(clients []procfs.Client, log *logger.Logger) string {
	mounts := make([]string, 0, len(clients))
	for _, c := range clients {
		addr := "<unknown>"
		if host, port, err := kernelMgmtdAddr(c); err == nil {
			addr = net.JoinHostPort(host, port)
			if cfgFileHost := mgmtdHostFromCfgFile(c.Config[procfsCfgFile], log); cfgFileHost != "" {
				addr = net.JoinHostPort(cfgFileHost, port)
			}
		}
		fsUUID := c.FsUUID
		if fsUUID == "" {
			fsUUID = "<not registered>"
		}
		mounts = append(mounts, fmt.Sprintf("beegfs://%s (FsUUID: %s) @ %s", addr, fsUUID, describeMount(c)))
	}
	return strings.Join(mounts, ", ")
}

// newMgmtdClient creates a management client for addr with the TLS settings from viper. Creating
// the client does not connect. The connection is made by the first RPC.
func newMgmtdClient(addr string, cert []byte, authSecret []byte) (*beegrpc.Mgmtd, error) {
	return beegrpc.NewMgmtd(
		addr,
		beegrpc.WithTLSDisable(viper.GetBool(TlsDisableKey)),
		beegrpc.WithTLSDisableVerification(viper.GetBool(TlsDisableVerificationKey)),
		beegrpc.WithTLSCaCert(cert),
		beegrpc.WithAuthSecret(authSecret),
		beegrpc.WithProxy(viper.GetBool(UseProxyKey)),
	)
}

// connectAndGetFsUUID creates a management client for addr and asks the node for the filesystem
// UUID it serves. That RPC is the first one, so it also makes the connection. On error the client
// is closed again and nil is returned.
func connectAndGetFsUUID(ctx context.Context, addr string, cert []byte, authSecret []byte) (*beegrpc.Mgmtd, string, error) {
	mgmtd, err := newMgmtdClient(addr, cert, authSecret)
	if err != nil {
		return nil, "", err
	}
	fsUUID, err := mgmtd.GetFsUUID(ctx)
	if err != nil {
		mgmtd.Cleanup()
		return nil, "", err
	}
	return mgmtd, fsUUID, nil
}
