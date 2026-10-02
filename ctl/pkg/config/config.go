package config

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/common/beemsg"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/registry"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
	pb "github.com/thinkparq/protobuf/go/beegfs"
	"github.com/thinkparq/protobuf/go/beeremote"
	pm "github.com/thinkparq/protobuf/go/management"
	"go.uber.org/zap"
)

// Viper keys for the global config. Should be used when accessing it instead of raw strings.
// Currently these are also used by the frontend for command line flag and env variable names.
const (
	// The gRPC listening address of the management. Note this defaults to auto, to reliably
	// determine the address call GetAddress() on the Mgmtd client returned by ManagementClient().
	ManagementAddrKey = "mgmtd-addr"
	// BeeRemotes gRPC listening address
	BeeRemoteAddrKey = "remote-addr"
	// A BeeGFS mount point on the local file system
	BeeGFSMountPointKey = "mount"
	// Use a mount even when CTL cannot verify which filesystem it belongs to, for example where CTL
	// cannot read /proc/fs/beegfs. Without it, such a mount is an error when it selects the
	// filesystem to manage. It never allows a mount that belongs to another filesystem than the
	// management node serves.
	AllowUnverifiedMountKey = "allow-unverified-mount"
	// The timeout for a single connection attempt
	ConnTimeoutKey = "conn-timeout"
	// Disable BeeMsg and gRPC client to server authentication
	AuthDisableKey = "auth-disable"
	// File containing the authentication secret (formerly known as "connAuthFile"). Generally
	// callers should not open and initialize the secret directly, but instead use
	// ManagementClient() then use getter methods on that client so the secret can be automatically
	// initialized from any client mounts if the default auth file path does not exist and a user
	// defined path was not provided. Other consumers of the auth secret such as the NodeStore and
	// Remote client also use the mgmtd client to automatically determine the correct auth secret.
	// Note if the auth path is automatically determined the path will be updated in Viper.
	AuthFileKey = "auth-file"
	// Disable TLS transport security for gRPC communication.
	TlsDisableKey = "tls-disable"
	// Disable TLS server verification for gRPC communication.
	TlsDisableVerificationKey = "tls-disable-verification"
	// Use a custom certificate for TLS server verification in addition to the system ones.
	TlsCertFile = "tls-cert-file"
	// Prints values in their raw, base form, without adding units and SI/IEC prefixes. Durations
	// excluded.
	RawKey = "raw"
	// Tells the command to print additional, normally hidden info. An example would be the entity
	// UIDs which currently are only used internally and hidden to avoid user confusion.
	DebugKey = "debug"
	// Disable emoji output in certain commands
	DisableEmojisKey = "disable-emojis"
	// The maximum number of workers to use when a command can complete work in parallel
	NumWorkersKey = "num-workers"
	// Set the log level (0 - least verbosity, 5 - highest verbosity).
	LogLevelKey = "log-level"
	// Sets up a reasonable default development logging configuration. Logging is enabled at
	// DebugLevel and above, and uses a console encoder. Logs are written to standard error.
	// Stacktraces are included on logs of WarnLevel and above. DPanicLevel logs will panic.
	LogDeveloperKey = "log-developer"
	// Start the pprof HTTP server at this address:port for performance debugging.
	PprofAddress = "pprof"
	// Print only the given columns of a table. Applied automatically when cmdfmt.NewTable() is used.
	// "all" prints all available columns, not only the default ones.
	ColumnsKey = "columns"
	// Determines the number of rows to be printed before the header is repeated. Also determines
	// how often output is actually flushed to stdout. Not applied automatically. If set to 0,
	// should not print a header at all and flush each row automatically (this requires NOT using
	// the go-pretty table printer and just print columns separated by spaces).
	PageSizeKey = "page-size"
	OutputKey   = "output"
	// Whether to use a proxy configured globally or via environment variables when making gRPC
	// connections to either mgmtd or remote.
	UseProxyKey   = "use-http-proxy"
	DisableAlerts = "disable-alerts"
)

// Viper values for certain configuration values.
const (
	BeeGFSMountPointNone  = "none"
	BeeGFSMgmtdAddrAuto   = "auto"
	BeeGFSAuthDefaultPath = "/etc/beegfs/conn.auth"
)

// OutputType is used to control what type of structured output should be printed.
type OutputType string

const (
	OutputTable      OutputType = "table"
	OutputJSON       OutputType = "json"
	OutputJSONPretty OutputType = "json-pretty"
	OutputNDJSON     OutputType = "ndjson"
	// When adding new output types, keep in mind both the Printomatic and commands with custom
	// output such as `health check` will need to be updated, at minimum to return an error if an
	// unsupported OutputType is used with those modes.
)

var (
	OutputOptions = []fmt.Stringer{OutputTable, OutputJSON, OutputJSONPretty, OutputNDJSON}
)

func (t OutputType) String() string {
	switch t {
	case OutputTable:
		return "table"
	case OutputJSON:
		return "json"
	case OutputJSONPretty:
		return "json-pretty"
	case OutputNDJSON:
		return "ndjson"
	default:
		return "unknown"
	}
}

// IsJSON reports whether the output type is one of the JSON-family formats (json, json-pretty, or
// ndjson) rather than the human-readable table. Useful for commands with custom output that branch
// between a human report and JSON.
func (t OutputType) IsJSON() bool {
	switch t {
	case OutputJSON, OutputJSONPretty, OutputNDJSON:
		return true
	default:
		return false
	}
}

// InitLoggerFromExternal allows CTL to use an externally configured/managed *logger.Logger. By
// default CTL uses an opinionated logger that is initialized on first call that logs to stderr
// only. This allows CTL logging to respect the logging configuration of the application that is
// using it including dynamic reconfiguration (like log level updates). This does not add any
// context to the logger and relies on the caller to do that if desired. For example:
//
//	ctl.InitLoggerFromExternal(logger.With(zap.String("component", "ctl")))
func InitLoggerFromExternal(log *logger.Logger) error {
	if globalLogger != nil {
		return fmt.Errorf("ctl logging was already initialized (this is probably a bug)")
	}
	globalLogger = log
	return nil
}

// GlobalConfig is used with InitViperFromExternal when the CTL backend is consumed as a library.
// While not all global configuration is applicable in this mode, it and InitViperFromExternal()
// should be kept in sync with any global configuration needed to use CTL as a library.
//
// If this evolves to include slices/maps/pointers where order/identity matters the method to
// determine if the config has changed (reflect.DeepEqual) in InitViperFromExternal must be updated.
type GlobalConfig struct {
	Mount                       string
	MgmtdAddress                string
	MgmtdTLSCertFile            string
	MgmtdTLSDisableVerification bool
	MgmtdTLSDisable             bool
	MgmtdUseProxy               bool
	AuthFile                    string
	AuthDisable                 bool
	RemoteAddress               string
	NumWorkers                  int
	ConnTimeoutMs               int
}

var alreadyInitViperFromExt bool
var globalCfg GlobalConfig
var ErrViperAlreadyInit = errors.New("reinitializing ctl config is not currently supported")

// InitViperFromExternal is used when the CTL backend is consumed as a library by applications other
// than the CTL CLI frontend. It is used to initialize the backend Viper config singleton from
// externally defined configuration. This approach gives callers flexibility in how they define
// equivalent configuration parameters (via flags, env variables, config files, etc) that are then
// passed through to CTL using the `GlobalConfig` struct.
//
// If the mount flag is empty then it will not be configured and is only needed when absolute paths
// are not used since BeeGFSClient will derive the mount path.
//
// This does not affect CTL logging. If you wish CTL to respect specific logging configuration call
// InitLoggerFromExternal() before calling any CTL library functions. These two functions are
// decoupled in case logging should be initialized before ctl is fully configured, for example if
// logging is configured at app startup and the rest of CTL config is set dynamically later on.
func InitViperFromExternal(cfg GlobalConfig) error {
	if alreadyInitViperFromExt {
		if !reflect.DeepEqual(cfg, globalCfg) {
			return ErrViperAlreadyInit
		}
		return nil
	}
	if cfg.NumWorkers < 1 {
		cfg.NumWorkers = runtime.GOMAXPROCS(0)
	}
	if cfg.ConnTimeoutMs < 500 {
		cfg.ConnTimeoutMs = 500
	}

	globalFlagSet := pflag.FlagSet{}
	if cfg.Mount != "" {
		globalFlagSet.String(BeeGFSMountPointKey, cfg.Mount, "")
	}
	globalFlagSet.String(ManagementAddrKey, cfg.MgmtdAddress, "")
	globalFlagSet.String(TlsCertFile, cfg.MgmtdTLSCertFile, "")
	globalFlagSet.Bool(TlsDisableKey, cfg.MgmtdTLSDisable, "")
	globalFlagSet.Bool(TlsDisableVerificationKey, cfg.MgmtdTLSDisableVerification, "")
	globalFlagSet.Bool(UseProxyKey, cfg.MgmtdUseProxy, "")
	globalFlagSet.String(AuthFileKey, cfg.AuthFile, "")
	globalFlagSet.Bool(AuthDisableKey, cfg.AuthDisable, "")
	globalFlagSet.String(BeeRemoteAddrKey, cfg.RemoteAddress, "")
	globalFlagSet.Int(NumWorkersKey, cfg.NumWorkers, "")
	globalFlagSet.Duration(ConnTimeoutKey, time.Duration(cfg.ConnTimeoutMs)*time.Millisecond, "")

	viper.SetEnvPrefix("beegfs")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	os.Setenv("BEEGFS_BINARY_NAME", "beegfs")
	globalFlagSet.VisitAll(func(flag *pflag.Flag) {
		viper.BindEnv(flag.Name)
		viper.BindPFlag(flag.Name, flag)
	})
	alreadyInitViperFromExt = true
	globalCfg = cfg
	return nil
}

// globalMount is the BeeGFS mount, or the unmounted filesystem for --mount "none". BeeGFSClient()
// sets it once, and it is read-only afterwards. It has no lock, like mgmtClient: commands resolve
// it before they start parallel work, for example on the goroutine that walks the paths.
var globalMount filesystem.Provider

var mgmtClient *beegrpc.Mgmtd

// mgmtClientFsUUID is the filesystem UUID that ManagementClient() checked mgmtClient serves. It is
// empty when nothing was checked, for example for an explicit --mgmtd-addr without --mount. Only
// ManagementClient() sets it, at the same time as mgmtClient, and it is read-only afterwards. So it
// needs no more synchronization than mgmtClient itself.
var mgmtClientFsUUID string

// Try to establish a connection to the managements gRPC service. This also handles any automatic
// configuration such as determining the mgmtd address and authentication secret if those were not
// explicitly configured.
//
// It applies these rules, in this order. mgmtdaddr.go explains the client facts behind them.
//   - An explicit --mgmtd-addr is dialed as it is.
//   - An automatic --mgmtd-addr comes from the local clients.
//   - A mount selects the filesystem: --mount, or else the mount that BeeGFSClient() already
//     resolved from a path argument. Only the client that serves it counts, matched by its mount
//     ID. This is how one filesystem is picked when several are mounted. A --mount that is neither
//     "none" nor an absolute path is an error.
//   - The client of a selecting mount must be found and registered, with any --mgmtd-addr.
//     Otherwise it is an error. The hidden --allow-unverified-mount flag lets an explicit
//     --mgmtd-addr skip this, for places where CTL cannot read /proc/fs/beegfs.
//   - Without a mount, an automatic --mgmtd-addr needs the registered clients to belong to one
//     filesystem. The first registered client selects it and supplies the address, so the user
//     sets --mgmtd-addr to use another.
//   - When a filesystem was selected, the management node must serve it. An auto-configured
//     address that reaches another filesystem is replaced by the kernel address.
//   - BeeGFSClient() applies the same check to a mount it resolves after this function ran.
func ManagementClient() (*beegrpc.Mgmtd, error) {
	if mgmtClient != nil {
		return mgmtClient, nil
	}

	log, _ := GetLogger()

	var cert []byte
	var err error
	if !viper.GetBool(TlsDisableKey) && viper.GetString(TlsCertFile) != "" {
		cert, err = os.ReadFile(viper.GetString(TlsCertFile))
		if err != nil {
			return nil, fmt.Errorf("reading certificate file failed: %w (hint: run 'beegfs --help' and review the options to configure TLS)", err)
		}
	}

	mgmtdAddr := viper.GetString(ManagementAddrKey)
	autoConfigured := mgmtdAddr == BeeGFSMgmtdAddrAuto

	// mount optionally selects the filesystem to manage: --mount, or else the mount of a path
	// argument, see selectedMount(). When set, only the client that serves it counts, which is how
	// one filesystem is picked when several are mounted. When empty, nothing selects one. Both
	// sources name the filesystem the user works in, so both are checked the same way, whatever
	// --mgmtd-addr is. selectedBy names the source in messages.
	mount, selectedBy, err := selectedMount()
	if err != nil {
		return nil, err
	}

	// The local clients are needed to auto-configure the address, and to learn which filesystem
	// the mount selects.
	var clients []procfs.Client
	if autoConfigured || mount != "" {
		clientsCfg := procfs.GetBeeGFSClientsConfig{}
		if mount != "" {
			// Only the client that serves the mount, even when other filesystems are mounted.
			clientsCfg.FilterByMounts = []string{mount}
		}
		ctx, cancel := context.WithTimeout(context.Background(), viper.GetDuration(ConnTimeoutKey))
		clients, err = procfs.GetBeeGFSClients(ctx, clientsCfg, log)
		cancel()
		// GetBeeGFSClients() only fails when asked to force connections, which this call does
		// not. Unreadable procfs shows up as missing clients, which the checks below handle.
		if err != nil {
			return nil, err
		}
	}

	// selected is the client that selects the filesystem to manage. wantFsUUID is its UUID, which
	// the management node must serve. Both stay empty only for an explicit address that no mount
	// checks, and then the address is used as it is.
	var selected procfs.Client
	var wantFsUUID string
	// mountedAt names the selected client, see describeMount(). It is only used in messages.
	var mountedAt string
	// kernelAddr and cfgFile are only set when the address was auto-configured.
	var kernelAddr, cfgFile string
	// autoAuthFile is the connAuthFile of the client an auto-configured address came from.
	var autoAuthFile string

	// The clients must select one registered client of one filesystem, see registeredClientOf().
	// Only an explicit address without a mount skips this, because then nothing selects a
	// filesystem.
	if autoConfigured || mount != "" {
		selected, err = registeredClientOf(clients)
		switch {
		case err == nil:
			wantFsUUID = selected.FsUUID
			mountedAt = describeMount(selected)
		case autoConfigured:
			// An automatic address comes from the selected client, so without one there is no
			// address to use. allow-unverified-mount cannot help here.
			return nil, fmt.Errorf("unable to auto-configure the management address: %s", whyNoClientSelected(err, selectedBy, clients, log))
		case !viper.GetBool(AllowUnverifiedMountKey):
			// An explicit address with a mount that cannot be verified. Using it anyway would be
			// unsafe: requests about entries in the mount could go to another filesystem, and
			// EntryIDs such as "root" exist in every filesystem.
			return nil, fmt.Errorf("%s (hint: if CTL cannot read /proc/fs/beegfs where it runs, set --%s to use --%s %s without checking that it serves this filesystem)", whyNoClientSelected(err, selectedBy, clients, log), AllowUnverifiedMountKey, ManagementAddrKey, mgmtdAddr)
		default:
			// The user accepted an explicit address that cannot be verified against the mount.
			log.Warn("unable to verify the filesystem of the mount, using the management address unchecked because allow-unverified-mount is set", zap.String("mgmtdAddr", mgmtdAddr), zap.String("reason", whyNoClientSelected(err, selectedBy, clients, log)))
		}
	}

	if autoConfigured {
		// The first registered client, in procfs order, supplies the address. A user who wants
		// another address sets --mgmtd-addr. The config file's sysMgmtdHost is preferred over the
		// kernel address, because TLS may only accept the hostname. describeMounts() lists
		// addresses with the same preference, so keep it in sync when this rule changes.
		host, port, err := kernelMgmtdAddr(selected)
		if err != nil {
			return nil, err
		}
		kernelAddr = net.JoinHostPort(host, port)
		mgmtdAddr = kernelAddr
		cfgFile = selected.Config[procfsCfgFile]
		if cfgFileHost := mgmtdHostFromCfgFile(cfgFile, log); cfgFileHost != "" {
			mgmtdAddr = net.JoinHostPort(cfgFileHost, port)
		}
		autoAuthFile = selected.Config[procfsAuthFile]
		log.Debug("attempting to use auto configured management address", zap.String("mgmtdAddr", mgmtdAddr), zap.String("kernelAddr", kernelAddr), zap.String("fsUUID", wantFsUUID), zap.String("mountPoint", selected.Mount.Path), zap.String("procfsDir", selected.ProcDir), zap.String(procfsCfgFile, cfgFile))
	} else {
		log.Debug("attempting to use user defined management address", zap.String("mgmtdAddr", mgmtdAddr), zap.String("fsUUID", wantFsUUID))
	}

	var authSecret []byte
	if !viper.GetBool(AuthDisableKey) {
		authFilePath := viper.GetString(AuthFileKey)
		if authSecret, err = os.ReadFile(authFilePath); err != nil {
			if authFilePath != BeeGFSAuthDefaultPath {
				return nil, fmt.Errorf("couldn't read auth file at %q (non-default path): %w", authFilePath, err)
			}
			if !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("couldn't read default auth file at %q: %w", authFilePath, err)
			}
			if autoAuthFile == "" {
				return nil, fmt.Errorf("couldn't read default auth file at %q: %w, and no auto-configured client auth file was found", authFilePath, err)
			}
			log.Debug("default auth file path does not exist but the management address was auto-configured, attempting to also auto-configure the auth file", zap.String("authFileFromAutoClient", autoAuthFile))
			var autoErr error
			if authSecret, autoErr = os.ReadFile(autoAuthFile); autoErr != nil {
				return nil, fmt.Errorf("default auth file does not exist and falling back to auto-configuring the auth file from the client failed due to an error reading the client's auth file at %q: %w",
					autoAuthFile, autoErr)
			}
			viper.Set(AuthFileKey, autoAuthFile)
		}
	}

	if wantFsUUID == "" {
		mgmtClient, err = newMgmtdClient(mgmtdAddr, cert, authSecret)
		return mgmtClient, err
	}

	// A filesystem was selected, so the management node must serve it. ManagementClient() takes
	// no context, so the caller cannot cancel this check. CTL sets no deadline on gRPC calls, so the
	// check has none either. Passing a context into ManagementClient() would let the caller cancel.
	ctx := context.Background()
	mgmtd, servedFsUUID, err := connectAndGetFsUUID(ctx, mgmtdAddr, cert, authSecret)
	if err != nil {
		// An error reaching the node is returned, and the kernel address is not tried. That keeps
		// the behavior from before the check. With a certificate that only lists the hostname, the
		// IP would fail as well and hide this error.
		return nil, fmt.Errorf("connecting to the management node at %s: %w", mgmtdAddr, err)
	}
	if servedFsUUID == wantFsUUID {
		mgmtClient, mgmtClientFsUUID = mgmtd, wantFsUUID
		return mgmtClient, nil
	}
	mgmtd.Cleanup()

	// An explicit address only reaches this point when a mount selected a filesystem, and the
	// address serves another one. The mount is --mount or the mount of a path argument, and
	// selectedBy says which, so the message points at the right input.
	//
	// The error is returned here, and the fallback below is not tried. The user chose this address,
	// so CTL must not swap it for another one. The fallback only repairs an auto-configured address
	// whose config file host no longer matches the mount.
	if !autoConfigured {
		return nil, fmt.Errorf("--%s %s is the management node of filesystem %s, but %s is filesystem %s (hint: specify the management node of the filesystem mounted at %s, or set --%s to %q to use the one the mounted client is using)", ManagementAddrKey, mgmtdAddr, servedFsUUID, selectedBy, wantFsUUID, mountedAt, ManagementAddrKey, BeeGFSMgmtdAddrAuto)
	}

	// The config file's sysMgmtdHost reaches the management node of another filesystem. The file
	// probably changed after the mount, so fall back to the kernel address.
	if mgmtdAddr != kernelAddr {
		log.Warn("the sysMgmtdHost in the client config file is not the management node the mounted client is using, falling back to the management IP the client kernel module is using", zap.String(procfsCfgFile, cfgFile), zap.String("cfgFileAddr", mgmtdAddr), zap.String("cfgFileAddrFsUUID", servedFsUUID), zap.String("kernelAddr", kernelAddr), zap.String("fsUUID", wantFsUUID))
		cfgFileAddr, cfgFileFsUUID := mgmtdAddr, servedFsUUID
		mgmtd, servedFsUUID, err = connectAndGetFsUUID(ctx, kernelAddr, cert, authSecret)
		if err != nil {
			return nil, fmt.Errorf("the %s in %s (%s) is the management node of filesystem %s, not of the filesystem mounted at %s (%s). The config file was likely changed after the filesystem was mounted, so CTL fell back to the management IP the client kernel module is using (%s, with the gRPC port the management node reported), but that failed: %w (hint: the management node may not accept gRPC connections on that IP, for example if its TLS certificate only lists a hostname. Set %s in %s to the management node of the mounted filesystem, or manually specify --%s <hostname|ip>:<grpc-port>)",
				procfsMgmtdHost, cfgFile, cfgFileAddr, cfgFileFsUUID, mountedAt, wantFsUUID, kernelAddr, err, procfsMgmtdHost, cfgFile, ManagementAddrKey)
		}
		if servedFsUUID == wantFsUUID {
			mgmtClient, mgmtClientFsUUID = mgmtd, wantFsUUID
			return mgmtClient, nil
		}
		mgmtd.Cleanup()
	}

	// The kernel address serves a different filesystem than the client registered with.
	return nil, fmt.Errorf("the management node at %s serves filesystem %s, but the client mounted at %s registered with filesystem %s at that IP (this should never happen. It suggests another management service answers gRPC on that IP and port, or the management service this client registered with was reinitialized or replaced without remounting the client)", kernelAddr, servedFsUUID, mountedAt, wantFsUUID)

}

var beeRemoteClient beeremote.BeeRemoteClient

func BeeRemoteClient() (beeremote.BeeRemoteClient, error) {
	if beeRemoteClient != nil {
		return beeRemoteClient, nil
	}

	var cert []byte
	var err error
	if !viper.GetBool(TlsDisableKey) && viper.GetString(TlsCertFile) != "" {
		cert, err = os.ReadFile(viper.GetString(TlsCertFile))
		if err != nil {
			return nil, fmt.Errorf("reading certificate file failed: %w", err)
		}
	}

	// Get the mgmtd client first so the auth secret can be automatically initialized if BeeGFS is
	// mounted and the default auth file does not exist. This does mean the mgmtd service must be
	// accessible to make any request to the Remote service, even if the request would not otherwise
	// require mgmtd (for example simply listing configure RSTs or getting local DB entries).
	mgmtd, err := ManagementClient()
	if err != nil {
		return nil, err
	}

	conn, err := beegrpc.NewClientConn(
		viper.GetString(BeeRemoteAddrKey),
		beegrpc.WithTLSDisable(viper.GetBool(TlsDisableKey)),
		beegrpc.WithTLSDisableVerification(viper.GetBool(TlsDisableVerificationKey)),
		beegrpc.WithTLSCaCert(cert),
		beegrpc.WithAuthSecret(mgmtd.GetAuthSecretBytes()),
		beegrpc.WithProxy(viper.GetBool(UseProxyKey)),
	)

	beeRemoteClient = beeremote.NewBeeRemoteClient(conn)

	return beeRemoteClient, err
}

var beeRemoteRegistry *registry.CachedComponentRegistry

// BeeRemoteRegistry returns the process-wide cached registry for BeeRemote capabilities. The
// registry is created on first call using BeeRemoteClient(). If lazy is true, the capabilities are
// deferred until first use.
//
//	reg, err := BeeRemoteRegistry(ctx, true)
//	if err != nil {
//	    return nil, err
//	}
//	if err := reg.RequireFeature(ctx, registry.FeatureFilterFiles); err != nil {
//	    return nil, err
//	}
func BeeRemoteRegistry(ctx context.Context, lazy bool) (*registry.CachedComponentRegistry, error) {
	if beeRemoteRegistry != nil {
		return beeRemoteRegistry, nil
	}

	opts := []registry.CachedComponentRegistryOpt{}
	if lazy {
		opts = append(opts, registry.WithSkipInit())
	}

	registry, err := registry.NewCachedComponentRegistry(ctx, beeRemoteRegistryClient, opts...)
	if err != nil {
		return nil, err
	}

	beeRemoteRegistry = registry
	return beeRemoteRegistry, err
}

func beeRemoteRegistryClient() (*registry.RegistryGetter, error) {
	client, err := BeeRemoteClient()
	if err != nil {
		return nil, err
	}
	var regClient registry.RegistryGetter = client
	return &regClient, nil
}

// BeeGFSClient provides a standardize way to interact with a mounted or unmounted BeeGFS instance
// through the globalMount.
//
// If BeeGFSMountPoint is not set, it requires a path inside BeeGFS and will handle determining
// where BeeGFS is mounted and initializing the globalMount the first time it is called.
//
// If the user wishes to interact with an unmounted BeeGFS instance they must specify
// BeeGFSMountPoint as "none". The use of none will never conflict with a legitimate mount point,
// because this should always be specified as an absolute path including a leading slash. When the
// user has specified there is no mount point, an "unmounted" file system along with ErrUnmounted is
// returned allowing BeeGFSClient can be used by all modes regardless if they require BeeGFS to be
// actually mounted.
//
// When BeeGFSMountPoint is set it always initializes and returns the globalMount based on that
// mount point and will return an error if BeeGFS is not mounted.
//
// Callers can always use relative paths inside BeeGFS with the filesystem. If a caller does not
// know if it is has an relative or absolute path, the Filesystem.GetRelativePathWithinMount(path)
// function can be used to get a sanitized relative path inside BeeGFS.
//
// Note the behavior of filesystem.GetRelativePathWithinMount() differs slightly depending if
// BeeGFSMountPoint or the provided path is used to determine where BeeGFS is mounted:
//
//   - If BeeGFSMountPoint is specified, users can use absolute or relative paths inside BeeGFS from any cwd.
//     Note absolute paths only work if they are inside the same mount point as BeeGFSMountPoint.
//   - If BeeGFSMountPoint is set to "none", then all paths are considered relative to the BeeGFS root directory.
//   - If BeeGFSMountPoint is NOT specified, users can only use relative paths when the cwd is somewhere in BeeGFS.
//
// If ManagementClient() already ran, a resolved BeeGFS mount must belong to the filesystem that
// management node serves. ManagementClient() could not check a mount it did not know about yet.
// The check is as strict as ManagementClient()'s check of a mount, including the
// --allow-unverified-mount escape hatch.
func BeeGFSClient(path string) (filesystem.Provider, error) {
	if globalMount == nil {
		var resolved filesystem.Provider
		// selectedBy names the mount in messages, as ManagementClient() does.
		var selectedBy string
		var err error
		if viper.IsSet(BeeGFSMountPointKey) {
			mp := viper.GetString(BeeGFSMountPointKey)
			if mp == BeeGFSMountPointNone {
				euid := syscall.Geteuid()
				// This is is also checked by the CTL CLI frontend (in root.go), but we should check
				// again here in case CTL is used as a library.
				if euid != 0 {
					return nil, fmt.Errorf("only root can interact with an unmounted file system")
				}
				globalMount = filesystem.UnmountedFS{}
				return globalMount, filesystem.ErrUnmounted
			}
			if !filepath.IsAbs(mp) {
				return nil, errMountNotAbsolute
			}
			resolved, err = filesystem.NewFromPath(mp)
			selectedBy = fmt.Sprintf(selectedByMountFlag, mp)
		} else {
			resolved, err = filesystem.NewFromPath(path)
		}
		if err != nil {
			return nil, err
		}

		// A management client built before this mount was known has not been checked against it,
		// so check now. The rules match ManagementClient()'s check of a mount.
		if bfs, ok := resolved.(filesystem.BeeGFS); ok && mgmtClient != nil {
			if selectedBy == "" {
				selectedBy = fmt.Sprintf(selectedByPathArg, bfs.GetMountPath())
			}
			log, _ := GetLogger()
			ctx, cancel := context.WithTimeout(context.Background(), viper.GetDuration(ConnTimeoutKey))
			clients, err := procfs.GetBeeGFSClients(ctx, procfs.GetBeeGFSClientsConfig{FilterByMounts: []string{bfs.GetMountPath()}}, log)
			cancel()
			// GetBeeGFSClients() only fails when asked to force connections, which this call does
			// not. Unreadable procfs shows up as missing clients, which the checks below handle,
			// including allow-unverified-mount.
			if err != nil {
				return nil, err
			}
			c, err := registeredClientOf(clients)
			switch {
			case err != nil && !viper.GetBool(AllowUnverifiedMountKey):
				return nil, fmt.Errorf("%s (hint: if CTL cannot read /proc/fs/beegfs where it runs, set --%s to use the management node at %s without checking that it serves this filesystem)", whyNoClientSelected(err, selectedBy, clients, log), AllowUnverifiedMountKey, mgmtClient.GetAddress())
			case err != nil:
				log.Warn("unable to verify the filesystem of the mount, using it unchecked because allow-unverified-mount is set", zap.String("mgmtdAddr", mgmtClient.GetAddress()), zap.String("reason", whyNoClientSelected(err, selectedBy, clients, log)))
			default:
				servedFsUUID := mgmtClientFsUUID
				if servedFsUUID == "" {
					// ManagementClient() did not check the node, so ask which filesystem it serves.
					// Like ManagementClient(), this takes no context and has no deadline. The
					// result is not stored, because this block only runs once per process.
					if servedFsUUID, err = mgmtClient.GetFsUUID(context.Background()); err != nil {
						return nil, fmt.Errorf("checking which filesystem the management node at %s serves: %w", mgmtClient.GetAddress(), err)
					}
				}
				// A mount of another filesystem is an error even with allow-unverified-mount, because
				// it was verified.
				if servedFsUUID != c.FsUUID {
					return nil, fmt.Errorf("the filesystem mounted at %s is %s, but the management node at %s serves filesystem %s (hint: specify the --%s of the filesystem mounted at %s)", describeMount(c), c.FsUUID, mgmtClient.GetAddress(), servedFsUUID, ManagementAddrKey, bfs.GetMountPath())
				}
			}
		}
		globalMount = resolved
	}
	return globalMount, nil
}

// The global node store singleton
var nodeStore *beemsg.NodeStore

// nodeStoreMu is used to coordinate initialization of the node store.
var nodeStoreMu sync.RWMutex

// Return a pointer to the global node store. Initializes and fetches node list on first call.
// Thread safe so multiple goroutines may call it simultaneously and only the first call will
// initialize the NodeStore and block the others until initialization completes.
func NodeStore(ctx context.Context) (*beemsg.NodeStore, error) {
	if nodeStore != nil {
		nodeStoreMu.RLock()
		defer nodeStoreMu.RUnlock()
		return nodeStore, nil
	}

	nodeStoreMu.Lock()
	defer nodeStoreMu.Unlock()

	// Configure the management first as this also handles any automatic configuration such as the
	// mgmtd address and conn auth file.
	mgmtd, err := ManagementClient()
	if err != nil {
		return nil, err
	}

	// Create a node store using the current settings. These are copied, so later changes to
	// globalConfig don't affect them!
	nodeStore = beemsg.NewNodeStore(viper.GetDuration(ConnTimeoutKey), mgmtd.GetAuthSecret())

	// Fetch the node list from management
	nodes, err := mgmtd.GetNodes(ctx, &pm.GetNodesRequest{
		IncludeNics: true,
	})
	if err != nil {
		return nil, fmt.Errorf("getting node list from management: %w", err)
	}

	// Loop through the node entries
	for _, n := range nodes.GetNodes() {
		nics := []beegfs.Nic{}
		for _, a := range n.Nics {
			nict := beegfs.InvalidNicType
			switch a.GetNicType() {
			case pb.NicType_ETHERNET:
				nict = beegfs.Tcp
			case pb.NicType_RDMA:
				nict = beegfs.Rdma
			}

			nics = append(nics, beegfs.Nic{Addr: a.Addr, Name: a.Name, Type: nict})
		}

		t := beegfs.InvalidNodeType
		switch n.GetId().GetLegacyId().GetNodeType() {
		case pb.NodeType_META:
			t = beegfs.Meta
		case pb.NodeType_STORAGE:
			t = beegfs.Storage
		case pb.NodeType_CLIENT:
			t = beegfs.Client
		case pb.NodeType_MANAGEMENT:
			t = beegfs.Management
		}

		// Add node to store
		nodeStore.AddNode(&beegfs.Node{
			Uid: beegfs.Uid(*n.Id.Uid),
			Id: beegfs.LegacyId{
				NumId:    beegfs.NumId(n.Id.LegacyId.NumId),
				NodeType: t,
			},
			Alias: beegfs.Alias(*n.Id.Alias),
			Nics:  nics,
		})
	}

	if metaRoot := nodes.GetMetaRootNode(); metaRoot != nil {
		metaRoot2, err := beegfs.EntityIdSetFromProto(metaRoot)
		if err != nil {
			return nil, err
		}

		err = nodeStore.SetMetaRootNode(metaRoot2.Uid)
		if err != nil {
			return nil, err
		}

		if rootBuddy := nodes.GetMetaRootBuddyGroup(); rootBuddy != nil {
			rootMirror, err := beegfs.EntityIdSetFromProto(rootBuddy)
			if err != nil {
				return nil, fmt.Errorf("parsing metadata root mirror: %w", err)
			}
			nodeStore.SetMetaRootBuddyGroup(rootMirror)
		}
	}
	return nodeStore, nil
}

// Resets the global state and frees resources
func Cleanup() {
	if nodeStore != nil {
		nodeStore.Cleanup()
	}
	nodeStore = nil
}

var globalLogger *logger.Logger

// Returns a global logger that logs to stderr. Don't rely solely on the logger to communicate
// important information to the user since all non-fatal log messages may be disabled by default for
// some consumers of this functionality (such as CTL). The logger DOES NOT replace the need to
// return meaningful errors.
//
// IMPORTANT: Unless your code is what is responsible for exiting when an error is encountered,
// generally calling `log.Fatal()` is discouraged as this will immediately terminate the program.
//
// When logging keep in mind it is bad practice to both log and return an error. That generally
// results in the same error gets logged multiple times at different layers. Instead the the logger
// should be used to add additional context, typically at the debug level, for what operations led
// up to some error being returned. Whatever is at the "top-level" can make the decision what to do
// with that error, such as log it and move on in the case of a long-running service, or immediately
// return it to the user in the case of an interactive/CLI tool.
//
// For example you might log an connection attempt to a node. If the attempt fails an error is
// returned, and if it is unclear what layer the error is coming from, debug logging could be
// enabled to troubleshoot.
//
// Note when getting the logger unless there is a bug in the logging implementation errors are
// unlikely and can usually be ignored for interactive tools where a panic due to the logger being
// unavailable is acceptable. However for long-running services errors should always be checked.
func GetLogger() (*logger.Logger, error) {
	var err error
	var invalidLogLevel = false
	if globalLogger == nil {
		// When CTL is used as a library the globalLogger can also be initialized by
		// InitLoggerFromExternal(). Otherwise it is always initialized here on first use.
		logLevel := viper.GetInt(LogLevelKey)
		if logLevel < 0 || logLevel > 5 {
			// If the user gave an invalid log level ignore it and set logging to the highest
			// verbosity. This means we can generally always return a valid logger so most callers
			// don't need to check for an error from GetLogger().
			logLevel = 5
			invalidLogLevel = true
		}
		globalLogger, err = logger.New(logger.Config{
			Level:     int8(logLevel),
			Type:      logger.StdErr,
			Developer: viper.GetBool(LogDeveloperKey),
		}, nil)
		if err != nil {
			return nil, err
		}
		if invalidLogLevel {
			globalLogger.Debug("enabling debug logging and ignoring user provided log level (was not in the range 0-5)")
		}
	}
	return globalLogger, nil
}
