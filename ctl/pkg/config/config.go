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
	UseProxyKey = "use-http-proxy"
)

// Viper values for certain configuration values.
const (
	BeeGFSMountPointNone  = "none"
	BeeGFSMgmtdAddrAuto   = "auto"
	BeeGFSAuthDefaultPath = "/etc/beegfs/conn.auth"
)

// BeeGFS procfs configuration keys.
const (
	procfsMgmtdHost = "sysMgmtdHost"
	procfsMgmtdGrpc = "connMgmtdGrpcPort"
)

// OutputType is used to control what type of structured output should be printed.
type OutputType string

const (
	OutputTable      OutputType = "table"
	OutputJSON       OutputType = "json"
	OutputJSONPretty OutputType = "json-pretty"
	OutputNDJSON     OutputType = "ndjson"
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

// InitLoggerFromExternal allows CTL to use an externally configured/managed zap.Logger. By default
// CTL uses an opinionated logger that is initialized on first call that logs to stderr only. This
// allows CTL logging to respect the logging configuration of the application that is using it
// including dynamic reconfiguration (like log level updates). This does not add any context to the
// logger and relies on the caller to do that if desired. For example:
//
//	ctl.InitLoggerFromExternal(logger.With(zap.String("component", "ctl")))
func InitLoggerFromExternal(logUsing *zap.Logger) error {
	if globalLogger != nil {
		return fmt.Errorf("ctl logging was already initialized (this is probably a bug)")
	}
	// Same approach used for the BadgerLoggerBridge. A new type could have been created (like
	// CtlLoggerBridge) but when this function was added the logger was already widely used in CTL
	// and would have caused a lot of unnecessary refactoring. Just don't do anything with level.
	globalLogger = &logger.Logger{
		Logger: logUsing,
	}
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

// The global config singleton
var globalMount filesystem.Provider

var mgmtClient *beegrpc.Mgmtd

// Try to establish a connection to the managements gRPC service. This also handles any automatic
// configuration such as determining the mgmtd address and authentication secret if those were not
// explicitly configured.
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
			return nil, fmt.Errorf("reading certificate file failed: %w", err)
		}
	}

	var authSecret []byte
	// authFileFromAutoClient is used to auto configure the conn auth file when the management
	// address is auto configured from a client mount and the default auth file does not exist.
	var authFileFromAutoClient string

	mgmtdAddr := viper.GetString(ManagementAddrKey)
	if mgmtdAddr == BeeGFSMgmtdAddrAuto {
		ctx, cancel := context.WithTimeout(context.Background(), viper.GetDuration(ConnTimeoutKey))
		defer cancel()
		clients, err := procfs.GetBeeGFSClients(ctx, procfs.GetBeeGFSClientsConfig{}, log)
		if err != nil {
			return nil, err
		}
		if len(clients) == 0 {
			return nil, fmt.Errorf("unable to auto-configure the management address: BeeGFS does not appear to be mounted, manually specify --%s <hostname|ip>:<grpc-port> for the file system to manage", ManagementAddrKey)
		}
		for _, c := range clients {
			sysMgmtdHost, ok := c.Config[procfsMgmtdHost]
			if !ok {
				return nil, fmt.Errorf("unable to auto-configure the management address: configuration at %s/config does not appear to contain a %s, manually specify --%s <hostname|ip>:<grpc-port> for the file system to manage (this is likely a bug)", c.ProcDir, procfsMgmtdHost, ManagementAddrKey)
			}
			connMgmtdGrpcPort, ok := c.Config[procfsMgmtdGrpc]
			if !ok {
				return nil, fmt.Errorf("unable to auto-configure the management address: configuration at %s/config does not appear to contain a %s , manually specify --%s <hostname|ip>:<grpc-port>for the file system to manage (this is likely a bug)", c.ProcDir, procfsMgmtdGrpc, ManagementAddrKey)
			}
			foundMgmtdAddr := net.JoinHostPort(sysMgmtdHost, connMgmtdGrpcPort)
			log.Debug("found client mount point", zap.String("mgmtdAddr", foundMgmtdAddr), zap.String("mountPoint", c.Mount.Path), zap.String("procfsDir", c.ProcDir))
			if mgmtdAddr == BeeGFSMgmtdAddrAuto {
				mgmtdAddr = foundMgmtdAddr
			} else if len(clients) > 1 {
				// Generally CTL shouldn't care if the same BeeGFS is mounted multiple times. The
				// only time it might matter is if someone tries to run a command against multiple
				// paths in different BeeGFS mount points. But that needs to be handled elsewhere
				// anyway since users can always manually configure the mgmtd and do the same thing.
				if mgmtdAddr != foundMgmtdAddr {
					// Note the mgmtd address extracted from procfs should always be an IP address
					// because when the client is mounted it handles resolving any hostnames first.
					// However if the mgmtd service was available to this machine from multiple IPs
					// this would not work. We could use the UUID here instead, but then we'd need
					// to decide which hostname/port to use. Better for now to just ask the user.
					return nil, fmt.Errorf("unable to auto-configure the management address: multiple client mount points with different %s:%s configurations were found, manually specify --%s for the file system to manage", procfsMgmtdHost, procfsMgmtdGrpc, ManagementAddrKey)
				}
			}
			if connAuthFile, ok := c.Config["connAuthFile"]; ok {
				authFileFromAutoClient = connAuthFile
			}
		}
		log.Debug("attempting to use auto configured management address", zap.String("mgmtdAddr", mgmtdAddr))
	} else {
		log.Debug("attempting to use user defined management address", zap.String("mgmtdAddr", mgmtdAddr))
	}

	if !viper.GetBool(AuthDisableKey) {
		authFilePath := viper.GetString(AuthFileKey)
		if authSecret, err = os.ReadFile(authFilePath); err != nil {
			if authFilePath != BeeGFSAuthDefaultPath {
				return nil, fmt.Errorf("couldn't read auth file at %q (non-default path): %w", authFilePath, err)
			}
			if !errors.Is(err, os.ErrNotExist) {
				return nil, fmt.Errorf("couldn't read default auth file at %q: %w", authFilePath, err)
			}
			if authFileFromAutoClient == "" {
				return nil, fmt.Errorf("couldn't read default auth file at %q: %w, and no auto-configured client auth file was found", authFilePath, err)
			}
			log.Debug("default auth file path does not exist but the management address was auto-configured, attempting to also auto-configure the auth file", zap.String("authFileFromAutoClient", authFileFromAutoClient))
			var autoErr error
			if authSecret, autoErr = os.ReadFile(authFileFromAutoClient); autoErr != nil {
				return nil, fmt.Errorf("default auth file does not exist and falling back to auto-configuring the auth file from the client failed due to an error reading the client's auth file at %q: %w",
					authFileFromAutoClient, autoErr)
			}
			viper.Set(AuthFileKey, authFileFromAutoClient)
		}
	}

	mgmtClient, err = beegrpc.NewMgmtd(
		mgmtdAddr,
		beegrpc.WithTLSDisable(viper.GetBool(TlsDisableKey)),
		beegrpc.WithTLSDisableVerification(viper.GetBool(TlsDisableVerificationKey)),
		beegrpc.WithTLSCaCert(cert),
		beegrpc.WithAuthSecret(authSecret),
		beegrpc.WithProxy(viper.GetBool(UseProxyKey)),
	)

	return mgmtClient, err
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
func BeeGFSClient(path string) (filesystem.Provider, error) {
	if globalMount == nil {
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
				return nil, fmt.Errorf("the specified value for %s does not appear to be an absolute path", BeeGFSMountPointKey)
			}
			globalMount, err = filesystem.NewFromPath(mp)
		} else {
			globalMount, err = filesystem.NewFromPath(path)
		}
		if err != nil {
			return nil, err
		}
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
		return nil, fmt.Errorf("error getting node list from management: %w", err)
	}

	// Loop through the node entries
	for _, n := range nodes.GetNodes() {
		nics := []beegfs.Nic{}
		for _, a := range n.Nics {
			nict := beegfs.InvalidNicType
			switch a.GetNicType() {
			case pb.NicType_ETHERNET:
				nict = beegfs.Ethernet
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
				return nil, fmt.Errorf("error parsing meta root mirror: %w", err)
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
		})
		if err != nil {
			return nil, err
		}
		if invalidLogLevel {
			globalLogger.Debug("enabling debug logging and ignoring user provided log level (was not in the range 0-5)")
		}
	}
	return globalLogger, nil
}
