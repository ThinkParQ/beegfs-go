package config

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/common/beemsg"
	"github.com/thinkparq/beegfs-go/common/beemsg/util"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/logger"
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
	// File containing the authentication secret (formerly known as "connAuthFile").
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
)

// Viper values for certain configuration values.
const (
	BeeGFSMountPointNone = "none"
	BeeGFSMgmtdAddrAuto  = "auto"
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

// The global config singleton
var globalMount filesystem.Provider

var mgmtClient *beegrpc.Mgmtd

// Try to establish a connection to the managements gRPC service
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
	if !viper.GetBool(AuthDisableKey) {
		authSecret, err = os.ReadFile(viper.GetString(AuthFileKey))
		if err != nil {
			return nil, fmt.Errorf("couldn't read auth file: %w", err)
		}
	}

	mgmtdAddr := viper.GetString(ManagementAddrKey)
	if mgmtdAddr == BeeGFSMgmtdAddrAuto {
		ctx, cancel := context.WithTimeout(context.Background(), viper.GetDuration(ConnTimeoutKey))
		defer cancel()
		clients, err := procfs.GetBeeGFSClients(ctx, procfs.GetBeeGFSClientsConfig{}, log)
		if err != nil {
			return nil, err
		}
		if len(clients) == 0 {
			return nil, fmt.Errorf("unable to auto-configure the management address: BeeGFS does not appear to be mounted, manually specify --%s for the file system to manage", ManagementAddrKey)
		}
		for _, c := range clients {
			sysMgmtdHost, ok := c.Config[procfsMgmtdHost]
			if !ok {
				return nil, fmt.Errorf("unable to auto-configure the management address: configuration at %s/config does not appear to contain a %s, manually specify --%s for the file system to manage (this is likely a bug)", c.ProcDir, procfsMgmtdHost, ManagementAddrKey)
			}
			connMgmtdGrpcPort, ok := c.Config[procfsMgmtdGrpc]
			if !ok {
				return nil, fmt.Errorf("unable to auto-configure the management address: configuration at %s/config does not appear to contain a %s , manually specify --%s for the file system to manage (this is likely a bug)", c.ProcDir, procfsMgmtdGrpc, ManagementAddrKey)
			}
			foundMgmtdAddr := fmt.Sprintf("%s:%s", sysMgmtdHost, connMgmtdGrpcPort)
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
		}
		log.Debug("attempting to use auto configured management address", zap.String("mgmtdAddr", mgmtdAddr))
	} else {
		log.Debug("attempting to use user defined management address", zap.String("mgmtdAddr", mgmtdAddr))
	}

	mgmtClient, err = beegrpc.NewMgmtd(
		mgmtdAddr,
		beegrpc.WithTLSDisable(viper.GetBool(TlsDisableKey)),
		beegrpc.WithTLSDisableVerification(viper.GetBool(TlsDisableVerificationKey)),
		beegrpc.WithTLSCaCert(cert),
		beegrpc.WithAuthSecret(authSecret),
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

	var authSecret []byte
	if !viper.GetBool(AuthDisableKey) {
		authSecret, err = os.ReadFile(viper.GetString(AuthFileKey))
		if err != nil {
			return nil, fmt.Errorf("couldn't read auth file: %w", err)
		}
	}

	conn, err := beegrpc.NewClientConn(
		viper.GetString(BeeRemoteAddrKey),
		beegrpc.WithTLSDisable(viper.GetBool(TlsDisableKey)),
		beegrpc.WithTLSDisableVerification(viper.GetBool(TlsDisableVerificationKey)),
		beegrpc.WithTLSCaCert(cert),
		beegrpc.WithAuthSecret(authSecret),
	)

	beeRemoteClient = beeremote.NewBeeRemoteClient(conn)

	return beeRemoteClient, err
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

	authSecret := uint64(0)
	// Setup BeeMsg authentication from the given file
	if !viper.GetBool(AuthDisableKey) {
		key, err := os.ReadFile(viper.GetString(AuthFileKey))
		if err != nil {
			return nil, fmt.Errorf("couldn't read auth file: %w", err)
		} else {
			authSecret = util.GenerateAuthSecret(key)
		}
	}

	// Create a node store using the current settings. These are copied, so later changes to
	// globalConfig don't affect them!
	nodeStore = beemsg.NewNodeStore(viper.GetDuration(ConnTimeoutKey), authSecret)

	c, err := ManagementClient()
	if err != nil {
		return nil, err
	}

	// Fetch the node list from management
	nodes, err := c.GetNodes(ctx, &pm.GetNodesRequest{
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
