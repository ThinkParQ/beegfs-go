package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"

	"github.com/spf13/viper"
	"github.com/thinkparq/gobee/beegfs"
	"github.com/thinkparq/gobee/beemsg"
	"github.com/thinkparq/gobee/beemsg/util"
	"github.com/thinkparq/gobee/filesystem"
	pb "github.com/thinkparq/protobuf/go/beegfs"
	"github.com/thinkparq/protobuf/go/beeremote"
	pm "github.com/thinkparq/protobuf/go/management"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Viper keys for the global config. Should be used when accessing it instead of raw strings.
// Currently these are also used by the frontend for command line flag and env variable names.
const (
	// Managements gRPC listening address
	ManagementAddrKey = "mgmtd-addr"
	// BeeRemotes gRPC listening address
	BeeRemoteAddrKey = "bee-remote-addr"
	// A BeeGFS mount point on the local file system
	BeeGFSMountPointKey = "mount-point"
	// The timeout for a single connection attempt
	ConnTimeoutKey = "conn-timeout"
	// File containing the authentication secret (formerly known as "connAuthFile").
	// Disable authentication if empty or if the default file doesn't exist.
	AuthFileKey = "auth-file"
	// Disable TLS transport security for gRPC communication.
	TlsDisableKey = "tls-disable"
	// Disable TLS server verification for gRPC communication.
	TlsDisableVerificationKey = "tls-disable-verification"
	// Use a custom ca certificate for TLS server verification in addition to the system ones.
	TlsCaCertKey = "tls-ca-cert"
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
)

// The global config singleton
var globalMount filesystem.Provider
var globalConfigLock sync.Mutex

// Returns a grpc.DialOption configured according to the global TLS config
func tlsDialOption() (grpc.DialOption, error) {
	if viper.GetBool(TlsDisableKey) {
		return grpc.WithTransportCredentials(insecure.NewCredentials()), nil
	} else {
		certPool, err := x509.SystemCertPool()
		if err != nil {
			return nil, fmt.Errorf("couldn't load system cert pool: %w\n", err)
		}

		// Append custom ca certificate if provided
		if viper.GetString(TlsCaCertKey) != "" {
			cert, err := os.ReadFile(viper.GetString(TlsCaCertKey))
			if err != nil {
				// Silently ignore the default file not being found
				if viper.IsSet(TlsCaCertKey) {
					return nil, fmt.Errorf("reading certificate file failed: %w\n", err)
				}
			} else {
				if !certPool.AppendCertsFromPEM(cert) {
					return nil, fmt.Errorf("appending cert to pool failed")
				}
			}
		}

		creds := credentials.NewTLS(&tls.Config{
			RootCAs:            certPool,
			InsecureSkipVerify: viper.GetBool(TlsDisableVerificationKey),
		})

		return grpc.WithTransportCredentials(creds), nil
	}
}

// Try to establish a connection to the managements gRPC service
func ManagementClient() (pm.ManagementClient, error) {
	addr := viper.GetString(ManagementAddrKey)

	tlsDialOption, err := tlsDialOption()
	if err != nil {
		return nil, err
	}

	// Open gRPC connection to management.
	// Since the connection is opened asynchronously in the background, a context for timeouts is
	// not needed here. A potential timeout on connection will be checked when making an actual
	// request.
	g, err := grpc.Dial(addr, tlsDialOption)
	if err != nil {
		return nil, fmt.Errorf("connecting to management service on %s failed: %w", addr, err)
	}

	return pm.NewManagementClient(g), nil
}

func BeeRemoteClient() (beeremote.BeeRemoteClient, error) {
	addr := viper.GetString(BeeRemoteAddrKey)

	tlsDialOption, err := tlsDialOption()
	if err != nil {
		return nil, err
	}

	g, err := grpc.Dial(addr, tlsDialOption)
	if err != nil {
		return nil, fmt.Errorf("connecting to beeremote on %s failed: %w", addr, err)
	}

	return beeremote.NewBeeRemoteClient(g), nil
}

// BeeGFSClient provides a standardize way to interact with a mounted BeeGFS through the
// globalMount. If BeeGFSMountPoint is not set, it requires a path inside BeeGFS and will handle
// determining where BeeGFS is mounted and initializing the globalMount the first time it is called.
// When BeeGFSMountPoint is set it always initializes and returns the globalMount based on that
// mount point.
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
//   - If BeeGFSMountPoint is NOT specified, users can only use relative paths when the cwd is somewhere in BeeGFS.
func BeeGFSClient(path string) (filesystem.Provider, error) {
	if globalMount == nil {
		var err error
		if viper.IsSet(BeeGFSMountPointKey) {
			globalMount, err = filesystem.NewFromPath(viper.GetString(BeeGFSMountPointKey))
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

	authSecret := int64(0)
	// Setup BeeMsg authentication from the given file
	if viper.GetString(AuthFileKey) != "" {
		key, err := os.ReadFile(viper.GetString(AuthFileKey))
		if err != nil {
			// Silently ignore the default file not being found
			if viper.IsSet(AuthFileKey) {
				return nil, fmt.Errorf("couldn't read auth file: %w", err)
			}
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
		return nil, err
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
