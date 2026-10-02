package procfs

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/ioctl"
	"github.com/thinkparq/beegfs-go/common/logger"
	"go.uber.org/zap"
)

const (
	mountProcDir = "/proc/mounts"
	// The client prints this as its filesystem UUID until it has registered with the management
	// node, because the UUID comes from the registration response.
	unregisteredFsUUID = "(null)"
)

var (
	ErrEstablishingConnections = errors.New("forcing establishment of BeeGFS client/server connections")
)

type GetBeeGFSClientsConfig struct {
	// Call df to force the client module to establish storage server connections.
	ForceConnections bool
	FilterByUUID     string
	FilterByMounts   []string
}

type Client struct {
	// FsUUID is empty until the client has registered with the management node.
	FsUUID string
	// ID is the mount ID of this client instance, which is also the name of its procfs directory.
	ID           string
	ProcDir      string
	Mount        MountPoint
	Config       map[string]string
	MgmtdNodes   []Node
	MetaNodes    []Node
	StorageNodes []Node
}

type Node struct {
	Alias beegfs.Alias
	NumID beegfs.NumId
	Peers []Peer
	Root  bool
}

type Peer struct {
	Type        beegfs.NicType
	IP          string
	Connections int
	Fallback    bool
}

type MountPoint struct {
	Path string
	Opts map[string]string
}

// GetBeeGFSClients() gets the list of local BeeGFS client instances. It optionally applies the
// following filters before returning the list:
//
//   - Filters out mounts for BeeGFS instances other than the management service configured for CTL.
//     Set cfg.FilterByUUID to an empty string to return all clients.
//   - If cfg.FilterByMounts is specified, only the client(s) for those mount point(s) are returned.
//     See filterClients() for how a path is matched to a client.
func GetBeeGFSClients(ctx context.Context, cfg GetBeeGFSClientsConfig, log *logger.Logger) ([]Client, error) {
	mounts, fsTypes, err := getBeeGFSMounts()
	if err != nil {
		log.Warn("unexpected error getting mounted filesystems (ignoring)", zap.Error(err))
	}
	index := newMountIndex(mounts, ioctl.GetMountID, log)

	if cfg.ForceConnections {
		cmd := exec.CommandContext(ctx, "df", "-t", "beegfs")
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrEstablishingConnections, err)
		}
	}

	clients := make([]Client, 0)
	for _, fsType := range fsTypes {
		procDir := path.Join("/proc/fs", fsType)
		err = filepath.Walk(procDir, func(path string, info os.FileInfo, err error) error {
			log := log.With(zap.String("procfsDir", path))
			if err != nil {
				log.Warn("unexpected error walking procfs directory (ignoring)", zap.Error(err))
				return nil
			}
			if info.IsDir() && path != procDir {
				log.Debug("collecting mount info from procfs")
				mount, err := parseClient(path, index)
				if err != nil {
					log.Warn("unexpected error parsing mount (ignoring)", zap.Error(err))
					return nil
				}
				clients = append(clients, mount)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	return filterClients(clients, cfg, ioctl.GetMountID, log), nil
}

// filterClients runs at the end of GetBeeGFSClients() and decides which clients to return.
//
// Inputs:
//   - clients are all client instances found in procfs, already linked to their mount points.
//   - cfg holds the filters requested by the caller.
//   - getMountID returns the mount ID for a directory. It is ioctl.GetMountID() outside of tests.
//
// Rules:
//   - If cfg.FilterByUUID is set, only clients of the filesystem with that UUID are kept.
//   - If cfg.FilterByMounts is set, only clients that serve one of those paths are kept. The mount
//     ID of a path names the client that serves it, so any directory in a mount matches, including
//     a bind mount.
//   - If the mount ID of a path cannot be read, that path must equal a client's mount point.
func filterClients(clients []Client, cfg GetBeeGFSClientsConfig, getMountID func(dirPath string) (string, error), log *logger.Logger) []Client {
	if cfg.FilterByUUID == "" && len(cfg.FilterByMounts) == 0 {
		// No filtering requested, return list as is
		return clients
	}

	log.Debug("Applying client filters", zap.Any("UUID", cfg.FilterByUUID), zap.Any("Mount points", cfg.FilterByMounts))

	mountIDsFilter := make(map[string]struct{})
	mountPathsFilter := make(map[string]struct{})
	for _, arg := range cfg.FilterByMounts {
		mountID, err := getMountID(arg)
		if err != nil {
			log.Debug("unable to get the mount ID for a requested mount path, matching the path against client mount points instead", zap.String("path", arg), zap.Error(err))
			mountPathsFilter[path.Clean(arg)] = struct{}{}
			continue
		}
		mountIDsFilter[mountID] = struct{}{}
	}

	filteredClients := []Client{}
	for _, c := range clients {
		// filter by the configured UUID first
		if cfg.FilterByUUID != "" {
			log.Debug("filtering client mounts for the filesystem with the requested UUID", zap.Any("UUID", cfg.FilterByUUID))
			if c.FsUUID != cfg.FilterByUUID {
				log.Debug("ignoring client mount because it is for a BeeGFS instance other than the one with the requested UUID", zap.Any("mountProcDir", c.ProcDir), zap.String("mountFsUUID", c.FsUUID), zap.Any("mountPath", c.Mount.Path))
				continue
			}
		} else {
			log.Debug("not filtering by filesystem UUID: user requested all client mounts be included")
		}

		// otherwise, filter by configured mounts
		if len(cfg.FilterByMounts) > 0 {
			_, idRequested := mountIDsFilter[c.ID]
			_, pathRequested := mountPathsFilter[c.Mount.Path]
			if !idRequested && !pathRequested {
				log.Debug("ignoring client mount because it does not serve any of the user specified mount paths", zap.Any("procDir", c.ProcDir), zap.Any("mountPath", c.Mount.Path))
				continue
			}
		}
		log.Debug("including client mount", zap.Any("mountProcDir", c.ProcDir), zap.String("mountFsUUID", c.FsUUID), zap.Any("mountPath", c.Mount.Path))
		filteredClients = append(filteredClients, c)
	}
	return filteredClients
}

// mountIndex links each client directory in procfs to its entry in /proc/mounts.
//
// Two facts about the client make the mount ID the key:
//   - Each mount runs its own client instance, and the instance names its procfs directory after
//     its mount ID.
//   - The GET_MOUNTID ioctl returns that ID for a directory in the mount (see ioctl.GetMountID()).
//
// The ioctl has to open the mount point, which can fail. One example is a user who cannot read
// the mount point. Such a mount can only be matched by its cfgFile mount option, which the client
// also shows in procfs. Several mounts can share one config file, so a cfgFile match is only used
// when a single unidentified mount has that config file.
type mountIndex struct {
	// byID holds the mounts whose mount ID the ioctl returned, keyed by that ID.
	byID map[string]MountPoint
	// byCfgFile holds the mounts whose mount ID could not be read, keyed by their cfgFile option.
	byCfgFile map[string][]MountPoint
}

// newMountIndex runs once per GetBeeGFSClients() call, before procfs is walked. It reads the mount
// ID of every BeeGFS mount found in /proc/mounts. getMountID is ioctl.GetMountID() outside of tests.
func newMountIndex(mounts []MountPoint, getMountID func(dirPath string) (string, error), log *logger.Logger) mountIndex {
	index := mountIndex{
		byID:      make(map[string]MountPoint),
		byCfgFile: make(map[string][]MountPoint),
	}
	for _, m := range mounts {
		mountID, err := getMountID(m.Path)
		if err != nil {
			log.Debug("unable to get the mount ID, matching this mount to its client by config file instead", zap.String("mountPath", m.Path), zap.Error(err))
			cfgFile := m.Opts["cfgFile"]
			index.byCfgFile[cfgFile] = append(index.byCfgFile[cfgFile], m)
			continue
		}
		// A bind mount has the same mount ID as the mount it was made from. Linux lists mounts in
		// the order they were made, so keeping the first entry keeps the original mount point.
		if _, ok := index.byID[mountID]; !ok {
			index.byID[mountID] = m
		}
	}
	return index
}

// mountFor returns the mount point of the client with the given mount ID and cfgFile setting. It
// returns false when no mount matches, or when the cfgFile match is ambiguous.
func (idx mountIndex) mountFor(mountID string, cfgFile string) (MountPoint, bool) {
	if m, ok := idx.byID[mountID]; ok {
		return m, true
	}
	if candidates := idx.byCfgFile[cfgFile]; len(candidates) == 1 {
		return candidates[0], true
	}
	return MountPoint{}, false
}

// Parses a client from its procfs directory and associates it with its MountPoint (if available).
func parseClient(path string, mounts mountIndex) (Client, error) {
	client := Client{ProcDir: path}
	var err error
	// Parse config file:
	configFile, err := os.Open(filepath.Join(path, "config"))
	if err != nil {
		return client, err
	}
	defer configFile.Close()
	config, err := parseClientConfigFile(configFile)
	if err != nil {
		return client, err
	}
	client.Config = config

	// Parse node files:
	client.MgmtdNodes, err = parseClientNodesFile(filepath.Join(path, "mgmt_nodes"))
	if err != nil {
		return client, err
	}
	client.MetaNodes, err = parseClientNodesFile(filepath.Join(path, "meta_nodes"))
	if err != nil {
		return client, err
	}
	client.StorageNodes, err = parseClientNodesFile(filepath.Join(path, "storage_nodes"))
	if err != nil {
		return client, err
	}

	// Parse the client ID:
	client.ID = filepath.Base(path)

	// Associate the client with its mount point:
	if m, ok := mounts.mountFor(client.ID, client.Config["cfgFile"]); ok {
		client.Mount = m
	}

	// Parse the UUID:
	if client.FsUUID, err = parseClientFsUUIDFile(filepath.Join(path, "fs_uuid")); err != nil {
		return client, err
	}

	return client, nil
}

func parseClientFsUUIDFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	return parseClientFsUUID(file)
}

func parseClientFsUUID(input io.Reader) (string, error) {
	var uuid string
	scanner := bufio.NewScanner(input)
	if scanner.Scan() {
		// The UUID file contains a single line.
		uuid = strings.TrimSpace(scanner.Text())
	}
	if uuid == unregisteredFsUUID {
		uuid = ""
	}
	return uuid, scanner.Err()
}

func parseClientConfigFile(input io.Reader) (map[string]string, error) {
	clientConfig := make(map[string]string)
	scanner := bufio.NewScanner(input)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("unable to parse client configuration, line '%s' does not appear to contain a key=value pair", line)
		}
		clientConfig[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("unexpected error while scanning client configuration: %w", err)
	}

	return clientConfig, nil
}

// parseClientNodesFile wraps parseNodes so that function can accept an interface for testing.
func parseClientNodesFile(path string) ([]Node, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return parseNodes(file)
}

func parseNodes(input io.Reader) ([]Node, error) {

	nodes := []Node{}
	scanner := bufio.NewScanner(input)
	var current *Node

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.Contains(line, "[ID:") {
			if current != nil {
				nodes = append(nodes, *current)
			}
			current = new(Node)
			// Aliases may only contain letters, digits, hyphens, underscores, and periods. This
			// makes Sscanf a safe way to parse out the alias and num ID given otherwise arbitrary
			// user aliases.
			_, err := fmt.Sscanf(line, "%s [ID: %d]", &current.Alias, &current.NumID)
			if err != nil {
				return nil, fmt.Errorf("failed to parse node: %w", err)
			}
			current.Peers = []Peer{}
		} else if strings.HasPrefix(line, "Root:") {
			current.Root = true
		} else if strings.HasPrefix(line, "Connections:") {
			parts := strings.SplitSeq(line[len("Connections: "):], ";")
			for part := range parts {
				part = strings.TrimSpace(part)
				if part == "<none>" || part == "" {
					continue
				}
				peer := Peer{}
				if strings.HasPrefix(part, "TCP:") {
					peer.Type = beegfs.Tcp
				} else if strings.HasPrefix(part, "RDMA:") {
					peer.Type = beegfs.Rdma
				} else if strings.HasPrefix(part, "SDP:") {
					peer.Type = beegfs.Sdp
				}
				var peerType string
				_, err := fmt.Sscanf(part, "%s %d (%s", &peerType, &peer.Connections, &peer.IP)
				if err != nil {
					return nil, fmt.Errorf("failed to parse connections to peer (%s): %w", part, err)
				}
				if strings.Contains(part, "[fallback route]") {
					peer.Fallback = true
				}
				peer.IP = strings.TrimSuffix(peer.IP, ")")
				current.Peers = append(current.Peers, peer)
			}
		}
	}

	if current != nil {
		nodes = append(nodes, *current)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("unexpected error while scanning node info: %w", err)
	}

	return nodes, nil
}

func getBeeGFSMounts() ([]MountPoint, []string, error) {
	file, err := os.Open(mountProcDir)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	return parseMounts(file)
}

// parseMounts returns the BeeGFS mounts in the order /proc/mounts lists them, and the BeeGFS file
// system types in use.
func parseMounts(input io.Reader) ([]MountPoint, []string, error) {
	mounts := []MountPoint{}
	fsTypesMap := make(map[string]struct{})
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 4 {
			// This should never be the case for BeeGFS mount points. Probably this is not a BeeGFS
			// mount so ignore it.
			continue
		}

		fsType := fields[2]
		if strings.HasPrefix(fsType, "beegfs") {
			opts := make(map[string]string)
			for config := range strings.SplitSeq(fields[3], ",") {
				kv := strings.SplitN(config, "=", 2)
				if len(kv) == 2 {
					opts[kv[0]] = kv[1]
				} else {
					opts[kv[0]] = ""
				}
			}
			// A mount without a cfgFile option is valid. The client then uses its defaults plus
			// the mount options. Such a mount can still be matched to its client by mount ID.
			mounts = append(mounts, MountPoint{
				Path: path.Clean(fields[1]),
				Opts: opts,
			})
			// Record this BeeGFS type
			fsTypesMap[fsType] = struct{}{}
		}
	}

	if err := scanner.Err(); err != nil {
		return mounts, nil, fmt.Errorf("unexpected error while scanning mounts: %w", err)
	}

	fsTypes := make([]string, 0, len(fsTypesMap))
	for t := range fsTypesMap {
		fsTypes = append(fsTypes, t)
	}

	return mounts, fsTypes, nil
}
