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
	"reflect"
	"strings"

	"github.com/thinkparq/beegfs-go/beegfs-ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"go.uber.org/zap"
)

var (
	ErrEstablishingConnections = errors.New("error forcing establishment of BeeGFS client/server connections")
)

const (
	mountProcDir  = "/proc/mounts"
	beeGFSProcDir = "/proc/fs/beegfs"
)

type GetBeeGFSClientsConfig struct {
	// Call df to force the client module to establish storage server connections.
	ForceConnections bool
}

type Client struct {
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

func GetBeeGFSClients(ctx context.Context, cfg GetBeeGFSClientsConfig) ([]Client, error) {

	logger, _ := config.GetLogger()
	log := logger.With(zap.String("component", path.Base(reflect.TypeOf(Client{}).PkgPath())))

	mounts, err := getBeeGFSMounts()
	if err != nil {
		log.Warn("unexpected error getting mounted file systems (ignoring)", zap.Error(err))
	}

	if cfg.ForceConnections {
		cmd := exec.CommandContext(ctx, "df", "-t", "beegfs")
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("%w: %w", ErrEstablishingConnections, err)
		}
	}

	clients := make([]Client, 0)
	err = filepath.Walk(beeGFSProcDir, func(path string, info os.FileInfo, err error) error {
		log := log.With(zap.String("procfsDir", path))
		if err != nil {
			log.Warn("unexpected error walking procfs directory (ignoring)", zap.Error(err))
			return nil
		}
		if info.IsDir() && path != beeGFSProcDir {
			log.Debug("collecting mount info from procfs")
			mount, err := parseClient(path, mounts)
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

	return clients, nil
}

// Parses a client from its procfs directory and associated it with its MountPoint (if available).
func parseClient(path string, mounts map[string]MountPoint) (Client, error) {
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
		return Client{}, err
	}
	client.MetaNodes, err = parseClientNodesFile(filepath.Join(path, "meta_nodes"))
	if err != nil {
		return Client{}, err
	}
	client.StorageNodes, err = parseClientNodesFile(filepath.Join(path, "storage_nodes"))
	if err != nil {
		return Client{}, err
	}

	// Parse the client ID:
	client.ID = filepath.Base(path)

	// Associate the client with its mount point:
	if cfgPath, ok := client.Config["cfgFile"]; ok {
		if m, ok := mounts[cfgPath]; ok {
			client.Mount = m
		}
	}
	return client, nil
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

	return clientConfig, nil
}

// parseClientNodesFile wraps parseNodes so that function can accept an interface for testing.
func parseClientNodesFile(path string) ([]Node, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening %s: %w", path, err)
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
			parts := strings.Split(line[len("Connections: "):], ";")
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part == "<none>" || part == "" {
					continue
				}
				peer := Peer{}
				if strings.HasPrefix(part, "TCP:") {
					peer.Type = beegfs.Ethernet
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

func getBeeGFSMounts() (map[string]MountPoint, error) {
	file, err := os.Open(mountProcDir)
	if err != nil {
		return nil, fmt.Errorf("error opening %s: %w", mountProcDir, err)
	}
	defer file.Close()
	return parseMounts(file)
}

func parseMounts(input io.Reader) (map[string]MountPoint, error) {

	mounts := map[string]MountPoint{}
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

		if fields[2] == "beegfs" {
			cfgFile := ""
			opts := make(map[string]string)
			for _, config := range strings.Split(fields[3], ",") {
				kv := strings.SplitN(config, "=", 2)
				if len(kv) == 2 {
					opts[kv[0]] = kv[1]
					if kv[0] == "cfgFile" {
						cfgFile = kv[1]
					}
				} else {
					opts[kv[0]] = ""
				}
			}
			if cfgFile != "" {
				mounts[cfgFile] = MountPoint{
					Path: path.Clean(fields[1]),
					Opts: opts,
				}
			} else {
				return nil, fmt.Errorf("invalid BeeGFS mount in %s (no config file): %s", mountProcDir, line)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return mounts, fmt.Errorf("unexpected error while scanning mounts: %w", err)
	}
	return mounts, nil
}
