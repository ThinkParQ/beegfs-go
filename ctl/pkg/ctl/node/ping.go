package node

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/ioctl"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
	"go.uber.org/zap"
)

type PingConfig struct {
	Mountpoint string
	NodeType   beegfs.NodeType
	NodeIDs    []beegfs.EntityId
	Count      uint32
	Interval   time.Duration
	Parallel   bool
}

type PingResult struct {
	// The node we were asked to ping, resolved from the nodeStore. Useful for more detailed logging
	// in the frontend
	Node beegfs.Node
	// A Go version of the ioctl results struct `pingNodeArgResults` to pass to the frontend
	OutNode      string
	OutSuccess   uint32
	OutErrors    uint32
	OutTotalTime uint32
	OutPingTime  []uint32
	OutPingType  []string
}

type PingError struct {
	NodeID beegfs.EntityId
	Inner  error
}

func (p PingError) Error() string {
	return fmt.Sprintf("error during node ping: %v", p.Inner)
}

func PingNodes(ctx context.Context, cfg PingConfig) (<-chan *PingResult, <-chan *PingError, error) {
	log, _ := config.GetLogger()

	mgmtd, err := config.ManagementClient()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to connect to management: %w", err)
	}
	ctlFsUUID, err := mgmtd.GetFsUUID(ctx)
	if err != nil {
		return nil, nil, err
	}

	clientsConf := procfs.GetBeeGFSClientsConfig{
		FilterByUUID: ctlFsUUID,
	}
	if cfg.Mountpoint != "" {
		clientsConf.FilterByMounts = append(clientsConf.FilterByMounts, cfg.Mountpoint)
	}
	clients, err := procfs.GetBeeGFSClients(ctx, clientsConf, log)
	if err != nil {
		return nil, nil, fmt.Errorf("error while scanning mount points: %w", err)
	}
	if len(clients) == 0 {
		if cfg.Mountpoint != "" {
			return nil, nil, fmt.Errorf("file system at mount point \"%s\" is not managed by the configured mgmtd", cfg.Mountpoint)
		} else {
			return nil, nil, fmt.Errorf("unable to find at least one BeeGFS mount point")
		}
	}
	client := clients[0] // By now, all clients should be equivalent

	toPing := []beegfs.Node{}
	nodes, err := config.NodeStore(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to download nodes: %w", err)
	}

	results := make(chan *PingResult, 1)
	errs := make(chan *PingError, 1)
	go func() {
		defer close(results)
		defer close(errs)

		if len(cfg.NodeIDs) > 0 {
			// nodeIDs explicitly configured
			for _, id := range cfg.NodeIDs {
				if n, err := nodes.GetNode(id); err == nil {
					toPing = append(toPing, n)
					log.Debug("Found configured node. Pinging", zap.Any("node", n))
					continue
				} else {
					errs <- &PingError{
						NodeID: id,
						Inner:  fmt.Errorf("unable to find configured node in this file system"),
					}
				}
			}
		} else {
			// No nodeIDs supplied, get and ping all nodes of either explicitly configured type
			// or any type if none configured
			for _, n := range nodes.GetNodes() {
				log.Debug("Considering to ping", zap.Any("node", n))
				nodeType := beegfs.NodeType(n.Id.NodeType)
				if nodeType != beegfs.Meta && nodeType != beegfs.Storage && nodeType != beegfs.Management {
					// We only ever ping management, meta or storage nodes
					log.Debug("Can not ping node of this type:", zap.Any("node", n))
					continue
				}

				if nodeType == cfg.NodeType || cfg.NodeType == beegfs.InvalidNodeType {
					toPing = append(toPing, n)
					log.Debug("Node queued for pinging:", zap.Any("node", n))
				}
			}
		}

		wg := new(sync.WaitGroup)
		workers := 1
		if cfg.Parallel {
			workers = viper.GetInt(config.NumWorkersKey)
			log.Debug(fmt.Sprintf("Pinging in parallel with %d workers", workers))
		}
		pingChan := make(chan beegfs.Node, workers)
		for i := 0; i < workers; i++ {
			go func() {
				defer wg.Done()
				for {
					n, ok := <-pingChan
					if !ok {
						return
					}
					res, err := ioctl.PingNode(client.Mount.Path, n.Id, cfg.Count, cfg.Interval)
					if err != nil {
						errs <- &PingError{
							NodeID: n.Id,
							Inner:  err,
						}
						return
					}
					out := PingResult{
						Node:         n,
						OutNode:      strings.Trim(string(res.OutNode[:]), "\x00"),
						OutSuccess:   res.OutSuccess,
						OutErrors:    res.OutErrors,
						OutTotalTime: res.OutTotalTime,
						OutPingTime:  res.OutPingTime[:],
					}
					for _, tpe := range res.OutPingType {
						out.OutPingType = append(out.OutPingType, strings.Trim(string(tpe[:]), "\x00"))
					}
					results <- &out
				}
			}()
			wg.Add(1)
		}

		for _, n := range slices.SortedFunc(slices.Values(toPing), func(a beegfs.Node, b beegfs.Node) int {
			if a.Id.NodeType == b.Id.NodeType {
				return int(a.Id.NumId - b.Id.NumId)
			} else {
				return int(a.Id.NodeType - b.Id.NodeType)
			}
		}) {
			pingChan <- n
		}
		close(pingChan)

		wg.Wait()
	}()

	return results, errs, nil
}
