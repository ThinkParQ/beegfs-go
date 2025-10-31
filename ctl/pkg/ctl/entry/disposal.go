package entry

import (
	"context"
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/buddygroup"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"go.uber.org/zap"
)

// Equivalent of META_DISPOSALDIR_ID_STR in the C++ codebase.
var disposalDir = []byte("disposal")

// Equivalent of META_MIRRORDISPOSALDIR_ID_STR in the C++ codebase.
var disposalMirroredDir = []byte("mdisposal")

const (
	// Maximum number of entries that will be returned at one time.
	disposalMaxOutNames = 50
)

type DisposalCfg struct {
	// By default don't actually dispose (unlink) disposal entries, just return the list of entries
	// marked for disposal.
	Dispose bool
}

type DisposalResult struct {
	Node    beegfs.Node
	EntryID string
	// The result will be nil if disposal was not attempted.
	Result *beegfs.OpsErr
}

type disposalCleaner struct {
	ctx         context.Context
	store       *beemsg.NodeStore
	cfg         DisposalCfg
	resultsChan chan<- DisposalResult
	errChan     chan<- error
	log         *logger.Logger
}

// CleanupDisposals returns two channels where the results or errors running the disposal cleaner
// will be sent. It immediately returns an error if any initialization fails. The DisposalResult
// channel will be closed once all results have been returned, or if a fatal error occurs.
func CleanupDisposals(ctx context.Context, cfg DisposalCfg) (<-chan DisposalResult, <-chan error, error) {
	log, _ := config.GetLogger()

	store, err := config.NodeStore(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("error initializing node store: %w", err)
	}

	buddyMirrors, err := buddygroup.GetBuddyGroups(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get buddy groups from management: %w", err)
	}
	metaBGToPrimaryNode, err := util.MapMetaBuddyGroupToPrimaryNode(buddyMirrors, store)
	if err != nil {
		return nil, nil, err
	}

	resultsChan := make(chan DisposalResult, 1)
	errChan := make(chan error, 1)
	disposalCleaner := &disposalCleaner{
		ctx:         ctx,
		store:       store,
		cfg:         cfg,
		resultsChan: resultsChan,
		errChan:     errChan,
		log:         log,
	}

	go disposalCleaner.run(metaBGToPrimaryNode)
	return resultsChan, errChan, nil
}

func (d *disposalCleaner) run(metaBGToPrimaryNode util.Mapper[beegfs.EntityIdSet]) {
	defer close(d.resultsChan)
	defer close(d.errChan)

	nodes := d.store.GetNodes()
	primaryMetaMirrorNodes := make(map[beegfs.NumId]beegfs.LegacyId)
	for buddyGroup, primaryNode := range metaBGToPrimaryNode.LegacyIDs() {
		primaryMetaMirrorNodes[primaryNode.LegacyId.NumId] = buddyGroup
	}

	for _, node := range nodes {
		if node.Id.NodeType != beegfs.Meta {
			continue
		}
		d.log.Debug("walking non-mirrored entries on node", zap.Any("node", node))
		// First walk non-mirrored entries:
		err := d.walkNode(node, false)
		if err != nil {
			d.errChan <- err
			return
		}

		// Then walk mirrored entries:
		if bg, ok := primaryMetaMirrorNodes[node.Id.NumId]; ok {
			d.log.Debug("walking mirrored entries on node", zap.Any("node", node), zap.Any("buddyGroup", bg))
			// Skip mirrored metadata if the node is the secondary of its group. Otherwise we would
			// list and attempt to delete mirrored disposal files twice. If this node is not part of
			// a mirrored group we don't need to do anything.
			err = d.walkNode(node, true)
			if err != nil {
				d.errChan <- err
				return
			}
		}
	}
}

func (d *disposalCleaner) walkNode(node beegfs.Node, mirrored bool) error {

	numEntriesThisRound := 0
	var currentServerOffset int64 = 0

	for {
		var disposalDirEntry msg.EntryInfo
		if !mirrored {
			disposalDirEntry = msg.EntryInfo{
				OwnerID:       uint32(node.Id.NumId),
				ParentEntryID: []byte{},
				EntryID:       disposalDir,
				FileName:      disposalDir,
				EntryType:     beegfs.EntryDirectory,
			}
		} else {
			disposalDirEntry = msg.EntryInfo{
				OwnerID:       uint32(node.Id.NumId),
				ParentEntryID: []byte{},
				EntryID:       disposalMirroredDir,
				FileName:      disposalMirroredDir,
				EntryType:     beegfs.EntryDirectory,
			}
			disposalDirEntry.FeatureFlags.SetBuddyMirrored()
		}

		req := &msg.ListDirFromOffsetRequest{
			EntryInfo:    disposalDirEntry,
			ServerOffset: currentServerOffset,
			MaxOutNames:  disposalMaxOutNames,
			FilterDots:   true,
		}

		resp := &msg.ListDirFromOffsetResponse{}
		err := d.store.RequestTCP(d.ctx, node.Id, req, resp)
		if err != nil {
			// Failing to list the disposal directory is considered fatal.
			return fmt.Errorf("error listing disposal directory: %w", err)
		}

		numEntriesThisRound = len(resp.EntryIDs)
		currentServerOffset = resp.NewServerOffset
		for _, entry := range resp.EntryIDs {
			disposalResult := DisposalResult{
				Node:    node,
				EntryID: string(entry),
			}
			if d.cfg.Dispose {
				delReq := &msg.UnlinkFileRequest{
					ParentInfo:  disposalDirEntry,
					DelFileName: []byte(entry),
				}
				delResp := &msg.UnlinkFileResponse{}
				err = d.store.RequestTCP(d.ctx, node.Id, delReq, delResp)
				if err != nil {
					// Treat network request failures as fatal. If unlinking the entry fails due to
					// an internal metadata issue (i.e., its still open), this is indicated in the
					// response's result field.
					return fmt.Errorf("error unlinking entry ID %s: %w", entry, err)
				}
				disposalResult.Result = &delResp.Result
			}
			d.resultsChan <- disposalResult
		}
		d.log.Debug("finished RPC round", zap.Any("numEntries", numEntriesThisRound))
		if numEntriesThisRound != disposalMaxOutNames {
			break
		}
	}
	return nil
}
