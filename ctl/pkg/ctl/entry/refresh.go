package entry

import (
	"context"
	"errors"
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/util"
	"go.uber.org/zap"
)

type RefreshEntryResult struct {
	Path    string
	Status  beegfs.OpsErr
	EntryID string
}

func RefreshEntriesInfo(ctx context.Context, paths InputMethod) (<-chan *RefreshEntryResult, <-chan error, error) {
	log, _ := config.GetLogger()
	store, err := config.NodeStore(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("error accessing the node store: %w", err)
	}

	mappings, err := util.GetMappings(ctx)
	if err != nil {
		if !errors.Is(err, util.ErrMappingRSTs) {
			return nil, nil, fmt.Errorf("unable to proceed without entity mappings: %w", err)
		}
		// RSTs are not configured on all BeeGFS instances, silently ignore.
		log.Debug("remote storage mappings are not available (ignoring)", zap.Any("error", err))
	}

	processEntry := func(path string) (*RefreshEntryResult, error) {
		return refreshEntryInfo(ctx, mappings, store, path)
	}
	return processEntries(ctx, paths, false, processEntry)
}

func refreshEntryInfo(ctx context.Context, mappings *util.Mappings, store *beemsg.NodeStore, path string) (*RefreshEntryResult, error) {
	entryInfo, ownerNode, err := GetEntryAndOwnerFromPath(ctx, mappings, path)
	if err != nil {
		return &RefreshEntryResult{}, err
	}

	var resp = &msg.RefreshEntryInfoResponse{}
	request := &msg.RefreshEntryInfoRequest{
		EntryInfo: entryInfo,
	}
	err = store.RequestTCP(ctx, ownerNode.Uid, request, resp)
	if err != nil {
		return &RefreshEntryResult{}, fmt.Errorf("error refreshing entry information from node: %w", err)
	}
	if resp.Result != beegfs.OpsErr_SUCCESS {
		return &RefreshEntryResult{Path: path, EntryID: string(entryInfo.EntryID), Status: resp.Result}, resp.Result
	}

	return &RefreshEntryResult{Path: path, EntryID: string(entryInfo.EntryID), Status: resp.Result}, nil
}
