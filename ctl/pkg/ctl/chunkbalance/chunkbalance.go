package chunkbalance

import (
	"context"
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/entry"
)

func ExecuteChunkBalance(ctx context.Context, entryVar *entry.GetEntryCombinedInfo, SourceID uint16, DestinationID uint16) error {

	store, err := config.NodeStore(ctx)
	if err != nil {
		return fmt.Errorf("error accessing the node store: %w", err)
	}

	metaRoot := store.GetMetaRootNode()
	if metaRoot == nil {
		return fmt.Errorf("unable to proceed without a working root metadata server")
	}

	currentNode := *metaRoot

	chunkPathList := []string{entryVar.Entry.Verbose.ChunkPath}
	fmt.Println(chunkPathList)

	// Create the ChunkBalanceMsg
	chunkBalanceMsgPrep := &msg.StartChunkBalanceMsg{
		TargetID:      SourceID,
		DestinationID: DestinationID,
		EntryInfo:     entryVar.Entry.OrigEntryInfoMsg,
		RelativePaths: &chunkPathList,
	}
	var resp = &msg.StartChunkBalanceRespMsg{}

	err = store.RequestTCP(ctx, currentNode.Uid, chunkBalanceMsgPrep, resp)
	if err != nil {
		return fmt.Errorf("error sending chunk balance msg: %w", err)
	}

	if resp.Result != 0 {
		return fmt.Errorf("failed to start chunk balance, result: %d", resp.Result)
	}
	return nil
}
