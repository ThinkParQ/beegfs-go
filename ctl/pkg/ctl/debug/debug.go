package node

import (
	"context"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

func GenericDebugCmd(ctx context.Context, node beegfs.EntityId, command string) (string, error) {
	store, err := config.NodeStore(ctx)
	if err != nil {
		return "", err
	}

	resp := msg.GenericDebugResp{}
	err = store.RequestTCP(ctx, node, &msg.GenericDebug{Command: []byte(command)}, &resp)
	if err != nil {
		return "", err
	}

	return string(resp.Response), nil
}
