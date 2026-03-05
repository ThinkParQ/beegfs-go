package node

import (
	"context"
	"fmt"
	"net"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/common/beemsg/util"
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

func GenericDebugCmdByAddr(ctx context.Context, addr string, command string) (string, error) {
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return "", fmt.Errorf("invalid node address %q: %w", addr, err)
	}

	authSecret, err := config.AuthSecret()
	if err != nil {
		return "", err
	}

	conns := util.NewNodeConns()
	defer conns.CleanUp()

	resp := msg.GenericDebugResp{}
	err = conns.RequestTCP(
		ctx,
		[]string{addr},
		authSecret,
		viper.GetDuration(config.ConnTimeoutKey),
		&msg.GenericDebug{Command: []byte(command)},
		&resp,
	)
	if err != nil {
		return "", fmt.Errorf("TCP request to %s failed: %w", addr, err)
	}

	return string(resp.Response), nil
}
