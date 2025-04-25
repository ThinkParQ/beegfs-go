package beeremote

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type grpcProvider struct {
	conn   *grpc.ClientConn
	client beeremote.BeeRemoteClient
}

var _ Provider = &grpcProvider{}

func (c *grpcProvider) init(config Config) error {
	var cert []byte
	var err error
	if !config.TlsDisable && config.TlsCertFile != "" {
		cert, err = os.ReadFile(config.TlsCertFile)
		if err != nil {
			return fmt.Errorf("reading certificate file failed: %w", err)
		}
	}
	conn, err := beegrpc.NewClientConn(
		config.dynamic.GetAddress(),
		beegrpc.WithTLSCaCert(cert),
		beegrpc.WithTLSDisableVerification(config.TLSDisableVerification),
		beegrpc.WithTLSDisable(config.TlsDisable),
	)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnableToConnect, err)
	}

	c.conn = conn
	c.client = beeremote.NewBeeRemoteClient(c.conn)
	return nil
}

func (c *grpcProvider) disconnect() error {
	return c.conn.Close()
}

func (c *grpcProvider) updateWork(ctx context.Context, workResult *flex.Work) error {
	_, err := c.client.UpdateWork(ctx, beeremote.UpdateWorkRequest_builder{Work: workResult}.Build())
	if err != nil {
		if st, ok := status.FromError(err); ok {
			// TLS misconfiguration can cause a confusing error message so we handle it explicitly.
			// Note this is just a hint to the user, other error conditions may have the same
			// message so we don't adjust behavior (i.e., treat it as fatal).
			if strings.Contains(st.Message(), "error reading server preface: EOF") {
				return fmt.Errorf("%w (hint: check TLS is configured correctly on the client and server)", err)
			}
		}
		return err
	}

	return nil
}

func (c *grpcProvider) submitJob(ctx context.Context, jobRequest *beeremote.JobRequest) error {
	_, err := c.client.SubmitJob(ctx, beeremote.SubmitJobRequest_builder{Request: jobRequest}.Build())
	if err != nil {
		if st, ok := status.FromError(err); ok {
			// TLS misconfiguration can cause a confusing error message so we handle it explicitly.
			// Note this is just a hint to the user, other error conditions may have the same
			// message so we don't adjust behavior (i.e., treat it as fatal).
			if strings.Contains(st.Message(), "error reading server preface: EOF") {
				return fmt.Errorf("%w (hint: check TLS is configured correctly on the client and server)", err)
			}
		}
		return err
	}

	return nil
}
