package beeremote

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"

	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
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

const (
	syncMgmtdTLSCertFile = "/etc/beegfs/.beegfs-sync-mgmtd-tls-cert"
	syncAuthFile         = "/etc/beegfs/.beegfs-sync-auth-file"
)

func (c *grpcProvider) init(cfg Config) error {
	var cert []byte
	var err error
	if !cfg.TlsDisable && cfg.TlsCertFile != "" {
		cert, err = os.ReadFile(cfg.TlsCertFile)
		if err != nil {
			return fmt.Errorf("reading certificate file failed: %w", err)
		}
	}
	conn, err := beegrpc.NewClientConn(
		cfg.dynamic.GetAddress(),
		beegrpc.WithTLSCaCert(cert),
		beegrpc.WithTLSDisableVerification(cfg.TLSDisableVerification),
		beegrpc.WithTLSDisable(cfg.TlsDisable),
		beegrpc.WithProxy(cfg.UseProxy),
	)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnableToConnect, err)
	}

	c.conn = conn
	c.client = beeremote.NewBeeRemoteClient(c.conn)

	if !cfg.dynamic.MgmtdTlsDisable && len(cfg.dynamic.MgmtdTlsCert) > 0 {
		if err := os.WriteFile(syncMgmtdTLSCertFile, cfg.dynamic.MgmtdTlsCert, 0600); err != nil {
			return fmt.Errorf("unable to update management certificate: %w", err)
		}
	}

	if !cfg.dynamic.AuthDisable && len(cfg.dynamic.AuthSecret) > 0 {
		if err := os.WriteFile(syncAuthFile, cfg.dynamic.AuthSecret, 0600); err != nil {
			return fmt.Errorf("unable to update beegfs authentication secret: %w", err)
		}
	}

	config.InitViperFromExternal(
		config.GlobalConfig{
			MgmtdAddress:                cfg.dynamic.MgmtdAddress,
			MgmtdTLSCertFile:            syncMgmtdTLSCertFile,
			MgmtdTLSDisableVerification: cfg.dynamic.MgmtdTlsDisableVerification,
			MgmtdTLSDisable:             cfg.dynamic.MgmtdTlsDisable,
			MgmtdUseProxy:               cfg.dynamic.MgmtdUseProxy,
			AuthFile:                    syncAuthFile,
			AuthDisable:                 cfg.dynamic.AuthDisable,
			RemoteAddress:               cfg.dynamic.Address,
			LogLevel:                    3,
			NumWorkers:                  runtime.GOMAXPROCS(0),
			ConnTimeoutMs:               500,
		},
	)
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
