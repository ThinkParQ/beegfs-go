package deploy

import (
	"context"
	"errors"
	"fmt"

	"github.com/coreos/go-systemd/v22/dbus"
	"github.com/thinkparq/beegfs-go/agent/pkg/manifest"
)

type Servicer interface {
	ApplyService(ctx context.Context, add manifest.Service) error
	DestroyService(ctx context.Context, remove manifest.Service) error
}

func NewSystemd(ctx context.Context) (Systemd, error) {
	conn, err := dbus.NewSystemConnectionContext(ctx)
	if err != nil {
		return Systemd{}, fmt.Errorf("unable to connect to the system bus: %w", err)
	}
	return Systemd{
		conn: conn,
	}, nil

}

// Systemd provides a method to deploy BeeGFS services using systemd.
type Systemd struct {
	conn *dbus.Conn
}

func (d *Systemd) Cleanup() error {
	d.conn.Close()
	return nil
}

func (d *Systemd) ApplyService(ctx context.Context, add manifest.Service) error {
	return errors.New("not implemented")
}

func (d *Systemd) DestroyService(ctx context.Context, remove manifest.Service) error {
	return errors.New("not implemented")
}
