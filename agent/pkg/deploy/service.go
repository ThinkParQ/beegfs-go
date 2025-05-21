package deploy

import (
	"context"
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

	if err := d.DestroyService(ctx, add); err != nil {
		return fmt.Errorf("error destroying existing service to apply updates: %w", err)
	}

	cmd := append([]string{add.Executable}, add.GetConfig()...)
	properties := []dbus.Property{
		dbus.PropExecStart(cmd, false),
		dbus.PropDescription(add.GetDescription()),
		dbus.PropRemainAfterExit(false),
		dbus.PropType("simple"),
	}
	_, err := d.conn.StartTransientUnitContext(ctx, add.GetSystemdUnit(), "replace", properties, nil)
	if err != nil {
		return fmt.Errorf("failed to start transient unit %s: %w", add.GetSystemdUnit(), err)
	}
	return nil
}

func (d *Systemd) DestroyService(ctx context.Context, remove manifest.Service) error {
	units, err := d.conn.ListUnitsByNamesContext(ctx, []string{remove.GetSystemdUnit()})
	if err != nil {
		return fmt.Errorf("error querying systemd units: %w", err)
	}

	if len(units) == 0 || units[0].LoadState == "not-found" {
		return nil
	}

	ch := make(chan string)
	_, err = d.conn.StopUnitContext(ctx, remove.GetSystemdUnit(), "replace", ch)
	if err != nil {
		return err
	}
	select {
	case <-ch:
		if units[0].SubState == "failed" {
			if err := d.conn.ResetFailedUnitContext(ctx, remove.GetSystemdUnit()); err != nil {
				return fmt.Errorf("error resetting failed unit context: %w", err)
			}
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
