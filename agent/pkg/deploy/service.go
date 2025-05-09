package deploy

import (
	"context"
	"errors"

	"github.com/thinkparq/beegfs-go/agent/pkg/manifest"
)

type Servicer interface {
	Apply(ctx context.Context, add manifest.Node) error
	Modify(ctx context.Context, old manifest.Node, new manifest.Node) error
	Destroy(ctx context.Context, remove manifest.Node) error
}

// Systemd provides a method to deploy BeeGFS nodes using systemd.
type Systemd struct {
}

func (d *Systemd) Apply(ctx context.Context, add manifest.Node) error {
	return errors.New("not implemented")
}

func (d *Systemd) Modify(ctx context.Context, old manifest.Node, new manifest.Node) error {
	return errors.New("not implemented")
}

func (d *Systemd) Destroy(ctx context.Context, remove manifest.Node) error {
	return errors.New("not implemented")
}
