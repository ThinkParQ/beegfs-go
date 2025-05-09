package deploy

import (
	"context"
	"errors"

	"github.com/thinkparq/beegfs-go/agent/pkg/manifest"
)

type Mounter interface {
	AddTargets(ctx context.Context, add []manifest.Target) error
	ModifyTargets(ctx context.Context, old []manifest.Target, new []manifest.Target) error
	DestroyTargets(ctx context.Context, remove []manifest.Target) error
}

type Mount struct {
}

func (m *Mount) AddTargets(ctx context.Context, add []manifest.Target) error {
	return errors.New("not implemented")
}

func (m *Mount) ModifyTargets(ctx context.Context, old []manifest.Target, new []manifest.Target) error {
	return errors.New("not implemented")
}

func (m *Mount) DestroyTargets(ctx context.Context, remove []manifest.Target) error {
	return errors.New("not implemented")
}
