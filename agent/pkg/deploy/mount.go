package deploy

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/thinkparq/beegfs-go/agent/pkg/manifest"
)

type Mounter interface {
	ApplyTargets(ctx context.Context, add []manifest.Target) error
	DestroyTargets(ctx context.Context, remove []manifest.Target) error
}

type Mount struct {
}

func (m *Mount) ApplyTargets(ctx context.Context, add []manifest.Target) error {
	for _, target := range add {
		if target.ULFS != nil {
			return fmt.Errorf("unable to apply target %d: formatting and/or mounting an underlying file system is not implemented yet", target.ID)
		}
		if err := os.MkdirAll(target.GetPath(), 0700); err != nil {
			return fmt.Errorf("unable to apply target %d: unable to create root directory %s: %w", target.ID, target.RootDir, err)
		}
	}
	return nil
}

func (m *Mount) DestroyTargets(ctx context.Context, remove []manifest.Target) error {
	return errors.New("not implemented")
}
