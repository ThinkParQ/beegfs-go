package deploy

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"

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
			return fmt.Errorf("unable to apply target %d: unable to create root directory %s: %w", target.ID, target.Path, err)
		}
		name, args := target.GetInitCmd()
		output, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
		if err != nil {
			if !strings.Contains(string(output), "already exists") {
				return fmt.Errorf("unable to initialize target %d: %s (%w)", target.ID, output, err)
			}
		}
	}
	return nil
}

func (m *Mount) DestroyTargets(ctx context.Context, remove []manifest.Target) error {
	return errors.New("not implemented")
}
