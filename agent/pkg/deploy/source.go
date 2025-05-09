package deploy

import (
	"context"
	"errors"
	"fmt"

	"github.com/thinkparq/beegfs-go/agent/pkg/manifest"
)

type Sourcerer interface {
	AddSource(ctx context.Context, add manifest.Source) error
	UpdateSource(ctx context.Context, old manifest.Source, new manifest.Source) error
	DeleteSource(ctx context.Context, remove manifest.Source) error
}

// Package provides the ability to install BeeGFS using the package manager.
type Package struct {
	PackageManager
}

type PackageManager interface {
	InstallRepo(ctx context.Context, repo string) error
	RemoveRepo(ctx context.Context, repo string) error
	InstallPackage(ctx context.Context, pkg string) error
	RemovePackage(ctx context.Context, pkg string) error
}

func DetectPackageManager() (PackageManager, error) {
	return nil, fmt.Errorf("detecting package manager: unsupported or undetectable package manager")
}

func (p *Package) AddSource(ctx context.Context, add manifest.Source) error {
	return errors.New("not implemented")
}

func (p *Package) UpdateSource(ctx context.Context, old manifest.Source, new manifest.Source) error {
	return errors.New("not implemented")
}

func (p *Package) DeleteSource(ctx context.Context, remove manifest.Source) error {
	return errors.New("not implemented")
}
