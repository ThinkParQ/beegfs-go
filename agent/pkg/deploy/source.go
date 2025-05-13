package deploy

import (
	"context"
	"errors"
	"fmt"
	"os/exec"

	"github.com/thinkparq/beegfs-go/agent/pkg/manifest"
)

type Sourcerer interface {
	ApplySource(ctx context.Context, add manifest.Source) error
	DeleteSource(ctx context.Context, remove manifest.Source) error
	ApplySourceInstall(ctx context.Context, source manifest.NodeSource) error
	DeleteSourceInstall(ctx context.Context, source manifest.NodeSource) error
}

func DetectPackageManager() (Package, error) {
	isExecutableInPath := func(name string) bool {
		_, err := exec.LookPath(name)
		return err == nil
	}
	switch {
	case isExecutableInPath("apt"):
		return Package{
			manager: &AptPackage{},
		}, nil
	}
	return Package{}, fmt.Errorf("detecting package manager: unsupported or undetectable package manager")
}

// Package provides the ability to install BeeGFS using the package manager. It implements any
// general functionality and defers to the actual manager based on the specific distribution.
type Package struct {
	manager Sourcerer
	// isLocal is set if the manifest specifies the source type is local. This indicates all package
	// manager operations should be a no-op for this FS in the manifest. This allows the manifest to
	// fully control the installation source independent of the deployment strategy for each agent.
	isLocal bool
}

func (p *Package) ApplySource(ctx context.Context, add manifest.Source) error {
	if add.Type == manifest.LocalSource {
		p.isLocal = true
		return nil
	}
	return p.manager.ApplySource(ctx, add)
}

func (p *Package) DeleteSource(ctx context.Context, remove manifest.Source) error {
	if remove.Type == manifest.LocalSource {
		p.isLocal = false
		return nil
	}
	return p.manager.DeleteSource(ctx, remove)
}

func (p *Package) ApplySourceInstall(ctx context.Context, source manifest.NodeSource) error {
	if p.isLocal || source.Type == manifest.LocalSource {
		return nil
	}
	return p.manager.ApplySourceInstall(ctx, source)
}

func (p *Package) DeleteSourceInstall(ctx context.Context, source manifest.NodeSource) error {
	if p.isLocal || source.Type == manifest.LocalSource {
		return nil
	}
	return p.manager.DeleteSourceInstall(ctx, source)
}

type AptPackage struct{}

func (p *AptPackage) ApplySource(ctx context.Context, add manifest.Source) error {
	return errors.New("not implemented")
}

func (p *AptPackage) DeleteSource(ctx context.Context, remove manifest.Source) error {
	return errors.New("not implemented")
}

func (p *AptPackage) ApplySourceInstall(ctx context.Context, source manifest.NodeSource) error {
	return errors.New("not implemented")
}

func (p *AptPackage) DeleteSourceInstall(ctx context.Context, source manifest.NodeSource) error {
	return errors.New("not implemented")
}
