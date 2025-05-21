package deploy

import (
	"context"
	"errors"
	"fmt"
	"os/exec"

	"github.com/thinkparq/beegfs-go/agent/pkg/manifest"
)

type Installer interface {
	ApplySourceRepo(ctx context.Context, add manifest.InstallSource) error
	DeleteSourceRepo(ctx context.Context, remove manifest.InstallSource) error
	ApplyInstall(ctx context.Context, ref string) error
	DeleteInstall(ctx context.Context, ref string) error
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
	manager Installer
	// isLocal is set if the manifest specifies the source type is local. This indicates all package
	// manager operations should be a no-op for this FS in the manifest. This allows the manifest to
	// fully control the installation source independent of the deployment strategy for each agent.
	isLocal bool
}

func (p *Package) ApplySourceRepo(ctx context.Context, add manifest.InstallSource) error {
	if add.Type == manifest.LocalInstall {
		p.isLocal = true
		return nil
	}
	return p.manager.ApplySourceRepo(ctx, add)
}

func (p *Package) DeleteSourceRepo(ctx context.Context, remove manifest.InstallSource) error {
	if remove.Type == manifest.LocalInstall {
		p.isLocal = false
		return nil
	}
	return p.manager.DeleteSourceRepo(ctx, remove)
}

func (p *Package) ApplyInstall(ctx context.Context, ref string) error {
	if p.isLocal {
		return nil
	}
	return p.manager.ApplyInstall(ctx, ref)
}

func (p *Package) DeleteInstall(ctx context.Context, ref string) error {
	if p.isLocal {
		return nil
	}
	return p.manager.DeleteInstall(ctx, ref)
}

type AptPackage struct{}

func (p *AptPackage) ApplySourceRepo(ctx context.Context, add manifest.InstallSource) error {
	return errors.New("not implemented")
}

func (p *AptPackage) DeleteSourceRepo(ctx context.Context, remove manifest.InstallSource) error {
	return errors.New("not implemented")
}

func (p *AptPackage) ApplyInstall(ctx context.Context, ref string) error {
	return errors.New("not implemented")
}

func (p *AptPackage) DeleteInstall(ctx context.Context, ref string) error {
	return errors.New("not implemented")
}
