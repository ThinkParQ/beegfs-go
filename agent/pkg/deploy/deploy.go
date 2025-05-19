package deploy

import "context"

// Deployer is responsible for carrying out the steps needed to manage a BeeGFS "service" and
// handles starting/modifying/stopping various system resources.
type Deployer interface {
	Installer
	Networker
	Mounter
	Servicer
	// Cleanup should be called once the deployer is no longer needed to cleanup any long lived
	// resources created by setting up a particular deployment strategy.
	Cleanup() error
}

func NewDefaultStrategy(ctx context.Context) (Deployer, error) {
	packageManager, err := DetectPackageManager()
	if err != nil {
		return nil, err
	}

	systemd, err := NewSystemd(ctx)
	if err != nil {
		return nil, err
	}
	return &defaultStrategy{
		Package: packageManager,
		IP:      IP{},
		Mount:   Mount{},
		Systemd: systemd,
	}, nil
}

type defaultStrategy struct {
	Package // implements Sourcerer
	IP      // implements Networker
	Mount   // implements Mounter
	Systemd // implements Servicer
}

func (s *defaultStrategy) Cleanup() error {
	return s.Systemd.Cleanup()
}
