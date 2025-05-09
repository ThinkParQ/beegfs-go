package deploy

// Deployer is responsible for carrying out the steps needed to manage a BeeGFS "node" and handles
// starting/modifying/stopping various system resources.
type Deployer interface {
	Sourcerer
	Networker
	Mounter
	Servicer
}

func NewDefaultStrategy() (Deployer, error) {
	pm, err := DetectPackageManager()
	if err != nil {
		return nil, err
	}
	return &defaultStrategy{
		Systemd: Systemd{},
		Mount:   Mount{},
		IP:      IP{},
		Package: Package{PackageManager: pm},
	}, nil
}

type defaultStrategy struct {
	Package // implements Sourcerer
	Mount   // implements Mounter
	IP      // implements Networker
	Systemd // implements Servicer
}
