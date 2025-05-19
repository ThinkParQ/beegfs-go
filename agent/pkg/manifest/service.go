package manifest

import (
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beegfs"
)

type Service struct {
	// fsUUID is set by InheritGlobalConfig and used internally to generate globally unique names
	// and identifiers in case resources for multiple file systems exist on the same machine.
	fsUUID        string
	ID            beegfs.NumId          `yaml:"id"`
	Type          beegfs.NodeType       `yaml:"type"`
	Config        map[string]string     `yaml:"config"`
	Interfaces    []Nic                 `yaml:"interfaces"`
	Targets       []Target              `yaml:"targets"`
	InstallSource *ServiceInstallSource `yaml:"install-source,omitempty"`
}

func (s Service) GetSystemdUnit() string {
	return fmt.Sprintf("beegfs-%s-%s-%d.service", s.fsUUID, s.Type, s.ID)
}

type ServiceInstallSource struct {
	Type InstallType `yaml:"type"`
	Ref  string      `yaml:"ref"`
}
