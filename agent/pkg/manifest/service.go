package manifest

import (
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beegfs"
)

type Service struct {
	// shortUUID is set by InheritGlobalConfig and used internally to generate globally unique names
	// and identifiers in case resources for multiple file systems exist on the same machine.
	shortUUID  string
	ID         beegfs.NumId      `yaml:"id"`
	Type       beegfs.NodeType   `yaml:"type"`
	Config     map[string]string `yaml:"config"`
	Interfaces []Nic             `yaml:"interfaces"`
	Targets    []Target          `yaml:"targets"`
	Executable string            `yaml:"executable"`
}

func (s Service) GetSystemdUnit() string {
	return fmt.Sprintf("beegfs-%s-%s-%d.service", s.shortUUID, s.Type, s.ID)
}

type ServiceInstallSource struct {
	Type InstallType `yaml:"type"`
	Ref  string      `yaml:"ref"`
}
