package manifest

import (
	"fmt"

	"github.com/thinkparq/beegfs-go/common/beegfs"
)

type Node struct {
	// fsUUID is set by InheritGlobalConfig and used internally to generate globally unique names
	// and identifiers in case resources for multiple file systems exist on the same machine.
	fsUUID        string
	ID            beegfs.NumId       `yaml:"id"`
	Type          beegfs.NodeType    `yaml:"type"`
	Config        map[string]string  `yaml:"config"`
	Interfaces    []Nic              `yaml:"interfaces"`
	Targets       []Target           `yaml:"targets"`
	InstallSource *NodeInstallSource `yaml:"install-source,omitempty"`
}

func (n Node) GetSystemdUnit() string {
	return fmt.Sprintf("beegfs-%s-%s-%d.service", n.fsUUID, n.Type, n.ID)
}

type NodeInstallSource struct {
	Type InstallType `yaml:"type"`
	Ref  string      `yaml:"ref"`
}
