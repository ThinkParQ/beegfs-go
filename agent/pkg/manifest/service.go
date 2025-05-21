package manifest

import (
	"fmt"
	"path/filepath"

	"github.com/thinkparq/beegfs-go/common/beegfs"
)

type Service struct {
	// shortUUID is set by InheritGlobalConfig and used internally to generate globally unique names
	// and identifiers in case resources for multiple file systems exist on the same machine.
	shortUUID  string
	longUUID   string
	ID         beegfs.NumId      `yaml:"id"`
	Type       beegfs.NodeType   `yaml:"type"`
	Config     map[string]string `yaml:"config"`
	Interfaces []Nic             `yaml:"interfaces"`
	Targets    []Target          `yaml:"targets"`
	Executable string            `yaml:"executable"`
}

// GetTargetsConfig returns the string used to initialize
func (s Service) GetTargetsConfig() (map[string]string, error) {
	switch s.Type {
	case beegfs.Management:
		if len(s.Targets) != 1 {
			return nil, fmt.Errorf("invalid number of targets for node type %s: %d", s.Type.String(), len(s.Targets))
		}
		path := filepath.Clean(s.Targets[0].GetPath() + "/mgmtd.sqlite")
		return map[string]string{"db-file": path}, nil
	default:
		return nil, nil
	}

	// TODO: Implement remaining node types.
	// return "", nil, fmt.Errorf("unsupported node type: %v", s.Type)
}

func (s Service) GetDescription() string {
	return fmt.Sprintf("BeeGFS %s-%s-%d (managed by BeeOND)", s.shortUUID, s.Type, s.ID)
}

func (s Service) GetSystemdUnit() string {
	return fmt.Sprintf("%s-beegfs-%s-%d.service", s.shortUUID, s.Type, s.ID)
}

func (s Service) GetConfig() []string {
	config := make([]string, 0, len(s.Config))
	for k, v := range s.Config {
		if s.Type == beegfs.Management {
			config = append(config, fmt.Sprintf("--%s=%v", k, v))
		} else {
			config = append(config, fmt.Sprintf("%s=%v", k, v))
		}
	}
	return config
}
