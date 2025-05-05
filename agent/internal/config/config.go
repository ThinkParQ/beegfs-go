package config

import (
	"github.com/thinkparq/beegfs-go/agent/internal/server"
	"github.com/thinkparq/beegfs-go/agent/pkg/agent"
	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/logger"
)

type AppConfig struct {
	Log       logger.Config `mapstructure:"log"`
	Agent     agent.Config  `mapstructure:"agent"`
	Server    server.Config `mapstructure:"server"`
	Developer struct {
		DumpConfig bool `mapstructure:"dump-config"`
	}
}

func (c *AppConfig) NewEmptyInstance() configmgr.Configurable {
	return new(AppConfig)
}

func (c *AppConfig) UpdateAllowed(newConfig configmgr.Configurable) error {
	return nil
}

func (c *AppConfig) ValidateConfig() error {
	return nil
}
