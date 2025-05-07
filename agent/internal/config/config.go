package config

import (
	"github.com/thinkparq/beegfs-go/agent/internal/server"
	"github.com/thinkparq/beegfs-go/agent/pkg/reconciler"
	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/logger"
)

type AppConfig struct {
	AgentID    string            `mapstructure:"agent-id"`
	Log        logger.Config     `mapstructure:"log"`
	Reconciler reconciler.Config `mapstructure:"reconciler"`
	Server     server.Config     `mapstructure:"server"`
	Developer  struct {
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

// GetReconcilerConfig returns only the part of an AppConfig expected by the reconciler.
func (c *AppConfig) GetReconcilerConfig() reconciler.Config {
	return c.Reconciler
}
