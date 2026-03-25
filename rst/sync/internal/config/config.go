package config

import (
	"fmt"

	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/telemetry"
	"github.com/thinkparq/beegfs-go/rst/sync/internal/beeremote"
	"github.com/thinkparq/beegfs-go/rst/sync/internal/server"
	"github.com/thinkparq/beegfs-go/rst/sync/internal/workmgr"
)

// We use ConfigManager to handle configuration updates.
// Verify all interfaces that depend on AppConfig are satisfied.
var _ configmgr.Configurable = &AppConfig{}
var _ telemetry.Configurer = &AppConfig{}
var _ logger.Configurer = &AppConfig{}

type AppConfig struct {
	MountPoint string           `mapstructure:"mount-point"`
	WorkMgr    workmgr.Config   `mapstructure:"manager"`
	BeeRemote  beeremote.Config `mapstructure:"remote"`
	Server     server.Config    `mapstructure:"server"`
	Log        logger.Config    `mapstructure:"log"`
	Telemetry  telemetry.Config `mapstructure:"telemetry"`
	Developer  struct {
		PerfProfilingPort int  `mapstructure:"perf-profiling-port"`
		DumpConfig        bool `mapstructure:"dump-config"`
	}
}

func (c *AppConfig) GetLoggingConfig() logger.Config {
	return c.Log
}

func (c *AppConfig) GetTelemetryConfig() telemetry.Config {
	return c.Telemetry
}

// NewEmptyInstance() returns an empty AppConfig for ConfigManager to use with
// when unmarshalling the configuration.
func (c *AppConfig) NewEmptyInstance() configmgr.Configurable {
	return new(AppConfig)
}

func (c *AppConfig) UpdateAllowed(newConfig configmgr.Configurable) error {
	// Currently all configuration allowed to be dynamically updated is provided by BeeRemote and
	// ConfigMgr is not involved with it.
	return nil
}
func (c *AppConfig) ValidateConfig() error {
	if c.WorkMgr.NumWorkers <= 0 {
		return fmt.Errorf("at least one worker is required to start (specified number of workers: %d)", c.WorkMgr.NumWorkers)
	}
	if c.WorkMgr.ActiveWorkQueueSize <= 0 {
		return fmt.Errorf("the active work queue size must at least be one (specified size: %d)", c.WorkMgr.ActiveWorkQueueSize)
	}
	if err := c.Telemetry.ValidateConfig(); err != nil {
		return err
	}
	return nil
}
