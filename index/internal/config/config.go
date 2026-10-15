// Package config holds the beegfs-index-sync configuration that configmgr reads from flags,
// environment variables and the TOML file.
package config

import (
	"errors"

	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/index/internal/collector"
	"github.com/thinkparq/beegfs-go/index/internal/markwalk"
)

// Verify all interfaces that depend on AppConfig are satisfied.
var _ configmgr.Configurable = &AppConfig{}
var _ logger.Configurer = &AppConfig{}

type AppConfig struct {
	Collector collector.Config `mapstructure:"collector"`
	Markwalk  markwalk.Config  `mapstructure:"markwalk"`
	Log       logger.Config    `mapstructure:"log"`
}

func (c *AppConfig) GetLoggingConfig() logger.Config {
	return c.Log
}

// NewEmptyInstance returns an empty AppConfig for configmgr to unmarshal into.
func (c *AppConfig) NewEmptyInstance() configmgr.Configurable {
	return new(AppConfig)
}

// UpdateAllowed is never consulted: the configuration is read once at start.
func (c *AppConfig) UpdateAllowed(configmgr.Configurable) error {
	return nil
}

// ValidateConfig reports every bad value at once.
func (c *AppConfig) ValidateConfig() error {
	var errs []error
	if co := c.Collector; co.BatchEvents < 1 || co.Queue < 1 || co.BatchQueue < 1 || co.BatchEvery <= 0 {
		errs = append(errs, errors.New("collector.batch-events, collector.queue and collector.batch-queue must be at least 1, collector.batch-every above 0"))
	}
	if m := c.Markwalk; m.Query == "" || m.QueryTimeout <= 0 {
		errs = append(errs, errors.New("markwalk.query is required, markwalk.query-timeout must be above 0"))
	}
	return errors.Join(errs...)
}
