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

// AppConfig is the whole configuration. Spool is the segment directory both halves share.
type AppConfig struct {
	Spool     string           `mapstructure:"spool"`
	Collector collector.Config `mapstructure:"collector"`
	Markwalk  markwalk.Config  `mapstructure:"markwalk"`
	Log       logger.Config    `mapstructure:"log"`
}

// GetLoggingConfig returns the logger configuration.
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
	if co := c.Collector; co.RollEvents < 1 || co.Queue < 1 || co.SealQueue < 1 || co.RollEvery <= 0 {
		errs = append(errs, errors.New("collector.roll-events, collector.queue and collector.seal-queue must be at least 1, collector.roll-every above 0"))
	}
	if m := c.Markwalk; m.MaxSegments < 1 || m.StatCache < 1 || m.Every <= 0 {
		errs = append(errs, errors.New("markwalk.max-segments and markwalk.stat-cache must be at least 1, markwalk.every above 0"))
	}
	return errors.Join(errs...)
}
