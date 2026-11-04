// Package config holds the beegfs-index-updater configuration that configmgr reads from flags,
// environment variables and the TOML file.
package config

import (
	"errors"
	"fmt"

	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/index/internal/collector"
	"github.com/thinkparq/beegfs-go/index/internal/updater"
)

// Verify all interfaces that depend on AppConfig are satisfied.
var _ configmgr.Configurable = &AppConfig{}
var _ logger.Configurer = &AppConfig{}

type Management struct {
	Address                string `mapstructure:"address"`
	TLSCertFile            string `mapstructure:"tls-cert-file"`
	TLSDisableVerification bool   `mapstructure:"tls-disable-verification"`
	TLSDisable             bool   `mapstructure:"tls-disable"`
	UseProxy               bool   `mapstructure:"use-http-proxy"`
	AuthFile               string `mapstructure:"auth-file"`
	AuthDisable            bool   `mapstructure:"auth-disable"`
}

type AppConfig struct {
	Collector  collector.Config `mapstructure:"collector"`
	Management Management       `mapstructure:"management"`
	Update     updater.Config   `mapstructure:"update"`
	Log        logger.Config    `mapstructure:"log"`
}

func (c *AppConfig) GetLoggingConfig() logger.Config {
	return c.Log
}

func (c *AppConfig) NewEmptyInstance() configmgr.Configurable {
	return new(AppConfig)
}

// UpdateAllowed is never consulted: the configuration is read once at start.
func (c *AppConfig) UpdateAllowed(configmgr.Configurable) error {
	return nil
}

// ValidateConfig reports every bad value at once. Two of the rules guard against a service that
// starts and then quietly does nothing:
//   - collector.ack-frequency must be above 0. With 0 the subscriber SDK never answers Watch's
//     connect, and Watch, by default, waits for that answer forever before it sends any event.
//   - update.work is required. Empty, suspect files would land in the working directory, and
//     the updater, which lists the work directory, would never find them.
func (c *AppConfig) ValidateConfig() error {
	var errs []error
	if co := c.Collector; co.Listen == "" || co.Checkpoint == "" {
		errs = append(errs, errors.New("collector.listen and collector.checkpoint are required"))
	}
	if co := c.Collector; co.BatchEvents < 1 || co.BatchEvery <= 0 || co.MaxInodes < 1 || co.AckFrequency <= 0 {
		errs = append(errs, errors.New("collector.batch-events and collector.max-inodes must be at least 1, collector.batch-every and collector.ack-frequency above 0"))
	}
	if co := c.Collector; co.Query == "" || co.Sqlite == "" {
		errs = append(errs, errors.New("collector.query and collector.sqlite are required"))
	}
	if u := c.Update; u.Work == "" || u.Index == "" {
		errs = append(errs, errors.New("update.work and update.index are required"))
	}
	if u := c.Update; u.MaxWorkSize < 1 || u.MaxWorkSize > updater.MaxWorkSizeLimit {
		errs = append(errs, fmt.Errorf("update.max-work-size must be between 1 and %d", updater.MaxWorkSizeLimit))
	}
	if c.Management.Address == "" {
		errs = append(errs, errors.New("management.address is required"))
	}
	if u := c.Update; u.Mount == "" || u.Binary == "" || u.State == "" || u.Threads < 1 || u.MaxSubtrees < 0 {
		errs = append(errs, errors.New("update.mount, update.binary and update.state are required, update.threads must be at least 1, update.max-subtrees not negative"))
	}
	if err := updater.ValidEvery(c.Update.Every); err != nil {
		errs = append(errs, fmt.Errorf("update.every: %w", err))
	}
	return errors.Join(errs...)
}
