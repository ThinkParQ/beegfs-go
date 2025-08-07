package config

import (
	"fmt"
	"reflect"

	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/types"
	"github.com/thinkparq/beegfs-go/watch/internal/metadata"
	"github.com/thinkparq/beegfs-go/watch/internal/subscriber"
	"github.com/thinkparq/beegfs-go/watch/internal/subscribermgr"
)

// We use ConfigManager to handle configuration updates.
// Verify all interfaces that depend on AppConfig are satisfied.
var _ configmgr.Configurable = &AppConfig{}
var _ logger.Configurer = &AppConfig{}
var _ subscribermgr.Configurer = &AppConfig{}

// AppConfig defines all configuration supported by all application components.
// IMPORTANT: When updating/refactoring AppConfig these changes need to be
// manually applied to the pflags defined in main.go.
type AppConfig struct {
	Log         logger.Config               `mapstructure:"log"`
	Management  MgmtdConfig                 `mapstructure:"management"`
	Handler     subscribermgr.HandlerConfig `mapstructure:"handler"`
	Metadata    []metadata.Config           `mapstructure:"metadata"`
	Subscribers []subscriber.Config         `mapstructure:"subscriber"`
	Developer   struct {
		PerfProfilingPort int  `mapstructure:"perf-profiling-port"`
		DumpConfig        bool `mapstructure:"dump-config"`
	}
}

type MgmtdConfig struct {
	Address                string `mapstructure:"address"`
	TLSCertFile            string `mapstructure:"tls-cert-file"`
	TLSDisableVerification bool   `mapstructure:"tls-disable-verification"`
	TLSDisable             bool   `mapstructure:"tls-disable"`
	UseProxy               bool   `mapstructure:"use-http-proxy"`
	AuthFile               string `mapstructure:"auth-file"`
	AuthDisable            bool   `mapstructure:"auth-disable"`
}

// The GetXConfig() functions are used by components that support dynamic configuration updates.
// They allow us to avoid cyclical imports of AppConfig into the respective component packages.

// GetLoggingConfig returns only the part of an AppConfig expected by the logger.
func (c *AppConfig) GetLoggingConfig() logger.Config {
	return c.Log
}

// GetSMConfig returns only the part of an AppConfig expected by Subscriber Manager.
func (c *AppConfig) GetSMConfig() (subscribermgr.HandlerConfig, []subscriber.Config) {
	return c.Handler, c.Subscribers
}

// NewEmptyInstance() returns an empty AppConfig for ConfigManager to use with
// when unmarshalling the configuration.
func (c *AppConfig) NewEmptyInstance() configmgr.Configurable {
	return new(AppConfig)
}

// UpdateAllowed() determines if the existing AppConfig c can be safely updated
// to the provided newConfig. It is required to implement the Configurable interface
// and is used to define any application specific rules around config updates.
func (c *AppConfig) UpdateAllowed(newConfig configmgr.Configurable) error {

	nc, ok := newConfig.(*AppConfig)
	if !ok {
		return fmt.Errorf("invalid configuration provided (expected BeeWatch application configuration)")
	}

	if nc.Developer != c.Developer {
		return fmt.Errorf("rejecting configuration update: unable to change developer configuration settings after startup (current settings: %+v | proposed settings: %+v)", c.Developer, nc.Developer)
	}
	if !reflect.DeepEqual(nc.Metadata, c.Metadata) {
		return fmt.Errorf("rejecting configuration update: unable to change metadata configuration settings after startup (current settings: %+v | proposed settings: %+v)", c.Metadata, nc.Metadata)
	}
	if nc.Log != c.Log {
		// Use reflection to iterate over the fields of the logging conflict
		// struct and ensure only fields we allowed to change do (currently
		// only the level).
		newConfigLog := reflect.ValueOf(nc.Log)
		currentConfigLog := reflect.ValueOf(c.Log)

		for i := 0; i < newConfigLog.NumField(); i++ {
			fieldName := newConfigLog.Type().Field(i).Name
			if fieldName != "Level" && newConfigLog.Field(i).Interface() != currentConfigLog.Field(i).Interface() {
				return fmt.Errorf("rejecting configuration update: unable to change logging configuration settings after startup (current settings: %+v | proposed settings: %+v)", c.Log, nc.Log)
			}
		}
	}

	return nil
}

// validateConfig checks we received sane configuration values. Any issues are
// returned as a MultiError specifying the problematic values. Note it only
// performs static checks, and will not (for example) catch if a file doesn't
// exist or we don't have permissions to access it.
func (c *AppConfig) ValidateConfig() error {

	// TODO: Consider moving validation checks into the respective packages where the config is defined.
	var multiErr types.MultiError

	// TODO (https://github.com/ThinkParQ/bee-watch/issues/24): Support multiple metadata services.
	if len(c.Metadata) != 1 {
		multiErr.Errors = append(multiErr.Errors, fmt.Errorf("exactly one metadata service must be specified"))
	} else {
		if c.Metadata[0].EventLogTarget == "" {
			multiErr.Errors = append(multiErr.Errors, fmt.Errorf("the event-log-target for this metadata service must be specified"))
		}
		if c.Metadata[0].EventBufferGCFrequency == 0 {
			multiErr.Errors = append(multiErr.Errors, fmt.Errorf("the event-buffer-gc-frequency for this metadata service cannot be 0"))
		}
		if c.Metadata[0].EventBufferSize == 0 {
			multiErr.Errors = append(multiErr.Errors, fmt.Errorf("the event-buffer-size for this metadata service cannot be 0"))
		}
	}

	if len(multiErr.Errors) > 0 {
		return &multiErr
	}
	return nil
}
