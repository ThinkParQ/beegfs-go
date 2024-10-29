// Package configmgr contains functionality for managing application configuration.
package configmgr

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"reflect"
	"strings"
	"sync"
	"syscall"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/v8/common/types"
	"go.uber.org/zap"
)

// Configurable defines an interface for managing application configurations.
// Implementing this interface allows the configuration for different
// applications to be managed using common configuration manager implementation
// for generic tasks like loading and updating configuration from multiple
// sources.
type Configurable interface {
	// NewEmptyInstance creates a new, zero-valued instance of the same type as
	// the receiver. It should not share any state with the original instance
	// and should be ready to be configured with new values. It will be used
	// with viper.Unmarshal to get the actual configuration used by the
	// application.
	NewEmptyInstance() Configurable
	// UpdateAllowed checks whether the proposed new configuration represented
	// by the input Configurable is allowed based on the current state.
	// Implementations should compare the new configuration with the existing
	// one and return an error if the update is not permitted.
	UpdateAllowed(Configurable) error
	// ValidateConfig validates the current configuration to ensure that it
	// meets all required criteria and constraints. It should return an error if
	// the configuration is not valid.
	ValidateConfig() error
}

// ConfigManager provides a generic solution for managing configuration for
// multiple applications. By implementing a common interface (Configurable),
// different types of application configuration can be handled in a uniform
// manner to provide reusability and consistency with how configuration is
// handled.
//
// ConfigManager handles loading configuration from multiple sources (flags,
// environment variables, and a config file), then validating the configuration.
// Configuration is initially set when New() is called. If the application
// should support dynamic configuration updates the application should also call
// the Manage() method to allow configuration to be updated whenever the
// application receives a signal hangup (SIGHUP).
//
// The latest configuration can be accessed using one-off calls to the Get()
// method, or by registering one or more Listener. All listeners are
// automatically notified whenever configuration updates happen.
type ConfigManager struct {
	// One or more flags used to configure the application.
	initialFlags *pflag.FlagSet
	// This prefix will be used to determine what environment variables
	// should be used to set this applications configuration.
	envVarPrefix string
	// Whenever the configuration is updated, these listeners will be
	// automatically informed without having to manually call Get().
	listeners []Listener
	// currentConfig stores the actual configuration used by the application.
	// ConfigMgr doesn't know the actual type, but instead is provided a type
	// that satisfies the Configurable interface so we're able to handle
	// configuration generically.
	currentConfig Configurable
	// ConfigManager uses updateSignal to efficiently listen for OS signals
	// that indicate the configuration needs to be updated.
	updateSignal chan os.Signal
	// updateInProgress is used to lock the configuration while an update is
	// in progress. Without this spamming SIGHUP requests may result in
	// unpredictable behavior.
	updateInProgress sync.RWMutex
	// After the initial configuration is set, the rules defined by
	// UpdateAllowed() will be enforced.
	initialCfgSet   bool
	decodeHookFuncs []mapstructure.DecodeHookFuncType
}

// New creates a new ConfigManager that is setup to parse configuration based on
// the provided flagset and environment variable prefix. If the flagset includes
// a cfg-file flag, configuration from that file will also be used. It does not
// know anything about the actual application configuration, accepting instead
// the Configurable interface which is used to unmarshal the provided
// configuration sources into the specific type that represents the applications
// configuration. It optionally accepts one or more custom decode hook functions
// that can be used to fully customize how Viper unmarshals the configuration
// into the provided Configurable. If it fails to initialize the configuration
// it immediately returns an error to prevent the app from starting with bad
// configuration.
func New(flags *pflag.FlagSet, envVarPrefix string, config Configurable, decodeHookFuncs ...mapstructure.DecodeHookFuncType) (*ConfigManager, error) {

	cfgMgr := &ConfigManager{
		initialFlags:    flags,
		envVarPrefix:    envVarPrefix,
		currentConfig:   config,
		updateSignal:    make(chan os.Signal, 1),
		initialCfgSet:   false,
		decodeHookFuncs: decodeHookFuncs,
	}

	err := cfgMgr.updateConfiguration()
	if err != nil {
		return nil, err
	}

	signal.Notify(cfgMgr.updateSignal, syscall.SIGHUP)

	return cfgMgr, nil
}

// Listener is a component (for example a certain package in the application)
// that supports dynamic configuration updates and can be added to ConfigManager
// using the AddListener function. For example the [gobee.logger] package is a listener.
type Listener interface {
	// UpdateConfiguration is used to provide a Configurable to a listener. We
	// use "any" here instead of "Configurable" to give applications flexibility
	// in how they want to handle the configuration. If the listener is unable
	// to update the configuration it should return a meaningful error to help
	// diagnose and correct the configuration then wait for new configuration to
	// be provided. The component SHOULD NOT rollback to the previous version of
	// the listener. The listener SHOULD avoid data loss, for example if a bad
	// list of subscribers was provided, don't delete all subscribers and drop
	// events.
	//
	// To handle the "any" interface, applications can use either type assertion
	// to get the full configuration, or choose to implement additional
	// interfaces individual component packages can use to get only the
	// configuration they care/know about.
	//
	// Generally the latter is recommended so the application can define one
	// AppConfig struct in a Config package that uses composition to assemble
	// the entire application configuration from individual component packages
	// that define their own configuration. This allows easily unmarshalling and
	// checking of the entire configuration while also promoting encapsulation.
	// This approach would not work if we required listeners to always work with
	// the whole AppConfig, because then they would also need to import the
	// AppConfig from their config package, causing a cyclical import. See the
	// [gobee.logging] package for an example of this in action.
	UpdateConfiguration(any) error
}

// AddListener adds a Listener to ConfigManager. Listeners should not be added
// until the component they represent has been initialized.
func (cm *ConfigManager) AddListener(listener Listener) {
	cm.listeners = append(cm.listeners, listener)
}

// UpdateListeners updates any registered Listeners whose configuration can be
// set dynamically. If anything goes wrong the Listener is expected to handle
// the issue gracefully. At the end we'll return a summary of any errors, but we
// won't rollback the configuration.
func (cm *ConfigManager) UpdateListeners() error {
	var multiErr types.MultiError
	for _, listener := range cm.listeners {
		err := listener.UpdateConfiguration(cm.currentConfig)
		if err != nil {
			multiErr.Errors = append(multiErr.Errors, err)
		}
	}

	if len(multiErr.Errors) > 0 {
		multiErr.Errors = append([]error{fmt.Errorf("WARNING: configuration partially updated")}, multiErr.Errors...)
		return &multiErr
	}
	return nil
}

// Get returns the current configuration. The caller can use it with a type
// assertion to access the actual configuration values.
func (cm *ConfigManager) Get() Configurable {
	cm.updateInProgress.RLock()
	defer cm.updateInProgress.RUnlock()
	return cm.currentConfig
}

// Manage listens for an update signal (SIGHUP) and attempts to dynamically
// update the configuration. It is commonly run as a goroutine and accepts a
// context that should be cancelled when it should shutdown. It also requires a
// logger because the logger cannot be set on the ConfigManager struct since it
// is also responsible for handling log configuration.
func (cm *ConfigManager) Manage(ctx context.Context, log *zap.Logger) {

	log = log.With(zap.String("component", path.Base(reflect.TypeOf(ConfigManager{}).PkgPath())))

	for {
		// When we first start make sure all the managers have the latest configuration.
		err := cm.updateConfiguration()
		if err != nil {
			log.Warn("one or more errors occurred updating the configuration", zap.Error(err))
		}

		select {
		case <-ctx.Done():
			log.Info("shutting down because the app is shutting down")
			return
		case <-cm.updateSignal:
			log.Debug("updating configuration")
			continue
		}
	}
}

// updateConfiguration combines the following configuration sources. The
// following precedence order is respected (1) command line flags, (2)
// environmental variables, (3) a configuration file, (4) default values.
// For all configuration except for anything provided as a slice (notably
// subscribers), defaults are specified as part of the pflag definitions
// in main.go. For subscribers, defaults are defined as part of the
// subscriber implementation in the `new<TYPE>subscriber()` method.
//
// It takes these configuration sources and unmarshals the resulting
// configuration into the provided Configurable. It then uses the
// updateAllowed() and validateConfig() implemented on a particular
// Configurable, to determine if the new configuration is valid. If these checks
// fail the current configuration set on ConfigMgr will not be updated, and an
// error returned to be handled by the caller. Otherwise it will set
// cm.currentConfig equal to the new configuration.
//
// If validation succeeds it will attempt to propagate the configuration update
// to other components of the app that support dynamic configuration updates. It
// does this by calling UpdateConfiguration() on all configured listeners. If
// there is an error dynamically updating the configuration for any component,
// the component is expected to return a meaningful error. Any error(s) will be
// aggregated and returned to the caller for handling.
func (cm *ConfigManager) updateConfiguration() error {

	cm.updateInProgress.Lock()
	defer cm.updateInProgress.Unlock()

	// We don't want to use Viper to actually persist the configuration.
	// We mainly use Viper to simplify merging the configuration together,
	// following a predictable precedence order.
	v := viper.New()
	v.SetConfigType("toml")

	// Viper's precedence order means flags have the highest priority.
	// This means any configuration set using a flag is immutable.
	// We also get all of our defaults based on the flag setup.
	err := v.BindPFlags(cm.initialFlags)
	if err != nil {
		return fmt.Errorf("rejecting configuration update: unable to parse command line flags: %s", err)
	}

	// Allow specifying subscribers using a --subscribers flag. We do this by
	// transforming the flags into TOML format. Then later on we can just use
	// viper.Unmarshal to get the corresponding Golang structs. This avoids
	// having to write a custom unmarshaller. Note the flag we use to specify
	// subscribers cannot be the same as what is in the TOML file (subscriber).
	// This is because we need a way to allow users to specify multiple
	// subscribers in a single string, that is different from the actual slice
	// of subscribers that is unmarshalled from the final configuration.
	subscribersFromFlags := v.GetString("subscribers")
	if len(subscribersFromFlags) > 0 {
		tomlString := parseTOMLSubscribersFromString(subscribersFromFlags)

		if err := v.ReadConfig(strings.NewReader(tomlString)); err != nil {
			return fmt.Errorf("rejecting configuration update: unable to parse subscribers from command line flags (check flag values are enclosed in \"double\" quotes (--flag=\"value\") and all strings within flag values contained in 'single' quotes (--flag=\"key='value'\"): %w", err)
		}
	}

	// While Viper allows you to override values it reads from a config file
	// with environment variables, it does not appear to provide a way to just
	// read in configuration from environment variables.  We don't want to
	// require use of a config file, so this workaround searches for environment
	// variables that match our prefix then uses BindEnv to manually link the
	// desired environment variables that will later be used when we
	// call viper.Unmarshal(). This approach was adapted from:
	// https://renehernandez.io/snippets/bind-environment-variables-to-config-struct-with-viper/.
	// Note an alternative approach would be to use viper.AutomaticEnv() then
	// Viper.Get() would reevaluate environment variables every time Get() is
	// called. However we want also ConfigManager to handle all configuration
	// updates so we can check configuration before it is reapplied. This is
	// partially why we don't use Viper to persist the configuration and
	// require going through ConfigMgr instead of using Get() throughout the app.
	subscribersFromEnv := false
	for _, envVar := range os.Environ() {
		pair := strings.SplitN(envVar, "=", 2)
		key := pair[0]
		val := pair[1]

		if strings.HasPrefix(key, cm.envVarPrefix) {
			viperKey := strings.ReplaceAll(strings.ToLower(strings.TrimPrefix(key, cm.envVarPrefix)), "__", ".")
			viperKey = strings.ReplaceAll(viperKey, "_", "-")

			if viperKey == "subscribers" {
				// We do not want to allow subscribers to be specified multiple
				// ways, so we first check if they were also specified using flags.
				if len(subscribersFromFlags) > 0 {
					return fmt.Errorf("rejecting configuration update: subscribers cannot be set using both flags and environment variables")
				}
				// Use the same approach as we do for flags to get a list of subscribers:
				subscribersFromEnv = true
				tomlString := parseTOMLSubscribersFromString(val)
				if err := v.ReadConfig(strings.NewReader(tomlString)); err != nil {
					return fmt.Errorf("rejecting configuration update: unable to parse subscribers from environment variable (check the value is contained in \"double\" quotes? and all strings within the value are contained in 'single' quotes like \"id=1,name='subscriber',type='grpc'\"): %w", err)
				}
			} else if viperKey == "subscriber" {
				return fmt.Errorf("rejecting configuration update: subscribers specified using environment variables should be specified using '%sSUBSCRIBERS=<LIST>' with one or more subscribers separated by a semicolon (the singular form '%sSUBSCRIBER' is not allowed)", cm.envVarPrefix, cm.envVarPrefix)
			} else {
				if err := v.BindEnv(viperKey, strings.ToUpper(key)); err != nil {
					return err
				}
			}
		}
	}

	// Important we do this last as a cfg-file could be set as a flag or
	// environment variable. We also want to allow configuring BeeWatch entirely
	// without a config file.
	if v.GetString("cfg-file") != "" {
		// First we read the config file into a separate Viper instance.
		// We mainly do this so we can check subscribers are only being set in one place.
		// Otherwise we'd have to define complicated precedence rules for merging subscribers.
		vFile := viper.New()
		vFile.SetConfigFile(v.GetString("cfg-file"))

		if err := vFile.ReadInConfig(); err != nil {
			if _, ok := err.(viper.ConfigFileNotFoundError); ok {
				return fmt.Errorf("rejecting configuration update: configuration file at '%s' was not found (check it exists and permissions are set correctly)", v.GetString("cfg-file"))
			} else if _, ok := err.(viper.ConfigParseError); ok {
				return fmt.Errorf("rejecting configuration update: error parsing configuration file at '%s': %w", v.GetString("cfg-file"), err)
			}
			return fmt.Errorf("rejecting configuration update: an unknown error occurred reading config file '%s' (check permissions): %w", v.GetString("cfg-file"), err)
		}
		subscribersFromFile := vFile.GetStringSlice("subscriber")

		if len(subscribersFromFile) > 0 && (len(subscribersFromFlags) > 0 || subscribersFromEnv) {
			return fmt.Errorf("rejecting configuration update: subscribers cannot be set using a mix of flags, environment variables, and a configuration file (only one is allowed)")
		}
		// If all our checks pass we'll actually use the config file for the combined Viper instance.
		v.SetConfigFile(v.GetString("cfg-file"))

		// Merge the configuration set via flags and environment variables with the cfg-file.
		if err := v.MergeInConfig(); err != nil {
			return fmt.Errorf("rejecting configuration update: an unknown error occurred merging configuration sources: %w", err)
		}
	}

	if v.GetBool("developer.dump-config") {
		fmt.Printf("Dumping final merged configuration from Viper: \n\n%s\n\n", v.AllSettings())
	}

	// Optionally handle decoding custom formats.
	// Adapted from: https://sagikazarmark.hu/blog/decoding-custom-formats-with-viper/.
	var decoderOpts []viper.DecoderConfigOption
	for _, hookFunc := range cm.decodeHookFuncs {
		decoderOpts = append(decoderOpts, viper.DecodeHook(hookFunc))
	}

	// Get everything out of our temporary Viper store and unmarshall it into a
	// new empty instance of our configurable using decoderOpts if provided.
	newConfig := cm.currentConfig.NewEmptyInstance()
	if err := v.Unmarshal(newConfig, decoderOpts...); err != nil {
		return fmt.Errorf("rejecting configuration update: unable to parse configuration (check if the configuration valid): %w", err)
	}

	if err = newConfig.ValidateConfig(); err != nil {
		return err
	}

	// After the initial configuration is set, some values are immutable. By
	// performing the configuration check here and not as part of
	// UpdateListeners(), we can reject the entire configuration update so is
	// not partially applied.
	if cm.initialCfgSet {
		if err := cm.currentConfig.UpdateAllowed(newConfig); err != nil {
			return err
		}
	} else {
		cm.initialCfgSet = true
	}

	// Apply the configuration and attempt to update listeners. At this point if
	// the listeners reject the configuration we cannot rollback. We rely on the
	// listeners to handle bad configuration in whatever way is most
	// appropriate.
	cm.currentConfig = newConfig
	return cm.UpdateListeners()
}

// parseTOMLSubscribersFromString accepts a string containing one or more
// subscribers separated by a semicolon. Each subscriber can have one or more
// key=value pairs defining its configuration. This list of subscriber(s) and
// their configuration is then converted into TOML format and returned.
// Generally it is used to easily unmarshal a list of subscribers where the
// unmarshaller expects one or more TOML configuration sources.
func parseTOMLSubscribersFromString(subscribers string) string {
	var tomlSubscribers []string
	for _, s := range strings.Split(subscribers, ";") {
		subscriber := strings.ReplaceAll(s, ",", "\n")
		tomlSubscribers = append(tomlSubscribers, "[[subscriber]]\n"+subscriber)
	}

	return strings.Join(tomlSubscribers, "\n")
}
