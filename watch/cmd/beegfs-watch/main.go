package main

import (
	"context"

	"github.com/spf13/pflag"

	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/watch/internal/config"
	"github.com/thinkparq/beegfs-go/watch/internal/metadata"
	"github.com/thinkparq/beegfs-go/watch/internal/subscribermgr"
	"go.uber.org/zap"
)

// Set by the build process using ldflags.
var (
	binaryName = "unknown"
	version    = "unknown"
	commit     = "unknown"
	buildTime  = "unknown"
)

const (
	envVarPrefix      = "BEEWATCH_"
	FeatureLicenseStr = "io.beegfs.watch"
)

func main() {

	// The default values specified here will be used as configuration defaults. Note defaults for
	// configuration specified using a slice are not set here. Notably subscriber defaults are
	// handled as part of initializing a particular subscriber type.
	pflag.Bool("version", false, "Print the version then exit.")
	pflag.String("cfg-file", "", "The path to the configuration file containing the list of subscribers and other configuration.")
	pflag.String("log.type", "stderr", "Where log messages should be sent ('stderr', 'stdout', 'syslog', 'logfile').")
	pflag.String("log.file", "/var/log/beegfs/beegfs-watch.log", "The path to the desired log file when logType is 'logfile' (if needed the directory and all parent directories will be created).")
	pflag.Int8("log.level", 3, "Adjust the logging level (0=Fatal, 1=Error, 2=Warn, 3=Info, 4+5=Debug).")
	pflag.Int("log.max-size", 1000, "When log.type is 'logfile' the maximum size of the log.file in megabytes before it is rotated.")
	pflag.Int("log.num-rotated-files", 5, "When log.type is 'logfile' the maximum number old log.file(s) to keep when log.max-size is reached and the log is rotated.")
	pflag.Bool("log.incoming-event-rate", false, "Output the rate of incoming events per second.")
	pflag.Bool("log.developer", false, "Enable developer logging including stack traces and setting the equivalent of log.level=5 and log.type=stdout (all other log settings are ignored).")
	pflag.String("management.address", "127.0.0.1:8010", "The hostname:port of the BeeGFS management service.")
	pflag.String("management.tls-cert-file", "/etc/beegfs/cert.pem", "Use the specified certificate to verify and encrypt gRPC traffic to the Management node. Leave empty to only use the system's default certificate pool.")
	pflag.Bool("management.tls-disable-verification", false, "Disable TLS verification for gRPC communication to the Management node.")
	pflag.Bool("management.tls-disable", false, "Disable TLS entirely for gRPC communication to the Management node.")
	pflag.String("management.auth-file", "/etc/beegfs/conn.auth", "The file containing the connection authentication shared secret.")
	pflag.Bool("management.auth-disable", false, "Disable connection authentication.")
	pflag.Int("handler.max-reconnect-backoff", 60, "When a connection cannot be made to a subscriber subscriber reconnection attempts will be made with an exponential backoff. This is the maximum time in seconds between reconnection attempts to avoid increasing the backoff timer forever.")
	pflag.Int("handler.max-wait-for-response-after-connect", 2, "When a subscriber connects/reconnects wait this long for the subscriber to acknowledge the sequence ID of the last event it received successfully. This prevents sending duplicate events if the connection was disrupted unexpectedly.")
	pflag.Int("handler.poll-frequency", 1, "How often subscribers should poll the metadata buffer for new events (causes more CPU utilization when idle).")
	// Hidden flags:
	pflag.Bool("management.use-http-proxy", false, "Use proxy configured globally or in the environment for gRPC communication to the Management node.")
	pflag.CommandLine.MarkHidden("management.use-http-proxy")
	pflag.Int("developer.perf-profiling-port", 0, "Specify a port where performance profiles will be made available on the localhost via pprof (0 disables performance profiling).")
	pflag.CommandLine.MarkHidden("developer.perf-profiling-port")
	pflag.Bool("developer.dump-config", false, "Dump the full configuration and immediately exit.")
	pflag.CommandLine.MarkHidden("developer.dump-config")

	pflag.CommandLine.SortFlags = false
	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		pflag.PrintDefaults()
		helpText := `
Further info:
	Except for subscribers, configuration may be set using a mix of flags, environment variables, and values from a TOML configuration file. 
	Configuration will be merged using the following precedence order (highest->lowest): (1) flags (2) environment variables (3) configuration file (4) defaults.
	Subscribers can only be specified using a configuration file and can be updated without a restart by sending a hangup signal (SIGHUP).
Using environment variables:
	To specify configuration using environment variables specify %sKEY=VALUE where KEY is the flag name you want to specify in all capitals replacing dots (.) with double underscores (__) and hyphens (-) with an underscore (_).
	Examples: 
	export %sLOG__DEVELOPER=true
	export %sCONFIG_FILE=/etc/beegfs/beegfs-watch.toml
`
		fmt.Fprintf(os.Stderr, helpText, envVarPrefix, envVarPrefix, envVarPrefix)
		os.Exit(0)
	}

	pflag.Parse()

	if printVersion, _ := pflag.CommandLine.GetBool("version"); printVersion {
		fmt.Printf("%s %s (commit: %s, built: %s)\n", binaryName, version, commit, buildTime)
		os.Exit(0)
	}

	// We initialize ConfigManager first because all components require the initial config to start up.
	cfgMgr, err := configmgr.New(pflag.CommandLine, envVarPrefix, &config.AppConfig{})
	if err != nil {
		log.Fatalf("unable to get initial configuration: %s", err)
	}
	c := cfgMgr.Get()
	initialCfg, ok := c.(*config.AppConfig)
	if !ok {
		log.Fatalf("configuration manager returned invalid configuration (expected BeeWatch application configuration)")
	}

	if initialCfg.Developer.DumpConfig {
		fmt.Printf("Dumping AppConfig and exiting...\n\n")
		fmt.Printf("%+v\n", initialCfg)
		fmt.Println(`
		WARNING: Configuration listed here for individual subscribers may not reflect their final configuration.
		Individual subscriber types may define their own custom defaults, or automatically override invalid user configuration.
		`)
		os.Exit(0)
	}

	logger, err := logger.New(initialCfg.Log)
	if err != nil {
		log.Fatalf("Unable to initialize logger: %s", err)
	}
	defer logger.Sync() // Flush any final messages before exiting.
	logger.Info("<=== #### ===>")
	logger.Info("start-of-day", zap.String("application", binaryName), zap.String("version", version), zap.String("commit", commit), zap.String("built", buildTime))
	cfgMgr.AddListener(logger)

	if initialCfg.Developer.PerfProfilingPort != 0 {
		go func() {
			http.ListenAndServe(fmt.Sprintf(":%d", initialCfg.Developer.PerfProfilingPort), nil)
		}()
	}

	// Create a channel to receive OS signals to coordinate graceful shutdown:
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	// General context used to signal application shutdown was requested. Components should not use
	// this context unless they do not have ordering constraints for how they are stopped.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sigs
		cancel()
	}()

	// The mgmtd gRPC client expects the cert and auth file to already be read from their respective
	// sources (files in this case) and provided as a byte slice.
	var cert []byte
	if !initialCfg.Management.TLSDisable && initialCfg.Management.TLSCertFile != "" {
		cert, err = os.ReadFile(initialCfg.Management.TLSCertFile)
		if err != nil {
			logger.Fatal("unable to read management TLS certificate", zap.Error(err))
		}
	}
	var authSecret []byte
	if !initialCfg.Management.AuthDisable {
		authSecret, err = os.ReadFile(initialCfg.Management.AuthFile)
		if err != nil {
			logger.Fatal("unable to read management secret from auth file", zap.Error(err))
		}
	}
	// The only thing currently requiring a mgmtd client is license verification. Immediately
	// disconnect client after determining the license status to discourage reuse without first
	// evaluating if a more robust strategy to manage long-term connections to the mgmtd is required
	// for future use cases (such as downloading configuration from mgmtd).
	if mgmtdClient, err := beegrpc.NewMgmtd(
		initialCfg.Management.Address,
		beegrpc.WithTLSDisable(initialCfg.Management.TLSDisable),
		beegrpc.WithTLSDisableVerification(initialCfg.Management.TLSDisableVerification),
		beegrpc.WithTLSCaCert(cert),
		beegrpc.WithAuthSecret(authSecret),
		beegrpc.WithProxy(initialCfg.Management.UseProxy),
	); err != nil {
		// If the mgmtd is actually offline, usually we'll never get to this point. Startup will
		// hang earlier trying to determine the BeeGFS mount point. This is why we don't do any
		// extra retry logic here, the client will already try to reconnect and return an error
		// after some time. If we do get to this point it is more likely there is a configuration
		// error (for example the wrong mgmtd is configured on BeeRemote). The exception is if the
		// mgmtd just went offline and the client hasn't yet set targets probably-offline. But in
		// that scenario its probably better to refuse to startup prompting investigation.
		logger.Fatal("unable to initialize BeeGFS mgmtd client", zap.Error(err))
	} else {
		licenseDetails, err := mgmtdClient.VerifyLicense(ctx, FeatureLicenseStr)
		mgmtdClient.Cleanup()
		if licenseDetails != nil {
			logger.Info("downloaded license from BeeGFS management service", licenseDetails...)
		}
		if err != nil {
			logger.Fatal("unable to verify license", zap.Error(err))
		}
	}

	// We'll use a wait group to coordinate shutdown of all components including
	// verifying individual subscribers are disconnected:
	var wg sync.WaitGroup

	// Setup the Metadata manager:
	metaCtx, metaCancel := context.WithCancel(context.Background())
	defer metaCancel()

	metaMgr, metaCleanup, err := metadata.New(metaCtx, logger.Logger, initialCfg.Metadata)
	if err != nil {
		logger.Fatal("failed to start metadata manager", zap.Error(err))
	}
	defer metaCleanup()

	// Setup the subscriber manager:
	sm := subscribermgr.New(logger.Logger, metaMgr.EventBuffer, &wg)

	// Using a different context for subscribers is important so we can coordinate shutdown.
	// Notably we want to wait to disconnect until the buffer is empty.
	subscribersCtx, subscribersCancel := context.WithCancel(context.Background())
	defer subscribersCancel()
	go sm.Manage(subscribersCtx, &wg)
	wg.Add(1)
	cfgMgr.AddListener(sm)

	// We do this last so we don't start reading events from the metadata server until we're sure everything is ready.
	// If for some reason the subscriber configuration is bad it can be updated without restarting the app and potentially dropping events.
	go metaMgr.Manage(&wg)
	wg.Add(1)

	if initialCfg.Log.IncomingEventRate {
		go metaMgr.Sample() // We don't care about adding this to the wg. It'll just stop when the meta service is cancelled.
	}

	// Start accepting dynamic configuration updates:
	configCtx, configCancel := context.WithCancel(context.Background())
	go cfgMgr.Manage(configCtx, logger.Logger)

	<-ctx.Done() // Block here and wait for a signal to shutdown.
	logger.Info("shutdown signal received, no longer accepting events from the metadata service and waiting for outstanding events to be sent to subscribers before exiting (send another SIGINT or SIGTERM to shutdown immediately)")
	metaCancel() // When shutting down first stop adding events to the metadata buffer.

shutdownLoop:
	for {
		select {
		case <-sigs:
			// If we get another signal we should shutdown immediately.
			logger.Warn("attempting to disconnect all subscribers and shutdown down immediately, outstanding events in the buffer may be lost")
			subscribersCancel()
			break shutdownLoop
		case <-time.After(1 * time.Second):
			// Otherwise wait for subscribers to send all events and the buffer to be empty:
			if metaMgr.EventBuffer.AllEventsAcknowledged() {
				subscribersCancel()
				break shutdownLoop
			}
		}
	}

	configCancel() // Stop accepting configuration updates.
	// Wait for subscribers to disconnect and all components to stop.
	// We will wait here indefinitely even if we get another signal so we can try and cleanup resources.
	// SIGKILL would be needed at this point if something gets blocked shutting down.
	wg.Wait()
	logger.Info("all components stopped, exiting")
}
