package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/pflag"
	"github.com/thinkparq/beegfs-go/agent/internal/config"
	"github.com/thinkparq/beegfs-go/agent/internal/server"
	"github.com/thinkparq/beegfs-go/agent/pkg/reconciler"
	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/logger"
	"go.uber.org/zap"
)

const (
	envVarPrefix = "BEEAGENT_"
)

// Set by the build process using ldflags.
var (
	binaryName = "unknown"
	version    = "unknown"
	commit     = "unknown"
	buildTime  = "unknown"
)

func main() {
	pflag.Bool("version", false, "Print the version then exit.")
	pflag.String("cfg-file", "/etc/beegfs/agent.toml", "The path to the a configuration file (can be omitted to set all configuration using flags and/or environment variables). When Remote Storage Targets are configured using a file, they can be updated without restarting the application.")
	pflag.String("agent-id", "0", "A unique ID used to identify what nodes from the manifest this agent is responsible for. Should not change after initially starting the agent.")
	pflag.String("log.type", "stderr", "Where log messages should be sent ('stderr', 'stdout', 'syslog', 'logfile').")
	pflag.String("log.file", "/var/log/beegfs/beegfs-remote.log", "The path to the desired log file when logType is 'log.file' (if needed the directory and all parent directories will be created).")
	pflag.Int8("log.level", 3, "Adjust the logging level (0=Fatal, 1=Error, 2=Warn, 3=Info, 4+5=Debug).")
	pflag.Int("log.max-size", 1000, "When log.type is 'logfile' the maximum size of the log.file in megabytes before it is rotated.")
	pflag.Int("log.num-rotated-files", 5, "When log.type is 'logfile' the maximum number old log.file(s) to keep when log.max-size is reached and the log is rotated.")
	pflag.Bool("log.developer", false, "Enable developer logging including stack traces and setting the equivalent of log.level=5 and log.type=stdout (all other log settings are ignored).")
	pflag.String("server.address", "0.0.0.0:9008", "The hostname:port where this Agent should listen for requests from the BeeGFS CTL tool.")
	pflag.String("server.tls-cert-file", "/etc/beegfs/cert.pem", "Path to a certificate file that provides the identify of this Agent's gRPC server.")
	pflag.String("server.tls-key-file", "/etc/beegfs/key.pem", "Path to the key file belonging to the certificate for this Agent's gRPC server.")
	pflag.Bool("server.tls-disable", false, "Disable TLS entirely for gRPC communication to this Agent's gRPC server.")
	pflag.String("reconciler.manifest-path", "/etc/beegfs/manifest.yaml", "The path to the BeeGFS manifest this agent should apply. The manifest will be identical to the active manifest if applied successfully.")
	pflag.String("reconciler.active-manifest-path", "/etc/beegfs/.active.manifest.yaml", "The past to the last BeeGFS manifest successfully applied by this agent.")
	pflag.String("reconciler.deployment-strategy", "default", "The deployment strategy used by the reconciler.")
	pflag.Bool("developer.dump-config", false, "Dump the full configuration and immediately exit.")
	pflag.CommandLine.MarkHidden("developer.dump-config")
	pflag.CommandLine.SortFlags = false
	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		pflag.PrintDefaults()
		helpText := `
Further info:
	Configuration may be set using a mix of flags, environment variables, and values from a TOML configuration file. 
	Configuration will be merged using the following precedence order (highest->lowest): (1) flags (2) environment variables (3) configuration file (4) defaults.
Using environment variables:
	To specify configuration using environment variables specify %sKEY=VALUE where KEY is the flag name you want to specify in all capitals replacing dots (.) with a double underscore (__) and hyphens (-) with an underscore (_).
	Examples: 
	export %sLOG__DEBUG=true
`
		fmt.Fprintf(os.Stderr, helpText, envVarPrefix, envVarPrefix)
		os.Exit(0)
	}
	pflag.Parse()

	if printVersion, _ := pflag.CommandLine.GetBool("version"); printVersion {
		fmt.Printf("%s %s (commit: %s, built: %s)\n", binaryName, version, commit, buildTime)
		os.Exit(0)
	}

	cfgMgr, err := configmgr.New(pflag.CommandLine, envVarPrefix, &config.AppConfig{})
	if err != nil {
		log.Fatalf("unable to get initial configuration: %s", err)
	}
	c := cfgMgr.Get()
	initialCfg, ok := c.(*config.AppConfig)
	if !ok {
		log.Fatalf("configuration manager returned invalid configuration (expected Agent application configuration)")
	}
	if initialCfg.Developer.DumpConfig {
		fmt.Printf("Dumping AppConfig and exiting...\n\n")
		fmt.Printf("%+v\n", initialCfg)
		os.Exit(0)
	}

	logger, err := logger.New(initialCfg.Log)
	if err != nil {
		log.Fatalf("unable to initialize logger: %s", err)
	}
	defer logger.Sync()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	reconciler, err := reconciler.New(ctx, initialCfg.AgentID, logger.Logger, initialCfg.Reconciler)
	if err != nil {
		logger.Fatal("unable to initialize reconciler", zap.Error(err))
	}
	cfgMgr.AddListener(reconciler)
	agentServer, err := server.New(logger.Logger, initialCfg.Server, reconciler)
	if err != nil {
		logger.Fatal("unable to initialize gRPC server", zap.Error(err))
	}

	errChan := make(chan error, 2)
	agentServer.ListenAndServe(errChan)
	go cfgMgr.Manage(ctx, logger.Logger)

	select {
	case err := <-errChan:
		logger.Error("component terminated unexpectedly", zap.Error(err))
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}
	cancel()
	agentServer.Stop()
	if err := reconciler.Stop(); err != nil {
		logger.Error("error stopping reconciler", zap.Error(err))
	}
	logger.Info("shutdown all components, exiting")
}
