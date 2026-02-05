package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/spf13/pflag"
	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/registry"
	ctl "github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/config"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/job"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/server"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/workermgr"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
)

const (
	envVarPrefix = "BEEREMOTE_"
	// Note the concept of a BeeRemote nodeID will be used to support multiple BeeRemote nodes in the future.
	nodeID            = "0"
	FeatureLicenseStr = "io.beegfs.rst"
)

// Set by the build process using ldflags.
var (
	binaryName = "unknown"
	version    = "unknown"
	commit     = "unknown"
	buildTime  = "unknown"
)

var capabilities = map[string]*flex.Feature{
	registry.FeatureFilterFiles: nil,
}

func main() {
	// All application configuration (AppConfig) can be set using flags. The
	// default values specified here will be used as configuration defaults.
	// Note defaults for configuration specified using a slice are not set here.
	// Notably remote storage target defaults are handled as part of
	// initializing a particular RST type.
	pflag.Bool("version", false, "Print the version then exit.")
	pflag.String("cfg-file", "", "The path to the a configuration file (can be omitted to set all configuration using flags and/or environment variables). When Remote Storage Targets are configured using a file, they can be updated without restarting the application.")
	pflag.String("mount-point", "", "The path where BeeGFS is mounted.")
	pflag.String("log.type", "stderr", "Where log messages should be sent ('stderr', 'stdout', 'syslog', 'logfile').")
	pflag.String("log.file", "/var/log/beegfs/beegfs-remote.log", "The path to the desired log file when logType is 'log.file' (if needed the directory and all parent directories will be created).")
	pflag.Int8("log.level", 3, "Adjust the logging level (0=Fatal, 1=Error, 2=Warn, 3=Info, 4+5=Debug).")
	pflag.Int("log.max-size", 1000, "When log.type is 'logfile' the maximum size of the log.file in megabytes before it is rotated.")
	pflag.Int("log.num-rotated-files", 5, "When log.type is 'logfile' the maximum number old log.file(s) to keep when log.max-size is reached and the log is rotated.")
	pflag.Bool("log.developer", false, "Enable developer logging including stack traces and setting the equivalent of log.level=5 and log.type=stdout (all other log settings are ignored).")
	pflag.String("management.address", "127.0.0.1:8010", "The hostname:port of the BeeGFS management node.")
	pflag.String("management.tls-cert-file", "/etc/beegfs/cert.pem", "Use the specified certificate to verify and encrypt gRPC traffic to the Management node. Leave empty to only use the system's default certificate pool.")
	pflag.Bool("management.tls-disable-verification", false, "Disable TLS verification for gRPC communication to the Management node.")
	pflag.Bool("management.tls-disable", false, "Disable TLS entirely for gRPC communication to the Management node.")
	pflag.String("management.auth-file", "/etc/beegfs/conn.auth", "The file containing the shared secret for BeeGFS connection authentication.")
	pflag.Bool("management.auth-disable", false, "Disable BeeGFS connection authentication (not recommended).")
	pflag.String("server.address", "0.0.0.0:9010", "The hostname:port where this Remote node should listen for job requests from the BeeGFS CTL tool.")
	pflag.String("server.tls-cert-file", "/etc/beegfs/cert.pem", "Path to a certificate file that provides the identify of this Remote node's gRPC server.")
	pflag.String("server.tls-key-file", "/etc/beegfs/key.pem", "Path to the key file belonging to the certificate for this Remote node's gRPC server.")
	pflag.Bool("server.tls-disable", false, "Disable TLS entirely for gRPC communication to this Remote node's gRPC server.")
	pflag.String("job.path-db", "/var/lib/beegfs/remote/path.badger", "Path where the database tracking jobs for each path will be created/maintained.")
	pflag.Int("job.request-queue-depth", 1024, "Number of requests that can be made to JobMgr before new requests are blocked.")
	pflag.Int("job.min-job-entries-per-rst", 2, "This many jobs for each RST configured for a particular path is guaranteed to be retained. At minimum this should be set to 1 so we always know the last sync result for an RST.")
	pflag.Int("job.max-job-entries-per-rst", 4, "Once this threshold is exceeded, older jobs will be deleted (oldest-to-newest) until the number of jobs equals the min-job-entries-per-rst.")
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
	Except for Remote Storage Targets and workers, configuration may be set using a mix of flags, environment variables, and values from a TOML configuration file. 
	Configuration will be merged using the following precedence order (highest->lowest): (1) flags (2) environment variables (3) configuration file (4) defaults.
	Remote Storage Targets and workers can only be configured using a TOML config file.
Using environment variables:
	To specify configuration using environment variables specify %sKEY=VALUE where KEY is the flag name you want to specify in all capitals replacing dots (.) with a double underscore (__) and hyphens (-) with an underscore (_).
	Examples: 
	export %sLOG__DEBUG=true
`
		// TODO (https://github.com/ThinkParQ/bee-remote/issues/29): Consider if RSTs should ever be
		// configurable using flags and environment variables. If we want to allow them to be
		// configured using CTL this may never make sense, and better to just store them in a
		// DB/similar and not even allow them to be configured using a config file.
		fmt.Fprintf(os.Stderr, helpText, envVarPrefix, envVarPrefix)
		os.Exit(0)
	}
	pflag.Parse()

	if printVersion, _ := pflag.CommandLine.GetBool("version"); printVersion {
		fmt.Printf("%s %s (commit: %s, built: %s)\n", binaryName, version, commit, buildTime)
		os.Exit(0)
	}

	// We initialize ConfigManager first because all components require the initial config to start up.
	cfgMgr, err := configmgr.New(pflag.CommandLine, envVarPrefix, &config.AppConfig{}, config.SetRSTTypeHook())
	if err != nil {
		log.Fatalf("unable to get initial configuration: %s", err)
	}
	c := cfgMgr.Get()
	initialCfg, ok := c.(*config.AppConfig)
	if !ok {
		log.Fatalf("configuration manager returned invalid configuration (expected BeeRemote application configuration)")
	}
	if initialCfg.Developer.DumpConfig {
		fmt.Printf("Dumping AppConfig and exiting...\n\n")
		fmt.Printf("%+v\n", initialCfg)
		fmt.Println(`
		WARNING: Configuration listed here for individual Remote Storage Targets may not reflect their final configuration.
		Individual RST types may define their own custom defaults, or automatically override invalid user configuration.
		`)
		os.Exit(0)
	}

	if initialCfg.Developer.PerfProfilingPort != 0 {
		go func() {
			http.ListenAndServe(fmt.Sprintf(":%d", initialCfg.Developer.PerfProfilingPort), nil)
		}()
	}

	logger, err := logger.New(initialCfg.Log)
	if err != nil {
		log.Fatalf("unable to initialize logger: %s", err)
	}
	defer logger.Sync()

	err = ctl.InitLoggerFromExternal(logger.With(zap.String("component", "ctl")))
	if err != nil {
		logger.Fatal("unable to initialize ctl logging", zap.Error(err))
	}

	err = ctl.InitViperFromExternal(
		ctl.GlobalConfig{
			MgmtdAddress:                initialCfg.Management.Address,
			MgmtdTLSCertFile:            initialCfg.Management.TLSCertFile,
			MgmtdTLSDisableVerification: initialCfg.Management.TLSDisableVerification,
			MgmtdTLSDisable:             initialCfg.Management.TLSDisable,
			MgmtdUseProxy:               initialCfg.Management.UseProxy,
			AuthFile:                    initialCfg.Management.AuthFile,
			AuthDisable:                 initialCfg.Management.AuthDisable,
			RemoteAddress:               initialCfg.Server.Address,
			NumWorkers:                  runtime.GOMAXPROCS(0),
			ConnTimeoutMs:               500,
		},
	)
	if err != nil {
		logger.Fatal("unable to initialize ctl library", zap.Error(err))
	}

	logger.Info("<=== BeeRemote Initialized ===>")
	logger.Info("start-of-day", zap.String("application", binaryName), zap.String("version", version), zap.String("commit", commit), zap.String("built", buildTime))
	// Initialize and fetch the global CTL mount point. This ensures ctl.BeeGFSClient() can be used
	// with absolute or relative paths from anywhere in the application. We pass the mountPoint
	// directly to some components for legacy reasons (this can be refactored later if needed).
	logger.Info("checking BeeGFS mount point")
	// If we hang here, probably BeeGFS itself is not reachable.
	mountPoint, err := ctl.BeeGFSClient(initialCfg.MountPoint)
	if err != nil {
		logger.Fatal("unable to access BeeGFS mount point", zap.Error(err))
	}

	// Create a channel to receive OS signals to coordinate graceful shutdown:
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	// This context signals the application has received one of the above signals. It is used by the
	// main goroutine to understand when it should start a coordinated/ordered graceful shutdown of
	// all components. Generally it should not be used by individual components to coordinate their
	// shutdown, except if they require a context as part of their setup method to request
	// cancellation of any goroutines that may otherwise block shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sigs
		cancel()
	}()

	// Sanity check mount point config. This was only added in 8.1 and we want Remote to be
	// backwards compatible, so it is only enforced if the parameter exists and is set incorrectly.
	if procFsClients, err := procfs.GetBeeGFSClients(ctx, procfs.GetBeeGFSClientsConfig{FilterByMounts: []string{mountPoint.GetMountPath()}}, logger); err != nil {
		logger.Warn("unable to verify BeeGFS client configuration: unable to fetch configuration from /proc/fs/beegfs (ignoring)", zap.Error(err))
	} else if len(procFsClients) != 1 {
		logger.Warn("unable to verify BeeGFS client configuration: expected exactly one entry in /proc/fs/beegfs for this mount point (ignoring)", zap.String("mountPoint", mountPoint.GetMountPath()))
	} else {
		res, ok := procFsClients[0].Config["sysBypassFileAccessCheckOnMeta"]
		if ok {
			if res != "1" {
				logger.Fatal("invalid BeeGFS client configuration found: the mount point used by remote must be configured with 'sysBypassFileAccessCheckOnMeta = true' (no other applications should use this mount point)")
			}
		} else {
			logger.Warn("the BeeGFS client version does not appear to support file locking: parameter 'sysBypassFileAccessCheckOnMeta' was not found in the client configuration (ignoring)")
		}
	}

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
			logger.Info("downloaded license from BeeGFS management node", licenseDetails...)
		}
		if err != nil {
			logger.Fatal("unable to verify license", zap.Error(err))
		}
	}

	beeRemoteNode := flex.BeeRemoteNode_builder{
		Id:                          nodeID,
		Address:                     initialCfg.Server.Address,
		MgmtdAddress:                initialCfg.Management.Address,
		MgmtdTlsCert:                cert,
		MgmtdTlsDisableVerification: initialCfg.Management.TLSDisableVerification,
		MgmtdTlsDisable:             initialCfg.Management.TLSDisable,
		MgmtdUseProxy:               initialCfg.Management.UseProxy,
		AuthSecret:                  authSecret,
		AuthDisable:                 initialCfg.Management.AuthDisable,
	}.Build()

	workerManager, err := workermgr.NewManager(ctx, logger.Logger, initialCfg.WorkerMgr, initialCfg.Workers, initialCfg.RemoteStorageTargets, beeRemoteNode, mountPoint, capabilities)
	if err != nil {
		logger.Fatal("unable to initialize worker manager", zap.Error(err))
	}

	err = workerManager.Start()
	if err != nil {
		logger.Fatal("unable to start worker manager", zap.Error(err))
	}

	jobManager := job.NewManager(logger.Logger, initialCfg.Job, workerManager)
	err = jobManager.Start()
	if err != nil {
		logger.Fatal("unable to start job manager", zap.Error(err))
	}

	buildInfo := &flex.BuildInfo{BinaryName: binaryName, Version: version, Commit: commit, BuildTime: buildTime}
	jobServer, err := server.New(logger.Logger, initialCfg.Server, jobManager, buildInfo, capabilities)
	if err != nil {
		logger.Fatal("failed to initialize Remote gRPC server", zap.Error(err))
	}

	// Most components should not shutdown unexpectedly once they are started, but anything that
	// might should return an error on this channel signalling the application to shutdown. Increase
	// the channel size as needed if other components may also use this channel to log errors.
	errChan := make(chan error, 2)
	jobServer.ListenAndServe(errChan)

	// Block and wait for a shutdown signal:
	select {
	case err := <-errChan:
		logger.Error("component terminated unexpectedly", zap.Error(err))
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}
	jobServer.Stop()
	jobManager.Stop()
	workerManager.Stop()

	logger.Info("shutdown all components, exiting")
}
