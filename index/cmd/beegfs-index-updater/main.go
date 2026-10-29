// Command beegfs-index-updater keeps a GUFI index in step with a BeeGFS file system: it receives
// events from BeeGFS Watch, turns them into suspect files and applies them to the index with
// gufi_incremental_update, on a schedule or when the beegfs CLI asks.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/logger"
	ctl "github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/index/internal/collector"
	"github.com/thinkparq/beegfs-go/index/internal/config"
	"github.com/thinkparq/beegfs-go/index/internal/markwalk"
	"github.com/thinkparq/beegfs-go/index/internal/updater"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const envVarPrefix = "BEEINDEXUPDATER_"

// licensedFeature is the feature the beegfs CLI verifies before any index command.
const licensedFeature = "io.beegfs.index"

// Set by the build process using ldflags.
var (
	binaryName = "unknown"
	version    = "unknown"
	commit     = "unknown"
	buildTime  = "unknown"
)

func main() {
	pflag.Bool("version", false, "Print the version then exit.")
	pflag.String("cfg-file", "", "The path to a configuration file (can be omitted to set all configuration using flags and/or environment variables).")
	pflag.String("log.type", "stderr", "Where log messages should be sent ('stderr', 'stdout', 'syslog', 'logfile').")
	pflag.String("log.file", "/var/log/beegfs/beegfs-index-updater.log", "The path to the desired log file when logType is 'logfile' (if needed the directory and all parent directories will be created).")
	pflag.Int8("log.level", 3, "Adjust the logging level (0=Fatal, 1=Error, 2=Warn, 3=Info, 4+5=Debug).")
	pflag.Int("log.max-size", 1000, "When log.type is 'logfile' the maximum size of the log.file in megabytes before it is rotated.")
	pflag.Int("log.num-rotated-files", 5, "When log.type is 'logfile' the maximum number old log.file(s) to keep when log.max-size is reached and the log is rotated.")
	pflag.Bool("log.developer", false, "Enable developer logging including stack traces and setting the equivalent of log.level=5 and log.type=stdout (all other log settings are ignored).")
	pflag.String("management.address", "127.0.0.1:8010", "The address (host:port) of the BeeGFS management service.")
	pflag.String("management.auth-file", "/etc/beegfs/conn.auth", "The file containing the shared secret for BeeGFS connection authentication.")
	pflag.Bool("management.auth-disable", false, "Disable BeeGFS connection authentication (not recommended).")
	pflag.String("management.tls-cert-file", "/etc/beegfs/cert.pem", "The certificate used to verify the management service.")
	pflag.Bool("management.tls-disable-verification", false, "Disable TLS verification for the management connection.")
	pflag.Bool("management.tls-disable", false, "Disable TLS for the management connection.")
	pflag.Bool("management.use-http-proxy", false, "Use the proxy from the environment for the management connection.")
	pflag.String("collector.listen", "0.0.0.0:50052", "The address Watch and the beegfs CLI connect to.")
	pflag.String("collector.checkpoint", "/var/lib/beegfs/index-updater/checkpoint.json", "The file where acknowledged sequence IDs are recorded.")
	pflag.Duration("collector.ack-frequency", time.Second, "How often acknowledgements are sent to Watch and written to the checkpoint.")
	pflag.Duration("collector.batch-every", 30*time.Second, "Close the open batch at least this often.")
	pflag.Int("collector.batch-events", 50_000, "Close the open batch once it holds this many events.")
	pflag.Int("collector.batch-queue", 2, "The number of closed batches that may wait for markwalk before receiving blocks.")
	pflag.Uint64("collector.high-water-mark", 2_097_152, "Ask for an update once one metadata server has this many unacknowledged events, because only an update acknowledges them. Keep it well below Watch's event-buffer-size; the default is half of Watch's default. 0 disables it.")
	pflag.String("collector.tls-cert-file", "/etc/beegfs/cert.pem", "The certificate that identifies this server to Watch and the CLI.")
	pflag.String("collector.tls-key-file", "/etc/beegfs/key.pem", "The key belonging to collector.tls-cert-file.")
	pflag.Bool("collector.tls-disable", false, "Disable TLS for the gRPC server Watch and the CLI connect to.")
	pflag.String("markwalk.work", "/var/lib/beegfs/index-updater/markwalk", "The directory each update's suspect file is written to, as suspects-<N>.")
	pflag.String("markwalk.index", "/mnt/index/beegfs", "The GUFI index root: the index directory that corresponds to the BeeGFS mount, <index root>/<mount name> as 'beegfs index create' lays it out. Its name must match update.mount.")
	pflag.String("markwalk.query", "/opt/beegfs/bin/index/gufi_query", "The gufi_query binary, which reads directory inodes from the index and finds the other names of a hard-linked file.")
	pflag.String("markwalk.sqlite", "/opt/beegfs/bin/index/gufi_sqlite3", "The gufi_sqlite3 binary, which writes a batch's hard-linked inodes to a database the hard-link lookup attaches.")
	pflag.Bool("markwalk.track-opens", false, "Include open events so atime changes are indexed.")
	pflag.Int64("markwalk.max-work-size", 1024, "The most disk space, in MiB, the work directory may hold. Once it does, batches wait and Watch keeps the events.")
	pflag.String("update.mount", "/mnt/beegfs", "The BeeGFS mount point the index describes. No update runs while it is not a mounted BeeGFS.")
	pflag.String("update.binary", "/opt/beegfs/bin/index/gufi_incremental_update", "The gufi_incremental_update binary.")
	pflag.String("update.treesummary", "/opt/beegfs/bin/index/gufi_treesummary", "The gufi_treesummary binary, run after a successful update. Empty skips it.")
	pflag.String("update.plugin", "beegfs_index_ops:/opt/beegfs/lib/libbeegfs_indexing.so", "The BeeGFS indexing plugin, passed to the update so rebuilt directories keep BeeGFS metadata.")
	pflag.Bool("update.no-beegfs-metadata", false, "Skip the BeeGFS plugin for every run, as 'beegfs index create -B' does for the index. 'beegfs index update -B' skips it for one run.")
	pflag.Int("update.threads", runtime.GOMAXPROCS(0), "The thread count handed to the GUFI binaries, gufi_query included. Defaults to the number of CPUs.")
	pflag.Bool("update.compress", false, "Pass --compress to gufi_incremental_update: compress work items.")
	pflag.Int("update.max-subtrees", 0, "Pass --max-subtrees to gufi_incremental_update: subtrees processed in parallel, which bounds open file descriptors. 0 leaves the tool's default.")
	pflag.String("update.keep-artifacts", "", "Pass --keep-artifacts to gufi_incremental_update: an existing directory to keep the run's artifacts in instead of deleting them.")
	pflag.Duration("update.every", 0, "Run an update at least this often: 0 disables the timer, otherwise at least 1m. 'beegfs index update --every' changes it until the service restarts.")
	pflag.String("update.state", "/var/lib/beegfs/index-updater/update.json", "The file that keeps the pending repair and the highest processed sequence ID per metadata node.")
	pflag.CommandLine.SortFlags = false
	pflag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		pflag.PrintDefaults()
		helpText := `
Further info:
	Configuration may be set using a mix of flags, environment variables, and values from a TOML configuration file.
	Configuration will be merged using the following precedence order (highest->lowest): (1) flags (2) environment variables (3) configuration file (4) defaults.
Using environment variables:
	To specify configuration using environment variables specify %sKEY=VALUE where KEY is the flag name you want to specify in all capitals replacing dots (.) with double underscores (__) and hyphens (-) with an underscore (_).
	Examples:
	export %sLOG__DEVELOPER=true
	export %sCFG_FILE=/etc/beegfs/beegfs-index-updater.toml
`
		fmt.Fprintf(os.Stderr, helpText, envVarPrefix, envVarPrefix, envVarPrefix)
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
	cfg, ok := cfgMgr.Get().(*config.AppConfig)
	if !ok {
		log.Fatalf("configuration manager returned invalid configuration (expected BeeGFS Index Updater application configuration)")
	}

	logger, err := logger.New(cfg.Log, nil)
	if err != nil {
		log.Fatalf("unable to initialize logger: %s", err)
	}
	defer func() {
		if err := logger.Shutdown(context.Background()); err != nil {
			log.Printf("error during logger shutdown: %v", err)
		}
	}()
	logger.Info("start-of-day", zap.String("application", binaryName), zap.String("version", version), zap.String("commit", commit), zap.String("built", buildTime))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Everything that can be wrong with the environment is found here, before any event is
	// taken: management, license, index, binaries, state. A failure exits, and the unit restarts
	// the service after a delay.
	mg := cfg.Management
	if err := ctl.InitLoggerFromExternal(logger.With(zap.String("component", "ctl"))); err != nil {
		logger.Fatal("unable to initialize ctl library", zap.Error(err))
	}
	err = ctl.InitViperFromExternal(ctl.GlobalConfig{
		MgmtdAddress:                mg.Address,
		MgmtdTLSCertFile:            mg.TLSCertFile,
		MgmtdTLSDisableVerification: mg.TLSDisableVerification,
		MgmtdTLSDisable:             mg.TLSDisable,
		MgmtdUseProxy:               mg.UseProxy,
		AuthFile:                    mg.AuthFile,
		AuthDisable:                 mg.AuthDisable,
	})
	if err != nil {
		logger.Fatal("unable to initialize ctl library", zap.Error(err))
	}
	mgmtd, err := ctl.ManagementClient()
	if err != nil {
		logger.Fatal("unable to connect to management", zap.Error(err))
	}
	license, err := mgmtd.VerifyLicense(ctx, licensedFeature)
	if err != nil {
		logger.Fatal("unable to verify the index license", zap.Error(err))
	}
	logger.Info("index license verified", license...)

	// markwalk looks up index paths with lstat, which does not follow a symlinked index root.
	index, err := filepath.EvalSymlinks(cfg.Markwalk.Index)
	if err != nil {
		logger.Fatal("unable to resolve the index", zap.Error(err))
	}
	cfg.Markwalk.Index = index
	marker := markwalk.New(cfg.Markwalk, cfg.Update.Threads, logger.Logger)
	if err := marker.Check(ctx); err != nil {
		logger.Fatal("unable to check the index", zap.Error(err))
	}
	logger.Info("index checked", zap.String("index", index))
	// The updater checks the mount before every run, not here: collecting events does not need
	// it, and a mount that comes up later lets the next run go ahead. The path must be the mount
	// point itself: the tool deletes every index directory it does not find in the tree, so over a
	// subdirectory it would delete everything outside it.
	mount := filepath.Clean(cfg.Update.Mount)
	mounted := func() error {
		fs, err := filesystem.NewFromPath(mount)
		if err != nil {
			return fmt.Errorf("%s is not a mounted BeeGFS: %w", mount, err)
		}
		if fs.GetMountPath() != mount {
			return fmt.Errorf("%s is not a BeeGFS mount point: the mount point is %s", mount, fs.GetMountPath())
		}
		return nil
	}
	up, err := updater.New(cfg.Update, index, cfg.Markwalk.Work, mounted, marker, logger.Logger)
	if err != nil {
		logger.Fatal("unable to load updater state", zap.Error(err))
	}
	col, err := collector.New(cfg.Collector, marker, up, logger.Logger)
	if err != nil {
		logger.Fatal("unable to start collector", zap.Error(err))
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return up.Run(gctx) })
	g.Go(func() error { return col.Run(gctx) })
	if err := g.Wait(); err != nil {
		logger.Fatal("stopped on error", zap.Error(err))
	}
	logger.Info("stopped")
}
