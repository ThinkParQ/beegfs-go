// Command beegfs-index-sync feeds BeeGFS Watch events to GUFI's incremental update: it receives
// events from Watch, batches them and writes one suspect file per batch for beegfs index update.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/index/internal/collector"
	"github.com/thinkparq/beegfs-go/index/internal/config"
	"go.uber.org/zap"
)

const envVarPrefix = "BEEINDEXSYNC_"

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
	pflag.String("log.file", "/var/log/beegfs/beegfs-index-sync.log", "The path to the desired log file when logType is 'logfile' (if needed the directory and all parent directories will be created).")
	pflag.Int8("log.level", 3, "Adjust the logging level (0=Fatal, 1=Error, 2=Warn, 3=Info, 4+5=Debug).")
	pflag.Int("log.max-size", 1000, "When log.type is 'logfile' the maximum size of the log.file in megabytes before it is rotated.")
	pflag.Int("log.num-rotated-files", 5, "When log.type is 'logfile' the maximum number old log.file(s) to keep when log.max-size is reached and the log is rotated.")
	pflag.Bool("log.developer", false, "Enable developer logging including stack traces and setting the equivalent of log.level=5 and log.type=stdout (all other log settings are ignored).")
	pflag.String("collector.listen", "0.0.0.0:50052", "The address Watch connects to.")
	pflag.String("collector.checkpoint", "/var/lib/beegfs/index-sync/checkpoint.json", "The file where acknowledged sequence IDs are recorded.")
	pflag.Duration("collector.ack-frequency", time.Second, "How often acknowledgements are sent to Watch and written to the checkpoint.")
	pflag.Duration("collector.batch-every", 30*time.Second, "Close the open batch at least this often.")
	pflag.Int("collector.batch-events", 50_000, "Close the open batch once it holds this many events.")
	pflag.Int("collector.queue", 65536, "The number of events buffered between the gRPC streams and the batcher.")
	pflag.Int("collector.batch-queue", 2, "The number of closed batches that may wait for markwalk before receiving blocks.")
	pflag.Bool("collector.tls-disable", true, "Disable TLS for the gRPC server Watch connects to.")
	pflag.String("markwalk.work", "/var/lib/beegfs/index-sync/markwalk", "The directory each batch's suspect file is written to, as batch-<N>/suspects.")
	pflag.String("markwalk.mount", "/mnt/beegfs", "The path where BeeGFS is mounted.")
	pflag.String("markwalk.index", "/var/lib/gufi", "The GUFI index root.")
	pflag.String("markwalk.query", "/opt/beegfs/bin/index/gufi_query", "The gufi_query binary used to find the other names of a multi-link inode (empty disables the lookup).")
	pflag.Duration("markwalk.query-timeout", 30*time.Second, "The most time one gufi_query run may take; on timeout the batch goes on without those names.")
	pflag.Int("markwalk.stat-cache", 200_000, "The most stat results a batch keeps.")
	pflag.Bool("markwalk.track-opens", false, "Include open events so atime changes are indexed.")
	pflag.CommandLine.SortFlags = false
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
		log.Fatalf("configuration manager returned invalid configuration (expected BeeGFS Index Sync application configuration)")
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
	if err := collector.Run(ctx, cfg.Collector, cfg.Markwalk, logger.Logger); err != nil {
		logger.Fatal("stopped on error", zap.Error(err))
	}
	logger.Info("stopped")
}
