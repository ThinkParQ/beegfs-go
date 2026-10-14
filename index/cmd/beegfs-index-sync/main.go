// Command beegfs-index-sync feeds BeeGFS Watch events to GUFI's incremental update. The collector
// receives events from Watch and spools them to disk; markwalk turns the spool into suspect files
// for beegfs index update. --mode runs both in one process or either one alone, see MERGE-DESIGN.md.
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
	"github.com/thinkparq/beegfs-go/index/internal/markwalk"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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
	pflag.String("mode", "both", "What to run: 'both', 'collect' (Watch to spool) or 'process' (spool to suspect files).")
	pflag.String("spool", "/var/lib/beegfs/index-sync/spool", "The segment directory shared by the collector and markwalk.")
	pflag.String("log.type", "stderr", "Where log messages should be sent ('stderr', 'stdout', 'syslog', 'logfile').")
	pflag.String("log.file", "/var/log/beegfs/beegfs-index-sync.log", "The path to the desired log file when logType is 'logfile' (if needed the directory and all parent directories will be created).")
	pflag.Int8("log.level", 3, "Adjust the logging level (0=Fatal, 1=Error, 2=Warn, 3=Info, 4+5=Debug).")
	pflag.Int("log.max-size", 1000, "When log.type is 'logfile' the maximum size of the log.file in megabytes before it is rotated.")
	pflag.Int("log.num-rotated-files", 5, "When log.type is 'logfile' the maximum number old log.file(s) to keep when log.max-size is reached and the log is rotated.")
	pflag.Bool("log.developer", false, "Enable developer logging including stack traces and setting the equivalent of log.level=5 and log.type=stdout (all other log settings are ignored).")
	pflag.String("collector.listen", "0.0.0.0:50052", "The address Watch connects to.")
	pflag.String("collector.checkpoint", "/var/lib/beegfs/index-sync/checkpoint.json", "The file where acknowledged sequence IDs are recorded.")
	pflag.String("collector.counter", "/var/lib/beegfs/index-sync/counter.json", "The file recording the highest sealed segment number (empty keeps no counter).")
	pflag.Duration("collector.ack-frequency", time.Second, "How often acknowledgements are sent to Watch and written to the checkpoint.")
	pflag.Duration("collector.roll-every", 30*time.Second, "Seal the open segment at least this often.")
	pflag.Int("collector.roll-events", 100_000, "Seal the open segment once it holds this many events.")
	pflag.Int("collector.queue", 65536, "The number of events buffered between the gRPC streams and the segment writer.")
	pflag.Int("collector.seal-queue", 4, "The number of written segments that may wait for fsync before receiving blocks.")
	pflag.Bool("collector.tls-disable", true, "Disable TLS for the gRPC server Watch connects to.")
	pflag.String("markwalk.state", "/var/lib/beegfs/index-sync/markwalk.json", "The file recording the last segment whose suspect file was written.")
	pflag.String("markwalk.work", "/var/lib/beegfs/index-sync/markwalk", "The directory each batch's suspect file is written to, as batch-<N>/suspects.")
	pflag.String("markwalk.mount", "/mnt/beegfs", "The path where BeeGFS is mounted.")
	pflag.String("markwalk.index", "/var/lib/gufi", "The GUFI index root.")
	pflag.String("markwalk.query", "/opt/beegfs/bin/index/gufi_query", "The gufi_query binary used to find the other names of a multi-link inode (empty disables the lookup).")
	pflag.Duration("markwalk.every", time.Minute, "The least time between batches.")
	pflag.Duration("markwalk.poll", 30*time.Second, "How often to look for new segments when running without the collector.")
	pflag.Int("markwalk.max-segments", 100, "The most segments in one batch.")
	pflag.Int("markwalk.stat-cache", 200_000, "The most stat results a batch keeps.")
	pflag.Bool("markwalk.track-opens", false, "Include open events so atime changes are indexed.")
	pflag.Bool("markwalk.retain", false, "Remove segments once their suspect file is written, keeping the newest.")
	pflag.Bool("markwalk.once", false, "Process the segments present now and exit (mode 'process' only).")
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

	runCollector := cfg.Mode == "both" || cfg.Mode == "collect"
	runMarkwalk := cfg.Mode == "both" || cfg.Mode == "process"
	c, m := cfg.Collector, cfg.Markwalk
	c.Spool, m.Spool = cfg.Spool, cfg.Spool

	// In one process a seal nudges markwalk through this channel. Alone, markwalk polls instead.
	var sealed chan struct{}
	if runCollector && runMarkwalk {
		sealed = make(chan struct{}, 1)
	}

	// SIGINT or SIGTERM stops both: the collector seals the open segment and markwalk finishes
	// the batch it is on. A collector error stops the process too; markwalk retries its own.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	g, gctx := errgroup.WithContext(ctx)
	if runCollector {
		g.Go(func() error { return collector.Run(gctx, c, logger.Logger, sealed) })
	}
	if runMarkwalk {
		g.Go(func() error { return markwalk.Run(gctx, m, logger.Logger, sealed) })
	}
	if err := g.Wait(); err != nil {
		logger.Fatal("stopped on error", zap.Error(err))
	}
	logger.Info("stopped")
}
