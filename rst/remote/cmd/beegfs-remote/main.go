package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/mock"
	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/config"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/job"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/server"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/worker"
	"github.com/thinkparq/beegfs-go/rst/remote/internal/workermgr"
	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
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
	pflag.Int("developer.perf-profiling-port", 0, "Specify a port where performance profiles will be made available on the localhost via pprof (0 disables performance profiling).")
	pflag.CommandLine.MarkHidden("developer.perf-profiling-port")
	pflag.Bool("developer.dump-config", false, "Dump the full configuration and immediately exit.")
	pflag.CommandLine.MarkHidden("developer.dump-config")
	pflag.Uint32("developer.benchmark-target", 0, "Specify a target to perform an upload sync operation (0 uses mock target)")
	pflag.Int("developer.benchmark-count", 0, "Specify a target to perform an upload sync operation (0 disables performance benchmarking)")
	pflag.Int32("developer.benchmark-mock-work-requests", 1, "Specify the number of mocked work requests per job")
	pflag.Bool("developer.benchmark-mock-beegfs", false, "Whether BeeGFS should be mocked")
	pflag.CommandLine.MarkHidden("developer.benchmark-target")
	pflag.CommandLine.MarkHidden("developer.benchmark-count")
	pflag.CommandLine.MarkHidden("developer.benchmark-work-requests")
	pflag.CommandLine.MarkHidden("developer.benchmark-mock-beegfs")

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

	benchmarkTarget, _ := pflag.CommandLine.GetUint32("developer.benchmark-target")
	benchmarkCount, _ := pflag.CommandLine.GetInt("developer.benchmark-count")
	BenchmarkMockWorkRequests, _ := pflag.CommandLine.GetInt32("developer.benchmark-mock-work-requests")
	BenchmarkMockBeeGFS, _ := pflag.CommandLine.GetBool("developer.benchmark-mock-beegfs")
	if BenchmarkMockWorkRequests < 1 {
		BenchmarkMockWorkRequests = 1
	}
	if benchmarkCount != 0 {
		dbPath := "/var/lib/beegfs/remote/benchmark.badger"
		defer os.RemoveAll(dbPath)

		if err := pflag.CommandLine.Set("job.path-db", dbPath); err != nil {
			fmt.Printf("unable to set temporary BadgerDB path for performance benchmarking: %s\n", err)
			os.Exit(1)
		}
		pprofPort := "16060"
		if err := pflag.CommandLine.Set("developer.perf-profiling-port", pprofPort); err != nil {
			fmt.Printf("unable to set temporary BadgerDB path for performance benchmarking: %s\n", err)
			os.Exit(1)
		}
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
	logger.Info("<=== BeeRemote Initialized ===>")
	logger.Info("start-of-day", zap.String("application", binaryName), zap.String("version", version), zap.String("commit", commit), zap.String("built", buildTime))
	// Determine if we should use a real or mock mount point:
	logger.Info("checking BeeGFS mount point")
	// If we hang here, probably BeeGFS itself is not reachable.
	mountPoint, err := filesystem.NewFromMountPoint(initialCfg.MountPoint)
	if err != nil {
		logger.Fatal("unable to access BeeGFS mount point", zap.Error(err))
	}

	if benchmarkCount != 0 && benchmarkTarget == 0 {
		mockedMountPoint := benchmarkMockTarget(initialCfg)
		if BenchmarkMockBeeGFS {
			fmt.Println("Mocking BeeGFS mount point")
			mountPoint = mockedMountPoint
		}
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

	workerManager, err := workermgr.NewManager(ctx, logger.Logger, initialCfg.WorkerMgr, initialCfg.Workers, initialCfg.RemoteStorageTargets, flex.BeeRemoteNode_builder{
		Id:      nodeID,
		Address: initialCfg.Server.Address,
	}.Build(), mountPoint)
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

	jobServer, err := server.New(logger.Logger, initialCfg.Server, jobManager)
	if err != nil {
		logger.Fatal("failed to initialize Remote gRPC server", zap.Error(err))
	}

	// Most components should not shutdown unexpectedly once they are started, but anything that
	// might should return an error on this channel signalling the application to shutdown. Increase
	// the channel size as needed if other components may also use this channel to log errors.
	errChan := make(chan error, 2)
	jobServer.ListenAndServe(errChan)

	if benchmarkCount != 0 {
		var benchmarkTargetType *flex.RemoteStorageTarget
		for _, rst := range initialCfg.RemoteStorageTargets {
			if rst.Id == benchmarkTarget {
				benchmarkTargetType = rst
				break
			}
		}

		address := strings.Split(initialCfg.Server.Address, ":")[0]
		port := initialCfg.Developer.PerfProfilingPort
		varsUrl := fmt.Sprintf("http://%s:%d/debug/vars", address, port)

		time.Sleep(5 * time.Second) // Wait for job server to start
		if err := runBenchmarks(ctx, cancel, jobManager, mountPoint.GetMountPath(), benchmarkTarget, benchmarkTargetType, BenchmarkMockWorkRequests, benchmarkCount, varsUrl); err != nil {
			fmt.Printf("unable to complete performance benchmark: %v\n", err)
		}
	}

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

func runBenchmarks(ctx context.Context, cancel context.CancelFunc, jobMgr *job.Manager, mountPath string, target uint32, targetType *flex.RemoteStorageTarget, workRequests int32, count int, varsUrl string) error {
	if target == 0 {
		// target type is mocked so cancel immediately after testing
		defer cancel()
	}

	benchmarkCtx, benchmarkCancel := context.WithCancel(ctx)
	defer benchmarkCancel()

	inMountPath := "performance-testing.tmp"
	path := filepath.Join(mountPath, inMountPath)
	if err := os.Mkdir(path, 0755); err != nil && !errors.Is(err, fs.ErrExist) {
		return fmt.Errorf("unable to create temporary file directory: %w", err)
	}
	if err := benchmarkCreateFiles(benchmarkCtx, path, count); err != nil {
		return fmt.Errorf("failed to complete benchmark: %w", err)
	}

	logPath := fmt.Sprintf("benchmark-%v", time.Now().Unix())
	if err := os.Mkdir(logPath, 0755); err != nil {
		return fmt.Errorf("unable to create log directory: %w", err)
	}
	if err := collectPProfDebugVars(benchmarkCtx, varsUrl, logPath); err != nil {
		return err
	}
	measurementChan, err := benchmarkMeasure(benchmarkCtx, logPath)
	if err != nil {
		return err
	}

	if err := benchmarkSubmit(benchmarkCtx, jobMgr, measurementChan, target, targetType, workRequests, inMountPath, count); err != nil {
		return fmt.Errorf("failed to complete benchmark: %w", err)
	}

	return nil
}

func benchmarkCreateFiles(ctx context.Context, path string, count int) error {
	fmt.Println("Create temporary files...")
	workers := runtime.GOMAXPROCS(0)
	fileChan := benchmarkGetFilepaths(path, count)

	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < workers; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case path, ok := <-fileChan:
					if !ok {
						return nil
					}

					if _, err := os.Stat(path); err != nil && os.IsNotExist(err) {
						f, err := os.Create(path)
						if err != nil {
							return err
						}
						f.Close()
					}
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("failed to create temporary files: %w", err)
	}

	fmt.Printf("Temporary files created. Path: %s\n", path)
	return nil
}

type measurement struct {
	count       int64
	elapsedTime float64
}

func benchmarkMeasure(ctx context.Context, logPath string) (chan<- measurement, error) {
	path := filepath.Join(logPath, "results.csv")
	resultsHandle, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("unable to open file to write benchmark results: %w", err)
	}

	measurementChan := make(chan measurement, 128)
	go func() {
		defer resultsHandle.Close()
		resultsHandle.WriteString("count,elapsed_time,throughput")

		start := time.Now()
		var count int64
		throughputs := []float64{}
	collect:
		for {
			select {
			case <-ctx.Done():
				break collect
			case measurement, ok := <-measurementChan:
				if !ok {
					break collect
				}
				countMeasured := measurement.count - count
				count += countMeasured
				throughput := float64(countMeasured) / measurement.elapsedTime
				throughputs = append(throughputs, throughput)
				line := fmt.Sprintf("\n%d,%.2f,%.2f", count, measurement.elapsedTime, throughput)
				resultsHandle.WriteString(line)
			}
		}

		filteredThroughputs := filterOutliers(throughputs, 2.0)
		var sum float64
		var maximum float64
		var minimum float64
		for _, throughput := range filteredThroughputs {
			sum += throughput
			if throughput > maximum {
				maximum = throughput
			}
			if minimum == 0.0 || minimum > throughput {
				minimum = throughput
			}
		}
		filteredAvg := sum / float64(len(filteredThroughputs))
		totalSeconds := time.Since(start).Seconds()

		path = filepath.Join(logPath, "summary.log")
		summaryHandle, err := os.Create(path)
		if err != nil {
			return
		}
		summary := fmt.Sprintf("Files transferred: %d\nDuration: %.2f sec\nAverage: %.2f jobs/s\nAverage Without Outliers: %.2f jobs/s\nMaximum: %.2f jobs/s\nMinimum: %.2f jobs/s\n", count, totalSeconds, float64(count)/totalSeconds, filteredAvg, maximum, minimum)
		summaryHandle.WriteString(summary)
		fmt.Print("Benchmark Complete.\n\n")
		fmt.Println(summary)
	}()

	return measurementChan, nil
}

func collectPProfDebugVars(ctx context.Context, varsUrl string, logPath string) error {
	path := filepath.Join(logPath, "pprof-debug-vars.ndjson")
	statsHandle, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("unable to collect performance profile variables: %w", err)
	}

	go func() {
		defer statsHandle.Close()
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				resp, err := http.Get(varsUrl)
				if err != nil {
					fmt.Printf("failed to fetch pprof debug variables: %v", err)
					return
				}
				defer resp.Body.Close()

				body, err := io.ReadAll(resp.Body)
				if err != nil {
					fmt.Printf("failed to read pprof debug variables: %v", err)
					return
				}

				loadavg, err := os.ReadFile("/proc/loadavg")
				if err != nil {
					fmt.Printf("failed to read /proc/loadavg: %v", err)
					return
				}
				fields := strings.Fields(strings.Split(string(loadavg), "\n")[0])
				var data map[string]interface{}
				if err := json.Unmarshal(body, &data); err != nil {
					fmt.Printf("failed to parse /proc/loadavg: %v", err)
					return
				}
				data["load1"], _ = strconv.ParseFloat(fields[0], 64)
				data["load5"], _ = strconv.ParseFloat(fields[1], 64)
				data["load15"], _ = strconv.ParseFloat(fields[2], 64)
				data["timestamp"] = time.Now().Format(time.RFC3339)
				enc := json.NewEncoder(statsHandle)
				enc.SetIndent("", "")
				if err := enc.Encode(data); err != nil {
					fmt.Printf("failed to write compact JSON: %v", err)
				}
			}
		}
	}()

	return nil
}

func benchmarkSubmit(ctx context.Context, mgr *job.Manager, measurementChan chan<- measurement, target uint32, targetType *flex.RemoteStorageTarget, workRequests int32, path string, count int) error {
	defer close(measurementChan)
	if target == 0 {
		fmt.Println("Run performance benchmark against mocked target...")
	} else {
		fmt.Printf("Run performance benchmark against target %d...\n", target)
	}

	workers := runtime.GOMAXPROCS(0)
	fileChan := benchmarkGetFilepaths(path, count)
	baseRequest := &beeremote.JobRequest{RemoteStorageTarget: target}
	switch targetType.WhichType() {
	case flex.RemoteStorageTarget_S3_case:
		baseRequest.Type = &beeremote.JobRequest_Sync{
			Sync: &flex.SyncJob{
				Operation: flex.SyncJob_UPLOAD,
			},
		}
	case flex.RemoteStorageTarget_Mock_case:
		baseRequest.Type = &beeremote.JobRequest_Mock{
			Mock: &flex.MockJob{
				FileSize:        1,
				NumTestSegments: workRequests,
				CanRetry:        true,
			},
		}
	default:
		return rst.ErrUnsupportedOpForRST
	}

	var counter atomic.Int64
	previous := time.Now()
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < workers; i++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case path, ok := <-fileChan:
					if !ok {
						return nil
					}

					request := proto.Clone(baseRequest).(*beeremote.JobRequest)
					request.SetPath(path)
					_, err := mgr.SubmitJobRequest(request)
					if err != nil {
						return err
					}

					count := counter.Add(1)
					if count%2500 == 0 {
						current := time.Now()
						measurementChan <- measurement{elapsedTime: current.Sub(previous).Seconds(), count: count}
						previous = current
					}
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("unable to submit all upload job requests: %w", err)
	}
	fmt.Println("Performance benchmark complete.")

	return nil
}

func benchmarkGetFilepaths(path string, count int) <-chan string {
	fileChan := make(chan string, 1024)
	go func() {
		defer close(fileChan)
		for i := 0; i < count; i++ {
			fileChan <- fmt.Sprintf("%s/%d", path, i)
		}
	}()

	return fileChan
}

func benchmarkMockTarget(cfg *config.AppConfig) filesystem.Provider {
	cfg.WorkerMgr = workermgr.Config{}
	cfg.Workers = []worker.Config{
		{
			ID:                  "0",
			Name:                "test-node-0",
			Type:                worker.Mock,
			MaxReconnectBackOff: 5,
			MockConfig: worker.MockConfig{
				Expectations: []worker.MockExpectation{
					{
						MethodName: "connect",
						ReturnArgs: []interface{}{false, nil},
					},
					{
						MethodName: "SubmitWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							flex.Work_Status_builder{
								State:   flex.Work_SCHEDULED,
								Message: "test expects a scheduled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "UpdateWork",
						Args:       []interface{}{mock.Anything},
						ReturnArgs: []interface{}{
							flex.Work_Status_builder{
								State:   flex.Work_CANCELLED,
								Message: "test expects a cancelled request",
							}.Build(),
							nil,
						},
					},
					{
						MethodName: "disconnect",
						ReturnArgs: []interface{}{nil},
					},
				},
			},
		},
	}
	cfg.RemoteStorageTargets = []*flex.RemoteStorageTarget{
		flex.RemoteStorageTarget_builder{
			Id:   0,
			Mock: proto.String("test"),
		}.Build(),
	}
	return filesystem.NewMockFS()
}

func filterOutliers(measurements []float64, stdCount float64) []float64 {
	if len(measurements) == 0 {
		return measurements
	}
	var sum float64
	for _, measurement := range measurements {
		sum += measurement
	}
	mean := sum / float64(len(measurements))

	var sumSquares float64
	for _, measurement := range measurements {
		diff := measurement - mean
		sumSquares += diff * diff
	}
	threshold := stdCount * math.Sqrt(sumSquares/float64(len(measurements)))

	filtered := make([]float64, 0, len(measurements))
	for _, measurement := range measurements {
		if math.Abs(measurement-mean) <= threshold {
			filtered = append(filtered, measurement)
		}
	}
	return filtered
}
