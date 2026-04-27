package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/thinkparq/beegfs-go/common/configmgr"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"
)

const (
	protocolGRPC = "grpc"
	protocolHTTP = "http"
)

// Config represents the telemetry configuration.
type Config struct {
	Enabled    bool             `mapstructure:"enabled"`
	OTLP       OTLPConfig       `mapstructure:"otlp"`
	Prometheus PrometheusConfig `mapstructure:"prometheus"`
	Histograms HistogramConfig  `mapstructure:"histograms"`
	Logs       LogsConfig       `mapstructure:"logs"`
}

type OTLPConfig struct {
	Enabled                bool              `mapstructure:"enabled"`
	Protocol               string            `mapstructure:"protocol"` // protocolGRPC or protocolHTTP
	Endpoint               string            `mapstructure:"endpoint"`
	URLPath                string            `mapstructure:"url-path"`    // HTTP only; e.g. "/otlp/v1/metrics" for Loki-compatible backends
	Compression            string            `mapstructure:"compression"` // "gzip" or "none"
	Timeout                time.Duration     `mapstructure:"timeout"`
	Interval               time.Duration     `mapstructure:"interval"`
	TLSCertFile            string            `mapstructure:"tls-cert-file"`
	TLSDisableVerification bool              `mapstructure:"tls-disable-verification"`
	TLSDisable             bool              `mapstructure:"tls-disable"`
	Headers                map[string]string `mapstructure:"headers"`
}

type PrometheusConfig struct {
	Enabled       bool   `mapstructure:"enabled"`
	ListenAddress string `mapstructure:"listen-address"`
	Port          int    `mapstructure:"port"`
	Path          string `mapstructure:"path"`
	TLSCertFile   string `mapstructure:"tls-cert-file"`
	TLSKeyFile    string `mapstructure:"tls-key-file"`
	TLSDisable    bool   `mapstructure:"tls-disable"`
}

// HistogramConfig allows overriding the default OTel histogram bucket boundaries.
type HistogramConfig struct {
	DurationBoundaries []float64 `mapstructure:"duration-boundaries"`
	BytesBoundaries    []float64 `mapstructure:"bytes-boundaries"`
}

// Logs are always push-based (OTLP only) — there is no pull-based log
// collection equivalent to Prometheus in the OTel ecosystem.
// This config is independent of Config.Enabled: enabling logs does not
// require enabling OTLP metrics, and they may use different endpoints.
type LogsConfig struct {
	Enabled                bool              `mapstructure:"enabled"`
	Protocol               string            `mapstructure:"protocol"`
	Endpoint               string            `mapstructure:"endpoint"`
	URLPath                string            `mapstructure:"url-path"`    // HTTP only; e.g. "/otlp/v1/logs" for direct Loki ingestion
	Compression            string            `mapstructure:"compression"` // "gzip" or "none"
	Timeout                time.Duration     `mapstructure:"timeout"`
	TLSCertFile            string            `mapstructure:"tls-cert-file"`
	TLSDisableVerification bool              `mapstructure:"tls-disable-verification"`
	TLSDisable             bool              `mapstructure:"tls-disable"`
	Headers                map[string]string `mapstructure:"headers"`
}

// ValidateConfig checks telemetry configuration for consistency.
func (c *Config) ValidateConfig() error {
	if c.Enabled {
		if !c.OTLP.Enabled && !c.Prometheus.Enabled {
			return fmt.Errorf("telemetry is enabled but no exporter is configured: " +
				"enable at least one of [telemetry.otlp] or [telemetry.prometheus]")
		}
		if c.OTLP.Enabled {
			if err := validateOTLPTransport("telemetry.otlp", c.OTLP.Protocol, c.OTLP.Endpoint,
				c.OTLP.Compression, c.OTLP.Timeout, c.OTLP.TLSDisable, c.OTLP.TLSDisableVerification); err != nil {
				return err
			}
			if c.OTLP.Interval < time.Second {
				return fmt.Errorf("telemetry.otlp.interval must be at least 1s (got '%s')", c.OTLP.Interval)
			}
		}
		if c.Prometheus.Enabled {
			if c.Prometheus.Port <= 0 || c.Prometheus.Port > 65535 {
				return fmt.Errorf("telemetry.prometheus.port must be between 1 and 65535 (got %d)", c.Prometheus.Port)
			}
			if c.Prometheus.Path == "" {
				return fmt.Errorf("telemetry.prometheus.path must be set when Prometheus is enabled")
			}
			if !c.Prometheus.TLSDisable {
				certSet := c.Prometheus.TLSCertFile != ""
				keySet := c.Prometheus.TLSKeyFile != ""
				if certSet != keySet {
					return fmt.Errorf("telemetry.prometheus: tls-cert-file and tls-key-file must both be set or both be empty")
				}
			}
		}
	}
	// Log export validation is independent of c.Enabled: a user can enable logs
	// without enabling OTLP metrics.
	if c.Logs.Enabled {
		if err := validateOTLPTransport("telemetry.logs", c.Logs.Protocol, c.Logs.Endpoint,
			c.Logs.Compression, c.Logs.Timeout, c.Logs.TLSDisable, c.Logs.TLSDisableVerification); err != nil {
			return err
		}
	}
	return nil
}

// validateOTLPTransport checks the transport fields shared by both the OTLP
// metrics exporter (OTLPConfig) and the log exporter (LogsConfig).
// prefix is the config key path used in error messages (e.g. "telemetry.otlp").
func validateOTLPTransport(prefix, protocol, endpoint, compression string, timeout time.Duration, tlsDisable, tlsDisableVerification bool) error {
	if protocol != protocolGRPC && protocol != protocolHTTP {
		return fmt.Errorf("%s.protocol must be 'grpc' or 'http' (got '%s')", prefix, protocol)
	}
	if endpoint == "" {
		return fmt.Errorf("%s.endpoint must be set when OTLP is enabled", prefix)
	}
	if compression != "" && compression != "none" && compression != "gzip" {
		return fmt.Errorf("%s.compression must be 'gzip' or 'none' (got '%s')", prefix, compression)
	}
	if timeout != 0 && timeout < time.Second {
		return fmt.Errorf("%s.timeout must be at least 1s (got '%s')", prefix, timeout)
	}
	if tlsDisable && tlsDisableVerification {
		return fmt.Errorf("%s: tls-disable and tls-disable-verification are mutually exclusive; tls-disable-verification only applies when TLS is enabled", prefix)
	}
	return nil
}

// Configurer allows the telemetry package to extract its config from any
// AppConfig without importing the application's config package.
type Configurer interface {
	GetTelemetryConfig() Config
}

// Provider wraps an OTel MeterProvider and manages its lifecycle.
// It implements configmgr.Listener for SIGHUP hot-reload of safe parameters.
type Provider struct {
	meterProvider metric.MeterProvider
	sdkProvider   *sdkmetric.MeterProvider // nil when disabled (using noop)
	logProvider   *sdklog.LoggerProvider   // nil when log export is disabled
	config        Config
	promServer    *http.Server // nil when Prometheus is disabled
	promReader    *prometheusReader
}

// Verify all interfaces that depend on Provider are satisfied:
var _ configmgr.Listener = &Provider{}

// Option is a functional option for configuring a Provider with values that
// come from the service binary (e.g. build-time version, runtime address)
// rather than config files.
type Option func(*options)

type options struct {
	instanceID  string
	version     string
	serviceName string
}

// WithInstanceID sets the OTel service.instance.id resource attribute.
// Typically set to the server's listen address.
func WithInstanceID(id string) Option {
	return func(o *options) {
		o.instanceID = id
	}
}

// WithVersion sets the OTel service.version resource attribute.
func WithVersion(v string) Option {
	return func(o *options) {
		o.version = v
	}
}

// WithServiceName sets the OTel service.name resource attribute.
func WithServiceName(name string) Option {
	return func(o *options) {
		o.serviceName = name
	}
}

// New creates a telemetry Provider based on the given configuration.
// When telemetry is disabled, returns a Provider that uses a no-op
// MeterProvider — all metric calls become zero-cost no-ops.
func New(cfg Config, opts ...Option) (*Provider, error) {
	p := &Provider{config: cfg}

	if err := cfg.ValidateConfig(); err != nil {
		return nil, err
	}

	if !cfg.Enabled && !cfg.Logs.Enabled {
		p.meterProvider = noop.NewMeterProvider()
		return p, nil
	}

	// cleanupFns collects shutdown callbacks for resources acquired during
	// construction. On any error, cleanup() drains them best-effort with a
	// short timeout so New() never blocks indefinitely before signal handling
	// is set up in main.
	//
	// Resources are drained in registration order (oldest first). This differs
	// from the reverse order used by Shutdown(), but it is safe here because
	// cleanup() only fires during partial construction where the Prometheus
	// server and SDK provider have never handled real traffic.
	var cleanupFns []func(context.Context) error
	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, fn := range cleanupFns {
			_ = fn(ctx) // best-effort; the construction error is already being returned
		}
	}

	// Build resource whenever any signal is enabled; it is shared by both metrics
	// and log providers to identify the service in exported telemetry.
	res, err := buildResource(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build telemetry resource: %w", err)
	}

	if cfg.Enabled {
		if err := p.initMetrics(cfg, res, &cleanupFns); err != nil {
			cleanup()
			return nil, err
		}
	} else {
		p.meterProvider = noop.NewMeterProvider()
	}

	if cfg.Logs.Enabled {
		if err := p.initLogs(cfg, res, &cleanupFns); err != nil {
			cleanup()
			return nil, err
		}
	}

	return p, nil
}

// initMetrics creates and registers the OTel MeterProvider and any configured metric exporters
// (OTLP, Prometheus). On success it appends shutdown callbacks to cleanupFns.
func (p *Provider) initMetrics(cfg Config, res *resource.Resource, cleanupFns *[]func(context.Context) error) error {
	readers, promRdr, err := buildReaders(cfg)
	if err != nil {
		return fmt.Errorf("failed to build telemetry exporters: %w", err)
	}
	p.promReader = promRdr

	meterOpts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	for _, reader := range readers {
		meterOpts = append(meterOpts, sdkmetric.WithReader(reader))
	}

	// OTel's default histogram bucket boundaries ([0, 5ms, 10ms, ..., 10s]) are
	// designed for web request latency and are meaningless for job durations (seconds
	// to hours) or byte counts (KB to GB). Override the boundaries for any instruments
	// whose names match the configured patterns so the distribution data is useful.
	if len(cfg.Histograms.DurationBoundaries) > 0 {
		meterOpts = append(meterOpts, sdkmetric.WithView(
			sdkmetric.NewView(
				sdkmetric.Instrument{Name: "*.duration"},
				sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
					Boundaries: cfg.Histograms.DurationBoundaries,
				}},
			),
		))
	}
	if len(cfg.Histograms.BytesBoundaries) > 0 {
		meterOpts = append(meterOpts, sdkmetric.WithView(
			sdkmetric.NewView(
				sdkmetric.Instrument{Name: "*.bytes.*"},
				sdkmetric.Stream{Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
					Boundaries: cfg.Histograms.BytesBoundaries,
				}},
			),
		))
	}

	sdkProvider := sdkmetric.NewMeterProvider(meterOpts...)
	p.sdkProvider = sdkProvider
	p.meterProvider = sdkProvider
	*cleanupFns = append(*cleanupFns, sdkProvider.Shutdown)

	if cfg.Prometheus.Enabled {
		if err := p.startPrometheusServer(cfg.Prometheus); err != nil {
			return fmt.Errorf("failed to start Prometheus server: %w", err)
		}
		srv := p.promServer
		*cleanupFns = append(*cleanupFns, srv.Shutdown)
	}

	return nil
}

// initLogs creates the OTel LoggerProvider and its OTLP exporter.
// On success it appends a shutdown callback to cleanupFns.
func (p *Provider) initLogs(cfg Config, res *resource.Resource, cleanupFns *[]func(context.Context) error) error {
	logExporter, err := buildLogExporter(cfg.Logs)
	if err != nil {
		return fmt.Errorf("failed to build log exporter: %w", err)
	}
	p.logProvider = sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
	)
	*cleanupFns = append(*cleanupFns, p.logProvider.Shutdown)
	return nil
}

// Meter returns a named Meter for creating instruments.
// When telemetry is disabled, returns a no-op Meter (zero overhead).
func (p *Provider) Meter(name string) metric.Meter {
	return p.meterProvider.Meter(name)
}

// LogProvider returns the OTel LoggerProvider, or nil when log export is disabled.
func (p *Provider) LogProvider() *sdklog.LoggerProvider {
	return p.logProvider
}

// Shutdown flushes pending metrics and releases all resources.
// Must be called during graceful service shutdown.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.sdkProvider == nil && p.logProvider == nil {
		return nil
	}
	var errs []error
	if p.promServer != nil {
		if err := p.promServer.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("prometheus server shutdown: %w", err))
		}
	}
	if p.sdkProvider != nil {
		if err := p.sdkProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("meter provider shutdown: %w", err))
		}
	}
	if p.logProvider != nil {
		if err := p.logProvider.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("log provider shutdown: %w", err))
		}
	}
	return errors.Join(errs...)
}

// UpdateConfiguration implements configmgr.Listener.
// All telemetry config changes require a restart; this logs a warning if the
// config changed while a signal is running so operators aren't surprised.
func (p *Provider) UpdateConfiguration(newConfig any) error {
	configurer, ok := newConfig.(Configurer)
	if !ok {
		return fmt.Errorf("unable to get telemetry configuration from the application configuration (this indicates a bug)")
	}
	newCfg := configurer.GetTelemetryConfig()

	if err := newCfg.ValidateConfig(); err != nil {
		return fmt.Errorf("invalid telemetry configuration: %w", err)
	}

	// All telemetry config changes take effect only after a restart. Warn if at
	// least one signal (metrics or logs) is actually running, so operators are
	// not surprised by a no-op SIGHUP reload. If telemetry was disabled at
	// startup, both fields are nil and there is nothing to restart.
	if p.sdkProvider != nil || p.logProvider != nil {
		if !reflect.DeepEqual(newCfg, p.config) {
			zap.L().Warn("telemetry configuration changes require a restart to take effect")
		}
	}

	p.config = newCfg
	return nil
}
