// Package telemetry provides a common OpenTelemetry metrics implementation for
// BeeGFS Go services. It follows the same lifecycle and configuration patterns
// as the common/logger package.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/thinkparq/beegfs-go/common/configmgr"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
)

const (
	protocolGRPC = "grpc"
	protocolHTTP = "http"
)

// Config represents the telemetry configuration. It is embedded in each
// service's AppConfig and follows the same mapstructure pattern as logger.Config.
type Config struct {
	Enabled     bool             `mapstructure:"enabled"`
	ServiceName string           `mapstructure:"service-name"`
	OTLP        OTLPConfig       `mapstructure:"otlp"`
	Prometheus  PrometheusConfig `mapstructure:"prometheus"`
	Histograms  HistogramConfig  `mapstructure:"histograms"`
	Logs        LogsConfig       `mapstructure:"logs"`
}

// OTLPConfig holds configuration for the OTLP metric exporter.
type OTLPConfig struct {
	Enabled  bool              `mapstructure:"enabled"`
	Protocol string            `mapstructure:"protocol"` // protocolGRPC or protocolHTTP
	Endpoint string            `mapstructure:"endpoint"`
	Interval time.Duration     `mapstructure:"interval"`
	Insecure bool              `mapstructure:"insecure"`
	Headers  map[string]string `mapstructure:"headers"`
}

// PrometheusConfig holds configuration for the Prometheus metric exporter.
type PrometheusConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Port    int    `mapstructure:"port"`
	Path    string `mapstructure:"path"`
}

// HistogramConfig allows overriding the default OTel histogram bucket boundaries.
type HistogramConfig struct {
	DurationBoundaries []float64 `mapstructure:"duration-boundaries"`
	BytesBoundaries    []float64 `mapstructure:"bytes-boundaries"`
}

// LogsConfig holds configuration for the OTLP log exporter.
// Logs are always push-based (OTLP only) — there is no pull-based log
// collection equivalent to Prometheus in the OTel ecosystem.
// This config is independent of Config.Enabled: enabling logs does not
// require enabling OTLP metrics, and they may use different endpoints.
type LogsConfig struct {
	Enabled  bool              `mapstructure:"enabled"`
	Protocol string            `mapstructure:"protocol"`
	Endpoint string            `mapstructure:"endpoint"`
	Insecure bool              `mapstructure:"insecure"`
	Headers  map[string]string `mapstructure:"headers"`
}

// ValidateConfig checks telemetry configuration for consistency.
// Called by the containing AppConfig.ValidateConfig().
func (c *Config) ValidateConfig() error {
	if (c.Enabled || c.Logs.Enabled) && c.ServiceName == "" {
		return fmt.Errorf("telemetry.service-name must be set when telemetry or logs are enabled")
	}
	// Validate metrics exporters only if telemetry (metrics) is enabled.
	if c.Enabled {
		if !c.OTLP.Enabled && !c.Prometheus.Enabled {
			return fmt.Errorf("telemetry is enabled but no exporter is configured: " +
				"enable at least one of [telemetry.otlp] or [telemetry.prometheus]")
		}
		if c.OTLP.Enabled {
			if c.OTLP.Protocol != protocolGRPC && c.OTLP.Protocol != protocolHTTP {
				return fmt.Errorf("telemetry.otlp.protocol must be 'grpc' or 'http' (got '%s')", c.OTLP.Protocol)
			}
			if c.OTLP.Endpoint == "" {
				return fmt.Errorf("telemetry.otlp.endpoint must be set when OTLP is enabled")
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
		}
	}
	// Log export validation is independent of c.Enabled: a user can enable logs
	// without enabling OTLP metrics.
	if c.Logs.Enabled {
		if c.Logs.Protocol != protocolGRPC && c.Logs.Protocol != protocolHTTP {
			return fmt.Errorf("telemetry.logs.protocol must be 'grpc' or 'http' (got '%s')", c.Logs.Protocol)
		}
		if c.Logs.Endpoint == "" {
			return fmt.Errorf("telemetry.logs.endpoint must be set when log export is enabled")
		}
	}
	return nil
}

// Configurer allows the telemetry package to extract its config from any
// AppConfig without importing the application's config package.
// This follows the same pattern as logger.Configurer.
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
	instanceID string
	version    string
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

	// Build resource whenever any signal is enabled; it is shared by both metrics
	// and log providers to identify the service in exported telemetry.
	resource, err := buildResource(cfg, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to build telemetry resource: %w", err)
	}

	if cfg.Enabled {
		readers, promRdr, err := buildReaders(cfg)
		if err != nil {
			return nil, fmt.Errorf("failed to build telemetry exporters: %w", err)
		}
		p.promReader = promRdr

		meterOpts := []sdkmetric.Option{sdkmetric.WithResource(resource)}
		for _, reader := range readers {
			meterOpts = append(meterOpts, sdkmetric.WithReader(reader))
		}

		// Apply histogram bucket views if configured.
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

		if cfg.Prometheus.Enabled {
			if err := p.startPrometheusServer(cfg.Prometheus); err != nil {
				sdkProvider.Shutdown(context.Background())
				return nil, fmt.Errorf("failed to start Prometheus server: %w", err)
			}
		}
	} else {
		p.meterProvider = noop.NewMeterProvider()
	}

	if cfg.Logs.Enabled {
		logExporter, err := buildLogExporter(cfg.Logs)
		if err != nil {
			if p.sdkProvider != nil {
				p.sdkProvider.Shutdown(context.Background())
			}
			if p.promServer != nil {
				p.promServer.Shutdown(context.Background())
			}
			return nil, fmt.Errorf("failed to build log exporter: %w", err)
		}
		p.logProvider = sdklog.NewLoggerProvider(
			sdklog.WithResource(resource),
			sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		)
	}

	return p, nil
}

// Meter returns a named Meter for creating instruments.
// When telemetry is disabled, returns a no-op Meter (zero overhead).
func (p *Provider) Meter(name string) metric.Meter {
	return p.meterProvider.Meter(name)
}

// LogProvider returns the OTel LoggerProvider, or nil when log export is disabled.
// Used by the logger package to attach the otelzap bridge core.
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
// For v1, all telemetry config changes require a restart. This method logs a
// warning if immutable fields changed and stores the new config.
func (p *Provider) UpdateConfiguration(newConfig any) error {
	configurer, ok := newConfig.(Configurer)
	if !ok {
		return fmt.Errorf("unable to get telemetry configuration from the application configuration (this indicates a bug)")
	}
	newCfg := configurer.GetTelemetryConfig()

	// All telemetry config changes take effect only after a restart. Warn
	// whenever a live provider exists so operators are not surprised by a no-op
	// reload. No warning is emitted when nothing is running — there is nothing
	// to restart.
	if p.sdkProvider != nil || p.logProvider != nil {
		if !reflect.DeepEqual(newCfg, p.config) {
			zap.L().Warn("telemetry configuration changes require a restart to take effect")
		}
	}

	p.config = newCfg
	return nil
}

// Standard attribute keys used across BeeGFS metrics.
var (
	AttrState     = attribute.Key("state")
	AttrPriority  = attribute.Key("priority")
	AttrDirection = attribute.Key("direction")
	AttrOperation = attribute.Key("operation")
	AttrStatus    = attribute.Key("status")
	AttrErrorType = attribute.Key("error.type")
	AttrRSTType   = attribute.Key("rst.type")
	AttrRSTID     = attribute.Key("rst.id")
)
