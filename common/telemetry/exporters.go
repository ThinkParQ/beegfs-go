package telemetry

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"

	"google.golang.org/grpc/credentials"

	otlploggrpc "go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	otlploghttp "go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	otlpmetricgrpc "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	otlpmetrichttp "go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	prometheusexporter "go.opentelemetry.io/otel/exporters/prometheus"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// prometheusReader wraps the Prometheus exporter and its registry so we can
// serve its HTTP handler separately.
type prometheusReader struct {
	exporter *prometheusexporter.Exporter
	registry *prometheus.Registry
}

// buildReaders constructs metric readers based on configuration.
// Multiple readers can be active simultaneously — the OTel MeterProvider
// supports this natively. Returns the readers and an optional prometheusReader
// (non-nil when Prometheus is enabled).
func buildReaders(cfg Config) ([]sdkmetric.Reader, *prometheusReader, error) {
	var readers []sdkmetric.Reader
	var promRdr *prometheusReader

	if cfg.OTLP.Enabled {
		reader, err := buildOTLPReader(cfg.OTLP)
		if err != nil {
			return nil, nil, err
		}
		readers = append(readers, reader)
	}

	if cfg.Prometheus.Enabled {
		rdr, err := buildPrometheusReader()
		if err != nil {
			return nil, nil, err
		}
		readers = append(readers, rdr.exporter)
		promRdr = rdr
	}

	return readers, promRdr, nil
}

// buildClientTLSConfig constructs a *tls.Config for OTLP exporters.
// Returns nil when certFile is empty and disableVerification is false; callers must
// not pass any TLS option to the exporter in that case, letting the OTel SDK use its
// default transport (which enables TLS using the system cert pool).
func buildClientTLSConfig(certFile string, disableVerification bool) (*tls.Config, error) {
	if certFile == "" && !disableVerification {
		return nil, nil
	}
	certPool, err := x509.SystemCertPool()
	if err != nil {
		return nil, fmt.Errorf("couldn't load system cert pool: %w", err)
	}
	if certFile != "" {
		cert, err := os.ReadFile(certFile)
		if err != nil {
			return nil, fmt.Errorf("reading certificate file failed: %w", err)
		}
		if !certPool.AppendCertsFromPEM(cert) {
			return nil, fmt.Errorf("appending provided certificate to pool failed")
		}
	}
	if disableVerification {
		zap.L().Warn("TLS certificate verification is disabled for this OTLP connection; the connection is vulnerable to man-in-the-middle attacks")
	}
	return &tls.Config{
		RootCAs:            certPool,
		InsecureSkipVerify: disableVerification,
	}, nil
}

// buildOTLPReader constructs a periodic metric reader for the given OTLP config.
// The option-building logic in this function mirrors buildLogExporter.
// Keep both in sync when adding new OTLP fields.
//
// context.Background() is passed to the exporter constructor because the OTel SDK
// connects lazily. If a future SDK version adds a synchronous handshake, pass ctx
// down from New() instead.
func buildOTLPReader(cfg OTLPConfig) (sdkmetric.Reader, error) {
	var exporter sdkmetric.Exporter
	var err error

	// If the user explicitly disabled TLS that takes precedence; TLSCertFile and
	// TLSDisableVerification are ignored when TLSDisable is set.
	switch cfg.Protocol {
	case protocolGRPC:
		if cfg.URLPath != "" {
			zap.L().Warn("telemetry.otlp.url-path is ignored for gRPC protocol")
		}
		grpcOpts := []otlpmetricgrpc.Option{
			otlpmetricgrpc.WithEndpoint(cfg.Endpoint),
		}
		if cfg.TLSDisable {
			grpcOpts = append(grpcOpts, otlpmetricgrpc.WithInsecure())
		} else {
			tlsCfg, err := buildClientTLSConfig(cfg.TLSCertFile, cfg.TLSDisableVerification)
			if err != nil {
				return nil, fmt.Errorf("failed to build TLS config: %w", err)
			}
			if tlsCfg != nil {
				grpcOpts = append(grpcOpts, otlpmetricgrpc.WithTLSCredentials(credentials.NewTLS(tlsCfg)))
			}
		}
		if cfg.Compression == "gzip" {
			// The gRPC exporter accepts a string compressor name, not a typed constant.
			grpcOpts = append(grpcOpts, otlpmetricgrpc.WithCompressor("gzip"))
		}
		if cfg.Timeout > 0 {
			grpcOpts = append(grpcOpts, otlpmetricgrpc.WithTimeout(cfg.Timeout))
		}
		if len(cfg.Headers) > 0 {
			grpcOpts = append(grpcOpts, otlpmetricgrpc.WithHeaders(cfg.Headers))
		}
		exporter, err = otlpmetricgrpc.New(context.Background(), grpcOpts...)

	case protocolHTTP:
		httpOpts := []otlpmetrichttp.Option{
			otlpmetrichttp.WithEndpoint(cfg.Endpoint),
		}
		if cfg.TLSDisable {
			httpOpts = append(httpOpts, otlpmetrichttp.WithInsecure())
		} else {
			tlsCfg, err := buildClientTLSConfig(cfg.TLSCertFile, cfg.TLSDisableVerification)
			if err != nil {
				return nil, fmt.Errorf("failed to build TLS config: %w", err)
			}
			if tlsCfg != nil {
				httpOpts = append(httpOpts, otlpmetrichttp.WithTLSClientConfig(tlsCfg))
			}
		}
		if cfg.URLPath != "" {
			httpOpts = append(httpOpts, otlpmetrichttp.WithURLPath(cfg.URLPath))
		}
		if cfg.Compression == "gzip" {
			httpOpts = append(httpOpts, otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression))
		}
		if cfg.Timeout > 0 {
			httpOpts = append(httpOpts, otlpmetrichttp.WithTimeout(cfg.Timeout))
		}
		if len(cfg.Headers) > 0 {
			httpOpts = append(httpOpts, otlpmetrichttp.WithHeaders(cfg.Headers))
		}
		exporter, err = otlpmetrichttp.New(context.Background(), httpOpts...)

	default:
		return nil, fmt.Errorf("unsupported OTLP protocol: %q (must be 'grpc' or 'http')", cfg.Protocol)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP %s exporter: %w", cfg.Protocol, err)
	}

	return sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(cfg.Interval)), nil
}

func buildPrometheusReader() (*prometheusReader, error) {
	// Use a custom registry so we can serve its handler independently of the
	// process-wide default Prometheus registry. Explicitly register the standard
	// Go runtime and process collectors so the /metrics endpoint includes them,
	// since they are only auto-registered on prometheus.DefaultRegisterer.
	reg := prometheus.NewRegistry()
	if err := reg.Register(collectors.NewGoCollector()); err != nil {
		return nil, fmt.Errorf("failed to register Go collector: %w", err)
	}
	if err := reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return nil, fmt.Errorf("failed to register process collector: %w", err)
	}
	exp, err := prometheusexporter.New(prometheusexporter.WithRegisterer(reg))
	if err != nil {
		return nil, fmt.Errorf("failed to create Prometheus exporter: %w", err)
	}
	return &prometheusReader{exporter: exp, registry: reg}, nil
}

// buildLogExporter constructs an OTLP log exporter for the given configuration.
// The option-building logic in this function mirrors buildOTLPReader.
// Keep both in sync when adding new OTLP fields.
//
// context.Background() is passed to the exporter constructor because the OTel SDK
// connects lazily. If a future SDK version adds a synchronous handshake, pass ctx
// down from New() instead.
func buildLogExporter(cfg LogsConfig) (sdklog.Exporter, error) {
	switch cfg.Protocol {
	case protocolGRPC:
		if cfg.URLPath != "" {
			zap.L().Warn("telemetry.logs.url-path is ignored for gRPC protocol")
		}
		grpcOpts := []otlploggrpc.Option{
			otlploggrpc.WithEndpoint(cfg.Endpoint),
		}
		if cfg.TLSDisable {
			grpcOpts = append(grpcOpts, otlploggrpc.WithInsecure())
		} else {
			tlsCfg, err := buildClientTLSConfig(cfg.TLSCertFile, cfg.TLSDisableVerification)
			if err != nil {
				return nil, fmt.Errorf("failed to build TLS config: %w", err)
			}
			if tlsCfg != nil {
				grpcOpts = append(grpcOpts, otlploggrpc.WithTLSCredentials(credentials.NewTLS(tlsCfg)))
			}
		}
		if cfg.Compression == "gzip" {
			// The gRPC exporter accepts a string compressor name, not a typed constant.
			grpcOpts = append(grpcOpts, otlploggrpc.WithCompressor("gzip"))
		}
		if cfg.Timeout > 0 {
			grpcOpts = append(grpcOpts, otlploggrpc.WithTimeout(cfg.Timeout))
		}
		if len(cfg.Headers) > 0 {
			grpcOpts = append(grpcOpts, otlploggrpc.WithHeaders(cfg.Headers))
		}
		exp, err := otlploggrpc.New(context.Background(), grpcOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP grpc log exporter: %w", err)
		}
		return exp, nil

	case protocolHTTP:
		httpOpts := []otlploghttp.Option{
			otlploghttp.WithEndpoint(cfg.Endpoint),
		}
		if cfg.TLSDisable {
			httpOpts = append(httpOpts, otlploghttp.WithInsecure())
		} else {
			tlsCfg, err := buildClientTLSConfig(cfg.TLSCertFile, cfg.TLSDisableVerification)
			if err != nil {
				return nil, fmt.Errorf("failed to build TLS config: %w", err)
			}
			if tlsCfg != nil {
				httpOpts = append(httpOpts, otlploghttp.WithTLSClientConfig(tlsCfg))
			}
		}
		if cfg.URLPath != "" {
			httpOpts = append(httpOpts, otlploghttp.WithURLPath(cfg.URLPath))
		}
		if cfg.Compression == "gzip" {
			httpOpts = append(httpOpts, otlploghttp.WithCompression(otlploghttp.GzipCompression))
		}
		if cfg.Timeout > 0 {
			httpOpts = append(httpOpts, otlploghttp.WithTimeout(cfg.Timeout))
		}
		if len(cfg.Headers) > 0 {
			httpOpts = append(httpOpts, otlploghttp.WithHeaders(cfg.Headers))
		}
		exp, err := otlploghttp.New(context.Background(), httpOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create OTLP http log exporter: %w", err)
		}
		return exp, nil

	default:
		return nil, fmt.Errorf("unsupported logs OTLP protocol: %q (must be 'grpc' or 'http')", cfg.Protocol)
	}
}

// startPrometheusServer starts the Prometheus HTTP server on the configured port
// and registers the metrics handler at the configured path.
func (p *Provider) startPrometheusServer(cfg PrometheusConfig) error {
	if p.promReader == nil {
		return fmt.Errorf("prometheus reader not initialized")
	}

	// Pre-bind the port synchronously so any bind error (e.g. port already in
	// use) is returned immediately to the caller rather than being lost in a
	// goroutine.
	addr := fmt.Sprintf("%s:%d", cfg.ListenAddress, cfg.Port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("prometheus server failed to bind on %s: %w", addr, err)
	}

	mux := http.NewServeMux()
	mux.Handle(cfg.Path, promhttp.HandlerFor(p.promReader.registry, promhttp.HandlerOpts{}))

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	p.promServer = srv

	serveListener := net.Listener(ln)
	if !cfg.TLSDisable && cfg.TLSCertFile != "" && cfg.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCertFile, cfg.TLSKeyFile)
		if err != nil {
			ln.Close()
			return fmt.Errorf("prometheus server: failed to load TLS cert/key: %w", err)
		}
		serveListener = tls.NewListener(ln, &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})
	} else if cfg.TLSDisable {
		zap.L().Info("prometheus metrics server TLS explicitly disabled")
	}

	go func() {
		if err := srv.Serve(serveListener); err != nil && err != http.ErrServerClosed {
			zap.L().Error("prometheus metrics server terminated unexpectedly", zap.Error(err))
		}
	}()

	return nil
}
