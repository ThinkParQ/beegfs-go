package telemetry

import (
	"fmt"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
)

func resolveOptions(opts []Option) options {
	o := options{
		version: "unknown",
	}
	// Default instance ID: hostname:pid
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	o.instanceID = fmt.Sprintf("%s:%d", hostname, os.Getpid())

	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// buildResource constructs the OTel Resource that identifies the service in all
// exported metrics. It merges the default resource with service-specific attributes.
func buildResource(cfg Config, opts []Option) (*resource.Resource, error) {
	o := resolveOptions(opts)

	// Use NewSchemaless to avoid schema URL conflicts when merging with
	// resource.Default(), which may use a different semconv version.
	return resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			attribute.String("service.name", cfg.ServiceName),
			attribute.String("service.instance.id", o.instanceID),
			attribute.String("service.version", o.version),
		),
	)
}
