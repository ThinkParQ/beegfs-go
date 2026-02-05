package registry

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type RegistryComponent interface {
	GetCapabilities(context.Context, *flex.GetCapabilitiesRequest, ...grpc.CallOption) (*flex.GetCapabilitiesResponse, error)
}

// ComponentRegistry is wrapper for component build information and capability features.
type ComponentRegistry struct {
	buildInfo *flex.BuildInfo
	features  map[string]*flex.Feature
}

// NewComponentRegistry returns a new registry with the specified build and capable features.
func NewComponentRegistry(buildInfo *flex.BuildInfo, features map[string]*flex.Feature) *ComponentRegistry {
	registry := &ComponentRegistry{
		features: make(map[string]*flex.Feature, len(features)),
	}
	if buildInfo != nil {
		registry.buildInfo = proto.Clone(buildInfo).(*flex.BuildInfo)
	}

	for key, feature := range features {
		if feature == nil {
			registry.features[key] = nil
		} else {
			registry.features[key] = proto.Clone(feature).(*flex.Feature)
		}
	}
	return registry
}

// GetComponentRegistry returns a copy of the client's registry.
func GetComponentRegistry(ctx context.Context, client RegistryComponent) (*ComponentRegistry, time.Time, error) {
	resp, err := client.GetCapabilities(ctx, &flex.GetCapabilitiesRequest{})
	if err != nil {
		return nil, time.Time{}, err
	}
	return NewComponentRegistry(resp.BuildInfo, resp.Features), resp.StartTimestamp.AsTime(), nil
}

func (r *ComponentRegistry) Copy() *ComponentRegistry {
	return NewComponentRegistry(r.buildInfo, r.features)
}

// GetBuildInfo returns the component registry's build information. If InitComponentRegistry has not
// been called this information will be unknown.
func (c *ComponentRegistry) GetBuildInfo() *flex.BuildInfo {
	if c.buildInfo == nil {
		return &flex.BuildInfo{
			BinaryName: "unknown",
			Version:    "unknown",
			Commit:     "unknown",
			BuildTime:  "unknown",
		}
	}
	return proto.Clone(c.buildInfo).(*flex.BuildInfo)
}

// GetCapabilities returns a copy of the component registry's capability features. If
// InitComponentRegistry has not been called then it will be empty.
func (c *ComponentRegistry) GetCapabilities() map[string]*flex.Feature {
	copy := make(map[string]*flex.Feature, len(c.features))
	for key, feature := range c.features {
		if feature == nil {
			copy[key] = nil
		} else {
			copy[key] = proto.Clone(feature).(*flex.Feature)
		}
	}
	return copy
}

// RequireFeature verifies the feature is supported and returns an error otherwise.
func (c *ComponentRegistry) RequireFeature(feature string, subFeatures ...string) error {
	if !c.isRequiredFeatureSupported(feature, subFeatures...) {
		name := strings.Join(append([]string{feature}, subFeatures...), "-")
		binary := "unknown"
		version := "unknown"
		if c.buildInfo != nil {
			binary = c.buildInfo.BinaryName
			version = c.buildInfo.Version
		}
		return fmt.Errorf("%s is not supported on %s %s: %w", name, binary, version, ErrUnsupportedFeature)
	}
	return nil
}

// RequireFeatures verifies the component registry has the required features.
func (c *ComponentRegistry) RequireFeatures(required map[string]*flex.Feature) error {
	if !isRequiredFeaturesSupported(c.features, required) {
		binary := "unknown"
		version := "unknown"
		if c.buildInfo != nil {
			binary = c.buildInfo.BinaryName
			version = c.buildInfo.Version
		}
		return fmt.Errorf("one or more features are not supported on %s %s: %w", binary, version, ErrUnsupportedFeature)
	}
	return nil
}

// isRequiredFeatureSupported returns whether the feature is supported by the component registry.
func (c *ComponentRegistry) isRequiredFeatureSupported(feature string, subFeatures ...string) bool {
	features, ok := c.features[feature]
	if !ok {
		return false
	}

	if features == nil {
		return len(subFeatures) == 0
	}

	for _, subFeature := range subFeatures {
		available := features.GetSubFeature()
		if available == nil {
			return false
		}
		features, ok = available[subFeature]
		if !ok {
			return false
		}
	}

	return true
}

// isRequiredFeaturesSupported returns true only if every required feature key exists in available.
func isRequiredFeaturesSupported(available map[string]*flex.Feature, required map[string]*flex.Feature) bool {
	if len(required) == 0 {
		return true
	}
	if len(available) == 0 {
		return false
	}

	for requiredKey, requiredFeature := range required {
		availableFeature, ok := available[requiredKey]
		if !ok {
			return false
		}
		if requiredFeature != nil {
			if availableFeature == nil {
				return len(requiredFeature.GetSubFeature()) == 0
			} else if !isRequiredFeaturesSupported(availableFeature.GetSubFeature(), requiredFeature.GetSubFeature()) {
				return false
			}
		}
	}
	return true
}
