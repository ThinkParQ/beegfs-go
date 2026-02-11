package registry

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type RegistryGetter interface {
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

// GetComponentRegistry returns a new client registry.
func GetComponentRegistry(ctx context.Context, client RegistryGetter) (*ComponentRegistry, time.Time, error) {
	resp, err := client.GetCapabilities(ctx, &flex.GetCapabilitiesRequest{})
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			err = ErrCapabilitiesNotSupported
		}
		return nil, time.Time{}, err
	}
	if resp == nil {
		return nil, time.Time{}, fmt.Errorf("server returned nil registry without error (this is a bug), build: unknown-unknown")
	}
	if resp.StartTimestamp == nil {
		return nil, time.Time{}, fmt.Errorf("server returned an invalid timestamp without error (this is a bug), build: %s", getBuild(resp.BuildInfo))
	}

	startTime := resp.StartTimestamp.AsTime()
	if startTime.IsZero() {
		return nil, time.Time{}, fmt.Errorf("server returned zero timestamp without error (this is a bug), build: %s", getBuild(resp.BuildInfo))
	}

	return NewComponentRegistry(resp.BuildInfo, resp.Features), startTime, nil
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
		return fmt.Errorf("%s is not supported on build %s: %w", name, c.GetBuild(), ErrUnsupportedFeature)
	}
	return nil
}

// RequireFeatures verifies the component registry has the required features.
func (c *ComponentRegistry) RequireFeatures(required map[string]*flex.Feature) error {
	if !isRequiredFeaturesSupported(c.features, required) {
		return fmt.Errorf("one or more features are not supported on build %s: %w", c.GetBuild(), ErrUnsupportedFeature)
	}
	return nil
}

// GetBuild returns a formatted string containing the build's binary name and version.
func (c *ComponentRegistry) GetBuild() string {
	return getBuild(c.buildInfo)
}

func getBuild(buildInfo *flex.BuildInfo) string {
	binary := "unknown"
	version := "unknown"
	if buildInfo != nil {
		binary = buildInfo.BinaryName
		version = buildInfo.Version
	}
	return fmt.Sprintf("%s (%s)", binary, version)
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
			}
			if !isRequiredFeaturesSupported(availableFeature.GetSubFeature(), requiredFeature.GetSubFeature()) {
				return false
			}
		}
	}
	return true
}
