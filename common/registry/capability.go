package registry

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/thinkparq/protobuf/go/beeremote"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

var ErrUnsupportedFeature = errors.New("unsupported feature")

// ComponentRegistry is wrapper for component build information and capability features.
type ComponentRegistry struct {
	buildInfo *flex.BuildInfo
	features  map[string]*flex.Feature
}

// registry maintains component build information and capabilities globally but privately and needs
// to be accessed by the exported functions.
//
// Be sure to update commonFeatures with any features that supported by all build components.
var (
	commonFeatures = map[string]*flex.Feature{
		FeatureFilterFiles: nil,
	}
	registry = ComponentRegistry{
		buildInfo: &flex.BuildInfo{
			BinaryName: "unknown",
			Version:    "unknown",
			Commit:     "unknown",
			BuildTime:  "unknown",
		},
		features: copyFeatures(commonFeatures),
	}
)

// InitComponentRegistry will initialize the component build and capabilities registry. Component
// specific features can be added using extraFeatures. Extra features that already exist in the base
// registry cause an error unless they are equal or overwrite is true.
func InitComponentRegistry(buildInfo *flex.BuildInfo, extraFeatures map[string]*flex.Feature, overwrite bool) error {
	if buildInfo != nil {
		registry.buildInfo = proto.Clone(buildInfo).(*flex.BuildInfo)
	}

	if len(extraFeatures) > 0 {
		for key, feature := range extraFeatures {
			if registryFeature, ok := registry.features[key]; ok && !overwrite {
				if (registryFeature == nil && feature == nil) || proto.Equal(registryFeature, feature) {
					continue
				}
				return fmt.Errorf("feature %q differs; set overwrite to true to replace", key)
			}
			if feature == nil {
				registry.features[key] = nil
			} else {
				registry.features[key] = proto.Clone(feature).(*flex.Feature)
			}
		}
	}
	return nil
}

// IsFeatureSupportedByRegistry verifies the feature is supported by the local registry.
func IsFeatureSupportedByRegistry(feature string, subFeatures ...string) bool {
	return registry.IsFeatureSupported(feature, subFeatures...)
}

// EnsureCapabilitiesSupportRegistry verifies the capabilities satisfy everything required by
// the local registry.
func EnsureCapabilitiesSupportRegistry(capabilities *flex.GetCapabilitiesResponse) error {
	featureRegistry := ComponentRegistry{
		buildInfo: capabilities.GetBuildInfo(),
		features:  capabilities.GetFeatures(),
	}
	return featureRegistry.RequireFeatures(registry.GetCapabilities())
}

// EnsureFeaturesSupportRegistry verifies the available features satisfy everything required by
// the local registry.
func EnsureFeaturesSupportRegistry(features map[string]*flex.Feature) error {
	featureRegistry := ComponentRegistry{features: features}
	return featureRegistry.RequireFeatures(registry.GetCapabilities())
}

// EnsureRegistrySupportsFeatures verifies the local registry includes the required features.
func EnsureRegistrySupportsFeatures(features map[string]*flex.Feature) error {
	return registry.RequireFeatures(features)
}

// GetComponentBuildInfo returns a copy of the component's build information which includes the
// binary's name, version, commit and build time. If InitComponentRegistry has not been called this
// information will be unknown.
func GetComponentBuildInfo() *flex.BuildInfo {
	return registry.GetBuildInfo()
}

// GetComponentCapabilities returns a copy of the component's capabilities. If InitComponentRegistry
// has not been called then only the build's common features will be returned.
func GetComponentCapabilities() map[string]*flex.Feature {
	return registry.GetCapabilities()
}

// GetRemoteRegistry returns a copy of the remote node's component registry.
func GetRemoteRegistry(ctx context.Context, remote beeremote.BeeRemoteClient) (*ComponentRegistry, error) {
	resp, err := remote.GetCapabilities(ctx, &beeremote.GetCapabilitiesRequest{})
	if err != nil {
		return nil, err
	}
	componentFeatures := &ComponentRegistry{
		buildInfo: resp.BuildInfo,
		features:  resp.Features,
	}
	return componentFeatures, nil
}

// RequireRemoteFeature verifies that a remote node supports the feature (and any optional
// sub-features), returning an error if capabilities can’t be determined or the required feature
// isn’t supported. If remoteRegistry is not nil then *remoteRegistry will be lazily initialized for
// future uses.
func RequireRemoteFeature(ctx context.Context, remote beeremote.BeeRemoteClient, remoteRegistry **ComponentRegistry, feature string, subFeatures ...string) error {
	if remoteRegistry == nil {
		remoteRegistry = new(*ComponentRegistry)
	}

	if *remoteRegistry == nil {
		registry, err := GetRemoteRegistry(ctx, remote)
		if err != nil {
			return fmt.Errorf("unable to determine BeeGFS remote capabilities: %w", err)
		}
		*remoteRegistry = registry
	}
	return (*remoteRegistry).RequireFeature(feature, subFeatures...)
}

// RequireRemoteFeatures verifies that a remote node supports the features, returning an error if
// capabilities can’t be determined or one or more required feature are not supported. If
// remoteRegistry is not nil then *remoteRegistry will be lazily initialized for future uses.
func RequireRemoteFeatures(ctx context.Context, remote beeremote.BeeRemoteClient, remoteRegistry **ComponentRegistry, required map[string]*flex.Feature) error {
	if remoteRegistry == nil {
		remoteRegistry = new(*ComponentRegistry)
	}

	if *remoteRegistry == nil {
		registry, err := GetRemoteRegistry(ctx, remote)
		if err != nil {
			return fmt.Errorf("unable to determine BeeGFS remote capabilities: %w", err)
		}
		*remoteRegistry = registry
	}

	return (*remoteRegistry).RequireFeatures(required)
}

// GetBuildInfo returns the component registry's build information. If InitComponentRegistry has not
// been called this information will be unknown.
func (c *ComponentRegistry) GetBuildInfo() *flex.BuildInfo {
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
	return copyFeatures(c.features)
}

// IsFeatureSupported returns whether the feature is supported by the component registry.
func (c *ComponentRegistry) IsFeatureSupported(feature string, subFeatures ...string) bool {
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

// RequireFeature verifies the feature is supported and returns an error otherwise.
func (c *ComponentRegistry) RequireFeature(feature string, subFeatures ...string) error {
	if !c.IsFeatureSupported(feature, subFeatures...) {
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

func copyFeatures(features map[string]*flex.Feature) map[string]*flex.Feature {
	copy := make(map[string]*flex.Feature, len(features))
	for key, feature := range features {
		if feature == nil {
			copy[key] = nil
		} else {
			copy[key] = proto.Clone(feature).(*flex.Feature)
		}
	}
	return copy
}
