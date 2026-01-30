package registry

import (
	"errors"
	"strings"
	"testing"

	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

func TestIsFeatureSupported(t *testing.T) {
	registry := ComponentRegistry{
		features: map[string]*flex.Feature{
			"plain": nil,
			"nested": {
				SubFeature: map[string]*flex.Feature{
					"child": {},
				},
			},
		},
	}

	tests := []struct {
		name    string
		feature string
		subs    []string
		want    bool
	}{
		{name: "plain feature", feature: "plain", want: true},
		{name: "plain with sub", feature: "plain", subs: []string{"child"}, want: false},
		{name: "missing feature", feature: "missing", want: false},
		{name: "nested feature", feature: "nested", want: true},
		{name: "nested child", feature: "nested", subs: []string{"child"}, want: true},
		{name: "nested missing child", feature: "nested", subs: []string{"absent"}, want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := registry.IsFeatureSupported(test.feature, test.subs...); got != test.want {
				t.Fatalf("IsFeatureSupported(%q, %v) = %v, want %v", test.feature, test.subs, got, test.want)
			}
		})
	}
}

func TestRequireFeature(t *testing.T) {
	registry := ComponentRegistry{
		buildInfo: &flex.BuildInfo{BinaryName: "bee", Version: "1.2.3"},
		features:  map[string]*flex.Feature{"supported": {}},
	}

	if err := registry.RequireFeature("supported"); err != nil {
		t.Fatalf("RequireFeature(supported) returned error: %v", err)
	}

	err := registry.RequireFeature("missing")
	if err == nil {
		t.Fatalf("RequireFeature(missing) = nil, want error")
	}
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("RequireFeature(missing) = %v, want ErrUnsupportedFeature", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "bee") || !strings.Contains(msg, "1.2.3") {
		t.Fatalf("RequireFeature(missing) message %q missing build info", msg)
	}
}

func TestRequireFeatures(t *testing.T) {
	available := map[string]*flex.Feature{
		"nested": {
			SubFeature: map[string]*flex.Feature{
				"child": {},
			},
		},
	}

	registry := ComponentRegistry{features: available}
	requiredOK := map[string]*flex.Feature{
		"nested": {
			SubFeature: map[string]*flex.Feature{
				"child": {},
			},
		},
	}
	if err := registry.RequireFeatures(requiredOK); err != nil {
		t.Fatalf("RequireFeatures(valid) returned error: %v", err)
	}

	registry.buildInfo = &flex.BuildInfo{BinaryName: "bee", Version: "1.2.3"}
	requiredMissing := map[string]*flex.Feature{
		"nested": {
			SubFeature: map[string]*flex.Feature{
				"absent": {},
			},
		},
	}
	err := registry.RequireFeatures(requiredMissing)
	if err == nil {
		t.Fatalf("RequireFeatures(missing) = nil, want error")
	}
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("RequireFeatures(missing) = %v, want ErrUnsupportedFeature", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "bee") || !strings.Contains(msg, "1.2.3") {
		t.Fatalf("RequireFeatures(missing) message %q missing build info", msg)
	}
}

func TestGetCapabilitiesDeepCopy(t *testing.T) {
	registry := ComponentRegistry{
		features: map[string]*flex.Feature{
			"plain":  nil,
			"nested": {SubFeature: map[string]*flex.Feature{"child": {}}},
		},
	}

	caps := registry.GetCapabilities()

	// mutate returned map and nested feature
	delete(caps, "plain")
	caps["nested"].SubFeature["child"] = &flex.Feature{SubFeature: map[string]*flex.Feature{"grandchild": {}}}

	if _, ok := registry.features["plain"]; !ok {
		t.Fatalf("original registry.features missing plain after mutation")
	}

	originalChild := registry.features["nested"].GetSubFeature()["child"]
	if originalChild == nil || len(originalChild.GetSubFeature()) != 0 {
		t.Fatalf("original nested feature was mutated by caller")
	}
}

func TestInitComponentRegistrySuccess(t *testing.T) {
	resetRegistry()

	buildInfo := &flex.BuildInfo{BinaryName: "bee", Version: "1.2.3"}
	extraFeature := &flex.Feature{SubFeature: map[string]*flex.Feature{"child": {}}}

	err := InitComponentRegistry(buildInfo, map[string]*flex.Feature{
		"extra": extraFeature,
	}, false)
	if err != nil {
		t.Fatalf("InitComponentRegistry returned error: %v", err)
	}

	if !proto.Equal(registry.buildInfo, buildInfo) {
		t.Fatalf("registry buildInfo = %+v, want %+v", registry.buildInfo, buildInfo)
	}
	buildInfo.BinaryName = "mutated"
	if registry.buildInfo.GetBinaryName() == "mutated" {
		t.Fatalf("registry buildInfo was not cloned")
	}

	if _, ok := registry.features[FeatureFilterFiles]; !ok {
		t.Fatalf("registry missing default feature %q", FeatureFilterFiles)
	}
	if _, ok := registry.features["extra"]; !ok {
		t.Fatalf("registry missing extra feature")
	}
	extraFeature.SubFeature["child"] = &flex.Feature{SubFeature: map[string]*flex.Feature{"grand": {}}}
	if len(registry.features["extra"].GetSubFeature()) != 1 {
		t.Fatalf("registry extra feature was mutated after init")
	}
}

func TestInitComponentRegistryConflictWithoutOverwrite(t *testing.T) {
	resetRegistry()
	buildInfo := &flex.BuildInfo{BinaryName: "bee", Version: "1.2.3"}
	conflictFeature := &flex.Feature{SubFeature: map[string]*flex.Feature{"child": {}}}

	err := InitComponentRegistry(buildInfo, map[string]*flex.Feature{
		FeatureFilterFiles: conflictFeature,
	}, false)
	if err == nil {
		t.Fatalf("InitComponentRegistry conflict = nil, want error")
	}
	if !strings.Contains(err.Error(), "differs") {
		t.Fatalf("InitComponentRegistry conflict error %q missing expected text", err)
	}
	if registry.features[FeatureFilterFiles] != nil {
		t.Fatalf("conflicting feature overwrote default value")
	}
	if registry.buildInfo.GetBinaryName() != "bee" {
		t.Fatalf("buildInfo was not set before conflict")
	}
}

// resetRegistry resets package state for testing InitComponentRegistry.
func resetRegistry() {
	registry = ComponentRegistry{
		buildInfo: &flex.BuildInfo{
			BinaryName: "unknown",
			Version:    "unknown",
			Commit:     "unknown",
			BuildTime:  "unknown",
		},
		features: copyFeatures(commonFeatures),
	}
}
