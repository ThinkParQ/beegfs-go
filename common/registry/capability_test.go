package registry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/protobuf/go/flex"
)

func TestRequireFeatureSupportMatrix(t *testing.T) {
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
			err := registry.RequireFeature(test.feature, test.subs...)
			if test.want {
				assert.NoError(t, err, "RequireFeature(%q, %v) returned error: %v", test.feature, test.subs, err)
				return
			}
			assert.Error(t, err, "RequireFeature(%q, %v) = nil, want error", test.feature, test.subs)
			assert.ErrorIs(t, err, ErrUnsupportedFeature, "RequireFeature(%q, %v) = %v, want ErrUnsupportedFeature", test.feature, test.subs, err)
		})
	}
}

func TestRequireFeature(t *testing.T) {
	registry := ComponentRegistry{
		buildInfo: &flex.BuildInfo{BinaryName: "bee", Version: "1.2.3"},
		features:  map[string]*flex.Feature{"supported": {}},
	}

	err := registry.RequireFeature("supported")
	assert.NoError(t, err, "RequireFeature(supported) returned error: %v", err)

	err = registry.RequireFeature("missing")
	assert.Error(t, err, "RequireFeature(missing) = nil, want error")
	assert.ErrorIs(t, err, ErrUnsupportedFeature, "RequireFeature(missing) = %v, want ErrUnsupportedFeature", err)
}

func TestRequireFeatures(t *testing.T) {
	available := map[string]*flex.Feature{
		"nested": {
			SubFeature: map[string]*flex.Feature{
				"child": {},
			},
		},
		"used":   nil,
		"unused": nil,
	}

	registry := ComponentRegistry{features: available}
	required := map[string]*flex.Feature{
		"nested": {
			SubFeature: map[string]*flex.Feature{
				"child": {},
			},
		},
		"used": nil,
	}
	err := registry.RequireFeatures(required)
	assert.NoError(t, err, "RequireFeatures(valid) returned error: %v", err)

	registry.buildInfo = &flex.BuildInfo{BinaryName: "bee", Version: "1.2.3"}
	requiredMissing := map[string]*flex.Feature{
		"nested": {
			SubFeature: map[string]*flex.Feature{
				"absent": {},
			},
		},
	}
	err = registry.RequireFeatures(requiredMissing)
	assert.Error(t, err, "RequireFeatures(missing) = nil, want error")
	assert.ErrorIs(t, err, ErrUnsupportedFeature, "RequireFeatures(missing) = %v, want ErrUnsupportedFeature", err)
}

func TestNewComponentRegistryCopiesFeatures(t *testing.T) {
	buildInfo := &flex.BuildInfo{BinaryName: "bee", Version: "1.2.3"}
	features := map[string]*flex.Feature{
		"plain": nil,
		"nested": {
			SubFeature: map[string]*flex.Feature{
				"child": {},
			},
		},
	}

	var reg *ComponentRegistry
	assert.NotPanics(t, func() {
		reg = NewComponentRegistry(buildInfo, features)
	})
	if !assert.NotNil(t, reg) {
		return
	}

	assert.NotNil(t, reg.features)
	assert.Len(t, reg.features, len(features))
	assert.NotSame(t, buildInfo, reg.buildInfo)
	assert.Nil(t, reg.features["plain"])
	assert.NotSame(t, features["nested"], reg.features["nested"])

	features["nested"].SubFeature["child"] = &flex.Feature{
		SubFeature: map[string]*flex.Feature{"grandchild": {}},
	}
	_, ok := reg.features["nested"].GetSubFeature()["grandchild"]
	assert.False(t, ok, "registry should clone feature data instead of sharing references")
}
