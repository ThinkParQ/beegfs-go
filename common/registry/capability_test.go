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
					"child": {
						SubFeature: map[string]*flex.Feature{
							"grandchild": {},
						},
					},
				},
			}}}

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
		{name: "nested grandchild", feature: "nested", subs: []string{"child", "grandchild"}, want: true},
		{name: "nested missing grandchild", feature: "nested", subs: []string{"child", "missing"}, want: false},
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
	registry := ComponentRegistry{features: map[string]*flex.Feature{
		"nested": {
			SubFeature: map[string]*flex.Feature{
				"child": {
					SubFeature: map[string]*flex.Feature{
						"grandchild": nil,
					},
				},
			},
		},
		"used":   nil,
		"unused": nil,
	}}

	requiredChild := map[string]*flex.Feature{"nested": {SubFeature: map[string]*flex.Feature{"child": {}}}, "used": nil}
	requiredGrandchild := map[string]*flex.Feature{"nested": {SubFeature: map[string]*flex.Feature{"child": {SubFeature: map[string]*flex.Feature{"grandchild": {}}}}}}
	requiredMissingChild := map[string]*flex.Feature{"nested": {SubFeature: map[string]*flex.Feature{"absent": {}}}}
	requiredMissingGrandchild := map[string]*flex.Feature{"nested": {SubFeature: map[string]*flex.Feature{"child": {SubFeature: map[string]*flex.Feature{"absent": {}}}}}}
	tests := []struct {
		name     string
		required map[string]*flex.Feature
		wantErr  bool
	}{
		{name: "supported nexted", required: map[string]*flex.Feature{"nested": nil}},
		{name: "supported child", required: requiredChild},
		{name: "supported grandchild", required: requiredGrandchild},
		{name: "missing child", required: requiredMissingChild, wantErr: true},
		{name: "missing grandchild", required: requiredMissingGrandchild, wantErr: true},
		{name: "missing nested", required: map[string]*flex.Feature{"absent": nil}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := registry.RequireFeatures(test.required)
			if test.wantErr {
				assert.ErrorIs(t, err, ErrUnsupportedFeature)
				return
			}
			assert.NoError(t, err, "RequireFeatures(%s) returned error: %v", test.name, err)
		})
	}
}
