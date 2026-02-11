package registry

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type mockResponse struct {
	resp *flex.GetCapabilitiesResponse
	err  error
}

type mockRegistryClient struct {
	mu        sync.Mutex
	responses []mockResponse
	calls     int
}

func (m *mockRegistryClient) GetCapabilities(context.Context, *flex.GetCapabilitiesRequest, ...grpc.CallOption) (*flex.GetCapabilitiesResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.calls++
	idx := m.calls - 1
	if idx < len(m.responses) {
		return m.responses[idx].resp, m.responses[idx].err
	}
	if len(m.responses) > 0 {
		last := m.responses[len(m.responses)-1]
		return last.resp, last.err
	}
	return nil, fmt.Errorf("no response configured")
}

func (m *mockRegistryClient) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func newCapabilities(binary string, start time.Time) *flex.GetCapabilitiesResponse {
	return &flex.GetCapabilitiesResponse{
		BuildInfo: &flex.BuildInfo{
			BinaryName: binary,
			Version:    "test",
			Commit:     "test",
			BuildTime:  "test",
		},
		Features:       map[string]*flex.Feature{},
		StartTimestamp: timestamppb.New(start),
	}
}

func registryClientFunc(client RegistryGetter) getClientFunc {
	return func() (*RegistryGetter, error) {
		return &client, nil
	}
}

func TestNewCachedComponentRegistry(t *testing.T) {
	ctx := context.Background()
	ttl := 42 * time.Second

	tests := []struct {
		name    string
		setup   func() (getClientFunc, []CachedComponentRegistryOpt)
		wantErr error
		wantTTL time.Duration
	}{
		{
			name: "missing client",
			setup: func() (getClientFunc, []CachedComponentRegistryOpt) {
				return func() (*RegistryGetter, error) {
					return nil, fmt.Errorf("no client configured")
				}, nil
			},
			wantErr: ErrRegistryClientUnavailable,
		},
		{
			name: "unimplemented",
			setup: func() (getClientFunc, []CachedComponentRegistryOpt) {
				mock := &mockRegistryClient{
					responses: []mockResponse{{
						err: status.Error(codes.Unimplemented, "not supported"),
					}},
				}
				return registryClientFunc(mock), nil
			},
			wantErr: ErrCapabilitiesNotSupported,
		},
		{
			name: "ttl applied",
			setup: func() (getClientFunc, []CachedComponentRegistryOpt) {
				mock := &mockRegistryClient{
					responses: []mockResponse{{
						resp: newCapabilities("v1", time.Now()),
					}},
				}
				return registryClientFunc(mock), []CachedComponentRegistryOpt{WithRegistryTTL(ttl)}
			},
			wantTTL: ttl,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clientFunc, opts := tt.setup()
			reg, err := NewCachedComponentRegistry(ctx, clientFunc, opts...)

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.wantTTL, reg.ttl)
		})
	}
}

func TestCachedComponentRegistry_NoRefreshWithinTTL(t *testing.T) {
	ctx := context.Background()
	mock := &mockRegistryClient{
		responses: []mockResponse{{
			resp: newCapabilities("v1", time.Now()),
		}},
	}

	reg, err := NewCachedComponentRegistry(ctx, registryClientFunc(mock), WithRegistryTTL(time.Hour))
	if !assert.NoError(t, err) {
		return
	}

	_ = reg.GetBuildInfo(ctx)
	assert.Equal(t, 1, mock.CallCount())
}

func TestCachedComponentRegistry_RefreshAfterTTL(t *testing.T) {
	ctx := context.Background()
	start1 := time.Now().Add(-time.Minute)
	start2 := time.Now()
	mock := &mockRegistryClient{
		responses: []mockResponse{
			{resp: newCapabilities("v1", start1)},
			{resp: newCapabilities("v2", start2)},
		},
	}

	reg, err := NewCachedComponentRegistry(ctx, registryClientFunc(mock), WithRegistryTTL(10*time.Millisecond))
	if !assert.NoError(t, err) {
		return
	}

	time.Sleep(15 * time.Millisecond)
	_ = reg.GetBuildInfo(ctx)

	assert.Eventually(t, func() bool {
		return mock.CallCount() >= 2
	}, 200*time.Millisecond, 5*time.Millisecond)

	assert.Eventually(t, func() bool {
		return reg.GetBuildInfo(ctx).BinaryName == "v2"
	}, 200*time.Millisecond, 5*time.Millisecond)
}

func TestCachedComponentRegistry_RefreshOnError(t *testing.T) {
	ctx := context.Background()
	start1 := time.Now().Add(-time.Minute)
	start2 := time.Now()
	mock := &mockRegistryClient{
		responses: []mockResponse{
			{resp: newCapabilities("v1", start1)},
			{err: status.Error(codes.Unavailable, "temporary")},
			{resp: newCapabilities("v2", start2)},
		},
	}

	reg, lastErr := NewCachedComponentRegistry(ctx, registryClientFunc(mock), WithRegistryTTL(10*time.Millisecond))
	if !assert.NoError(t, lastErr) {
		return
	}

	time.Sleep(15 * time.Millisecond)
	_ = reg.GetBuildInfo(ctx)

	assert.Eventually(t, func() bool {
		reg.mu.RLock()
		lastErr := reg.lastErr
		reg.mu.RUnlock()
		return lastErr != nil
	}, 200*time.Millisecond, 5*time.Millisecond)

	_ = reg.GetBuildInfo(ctx)
	assert.Equal(t, 3, mock.CallCount())
	reg.mu.RLock()
	lastErr = reg.lastErr
	reg.mu.RUnlock()
	assert.Nil(t, lastErr)
	assert.Equal(t, "v2", reg.GetBuildInfo(ctx).BinaryName)
}

func TestCachedComponentRegistry_RequireFeatures(t *testing.T) {
	ctx := context.Background()
	newRegistry := func() (*CachedComponentRegistry, *mockRegistryClient, error) {
		mock := &mockRegistryClient{
			responses: []mockResponse{{
				resp: &flex.GetCapabilitiesResponse{
					BuildInfo:      &flex.BuildInfo{BinaryName: "v1", Version: "test", Commit: "test", BuildTime: "test"},
					Features:       map[string]*flex.Feature{"nested": {SubFeature: map[string]*flex.Feature{"child": {SubFeature: map[string]*flex.Feature{"grandchild": nil}}}}, "used": nil},
					StartTimestamp: timestamppb.New(time.Now()),
				},
			}},
		}

		reg, err := NewCachedComponentRegistry(ctx, registryClientFunc(mock), WithSkipInit())
		return reg, mock, err
	}

	required := map[string]*flex.Feature{"nested": {SubFeature: map[string]*flex.Feature{"child": {SubFeature: map[string]*flex.Feature{"grandchild": nil}}}}, "used": nil}
	requiredMissingChild := map[string]*flex.Feature{"nested": {SubFeature: map[string]*flex.Feature{"absent": nil}}}
	requiredMissingGrandchild := map[string]*flex.Feature{"nested": {SubFeature: map[string]*flex.Feature{"child": {SubFeature: map[string]*flex.Feature{"absent": nil}}}}}
	tests := []struct {
		name      string
		required  map[string]*flex.Feature
		wantErr   bool
		wantErrIs error
	}{
		{name: "supported", required: required},
		{name: "missing child", required: requiredMissingChild, wantErr: true},
		{name: "missing grandchild", required: requiredMissingGrandchild, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reg, mock, err := newRegistry()
			if !assert.NoError(t, err) {
				return
			}

			err = reg.RequireFeatures(ctx, test.required)
			if test.wantErr {
				assert.ErrorIs(t, err, ErrUnsupportedFeature)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, 1, mock.CallCount())
		})
	}

}

func TestCachedComponentRegistry_RequireFeature(t *testing.T) {
	ctx := context.Background()
	newRegistry := func() (*CachedComponentRegistry, *mockRegistryClient, error) {
		mock := &mockRegistryClient{
			responses: []mockResponse{{
				resp: &flex.GetCapabilitiesResponse{
					BuildInfo:      &flex.BuildInfo{BinaryName: "v1", Version: "test", Commit: "test", BuildTime: "test"},
					Features:       map[string]*flex.Feature{"plain": nil, "nested": {SubFeature: map[string]*flex.Feature{"child": {SubFeature: map[string]*flex.Feature{"grandchild": nil}}}}},
					StartTimestamp: timestamppb.New(time.Now()),
				},
			}},
		}
		reg, err := NewCachedComponentRegistry(ctx, registryClientFunc(mock), WithSkipInit())
		return reg, mock, err
	}

	tests := []struct {
		name    string
		feature string
		sub     []string
		wantErr bool
	}{
		{name: "supported", feature: "plain"},
		{name: "supported child", feature: "nested", sub: []string{"child"}},
		{name: "supported grandchild", feature: "nested", sub: []string{"child", "grandchild"}},
		{name: "missing", feature: "missing", wantErr: true},
		{name: "missing child", feature: "nested", sub: []string{"absent"}, wantErr: true},
		{name: "missing grandchild", feature: "nested", sub: []string{"child", "absent"}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reg, mock, err := newRegistry()
			if !assert.NoError(t, err) {
				return
			}

			err = reg.RequireFeature(ctx, test.feature, test.sub...)
			if test.wantErr {
				assert.ErrorIs(t, err, ErrUnsupportedFeature)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, 1, mock.CallCount())
		})
	}
}
