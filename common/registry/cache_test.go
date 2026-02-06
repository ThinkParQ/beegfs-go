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

func registryClient(client RegistryComponent) *RegistryComponent {
	return &client
}

func TestNewCachedComponentRegistry(t *testing.T) {
	ctx := context.Background()
	ttl := 42 * time.Second

	tests := []struct {
		name    string
		setup   func() (*RegistryComponent, []CachedComponentRegistryOpt)
		wantErr string
		wantTTL time.Duration
	}{
		{
			name: "missing client",
			setup: func() (*RegistryComponent, []CachedComponentRegistryOpt) {
				return nil, nil
			},
			wantErr: "either client or WithGetClientFunc needs to be defined",
		},
		{
			name: "unimplemented",
			setup: func() (*RegistryComponent, []CachedComponentRegistryOpt) {
				mock := &mockRegistryClient{
					responses: []mockResponse{{
						err: status.Error(codes.Unimplemented, "not supported"),
					}},
				}
				return registryClient(mock), nil
			},
			wantErr: "component registry not implemented",
		},
		{
			name: "ttl applied",
			setup: func() (*RegistryComponent, []CachedComponentRegistryOpt) {
				mock := &mockRegistryClient{
					responses: []mockResponse{{
						resp: newCapabilities("v1", time.Now()),
					}},
				}
				return registryClient(mock), []CachedComponentRegistryOpt{WithRegistryTTL(ttl)}
			},
			wantTTL: ttl,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, opts := tt.setup()
			reg, err := NewCachedComponentRegistry(ctx, client, opts...)
			if tt.wantErr != "" {
				if assert.Error(t, err) {
					assert.Contains(t, err.Error(), tt.wantErr)
				}
				return
			}

			if !assert.NoError(t, err) {
				return
			}
			if tt.wantTTL > 0 {
				assert.Equal(t, tt.wantTTL, reg.ttl)
			}
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

	reg, err := NewCachedComponentRegistry(ctx, registryClient(mock), WithRegistryTTL(time.Hour))
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

	reg, err := NewCachedComponentRegistry(ctx, registryClient(mock), WithRegistryTTL(10*time.Millisecond))
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

	reg, err := NewCachedComponentRegistry(ctx, registryClient(mock), WithRegistryTTL(10*time.Millisecond))
	if !assert.NoError(t, err) {
		return
	}

	time.Sleep(15 * time.Millisecond)
	_ = reg.GetBuildInfo(ctx)

	assert.Eventually(t, func() bool {
		reg.mu.RLock()
		err := reg.lastErr
		reg.mu.RUnlock()
		return err != nil
	}, 200*time.Millisecond, 5*time.Millisecond)

	_ = reg.GetBuildInfo(ctx)
	assert.Equal(t, 3, mock.CallCount())
	reg.mu.RLock()
	err = reg.lastErr
	reg.mu.RUnlock()
	assert.Nil(t, err)
	assert.Equal(t, "v2", reg.GetBuildInfo(ctx).BinaryName)
}

func TestCachedComponentRegistry_RequireFeatures(t *testing.T) {
	ctx := context.Background()

	available := map[string]*flex.Feature{
		"nested": {
			SubFeature: map[string]*flex.Feature{
				"child": {},
			},
		},
		"used": nil,
	}

	required := map[string]*flex.Feature{
		"nested": {
			SubFeature: map[string]*flex.Feature{
				"child": {},
			},
		},
		"used": nil,
	}

	requiredMissing := map[string]*flex.Feature{
		"nested": {
			SubFeature: map[string]*flex.Feature{
				"absent": {},
			},
		},
	}

	newRegistry := func() (*CachedComponentRegistry, *mockRegistryClient, error) {
		mock := &mockRegistryClient{
			responses: []mockResponse{{
				resp: &flex.GetCapabilitiesResponse{
					BuildInfo: &flex.BuildInfo{
						BinaryName: "v1",
						Version:    "test",
						Commit:     "test",
						BuildTime:  "test",
					},
					Features:       available,
					StartTimestamp: timestamppb.New(time.Now()),
				},
			}},
		}

		reg, err := NewCachedComponentRegistry(ctx, nil,
			WithSkipInit(),
			WithGetClientFunc(func() (*RegistryComponent, error) {
				return registryClient(mock), nil
			}),
		)
		return reg, mock, err
	}

	t.Run("supported", func(t *testing.T) {
		reg, mock, err := newRegistry()
		if !assert.NoError(t, err) {
			return
		}

		err = reg.RequireFeatures(ctx, required)
		assert.NoError(t, err)
		assert.Equal(t, 1, mock.CallCount())
	})

	t.Run("missing", func(t *testing.T) {
		reg, mock, err := newRegistry()
		if !assert.NoError(t, err) {
			return
		}

		err = reg.RequireFeatures(ctx, requiredMissing)
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedFeature)
		assert.Equal(t, 1, mock.CallCount())
	})
}

func TestCachedComponentRegistry_RequireFeature(t *testing.T) {
	ctx := context.Background()

	available := map[string]*flex.Feature{
		"plain": nil,
		"nested": {
			SubFeature: map[string]*flex.Feature{
				"child": {},
			},
		},
	}

	newRegistry := func() (*CachedComponentRegistry, *mockRegistryClient, error) {
		mock := &mockRegistryClient{
			responses: []mockResponse{{
				resp: &flex.GetCapabilitiesResponse{
					BuildInfo: &flex.BuildInfo{
						BinaryName: "v1",
						Version:    "test",
						Commit:     "test",
						BuildTime:  "test",
					},
					Features:       available,
					StartTimestamp: timestamppb.New(time.Now()),
				},
			}},
		}

		reg, err := NewCachedComponentRegistry(ctx, nil,
			WithSkipInit(),
			WithGetClientFunc(func() (*RegistryComponent, error) {
				return registryClient(mock), nil
			}),
		)
		return reg, mock, err
	}

	t.Run("supported", func(t *testing.T) {
		reg, mock, err := newRegistry()
		if !assert.NoError(t, err) {
			return
		}

		err = reg.RequireFeature(ctx, "plain")
		assert.NoError(t, err)
		assert.Equal(t, 1, mock.CallCount())
	})

	t.Run("supported-subfeature", func(t *testing.T) {
		reg, mock, err := newRegistry()
		if !assert.NoError(t, err) {
			return
		}

		err = reg.RequireFeature(ctx, "nested", "child")
		assert.NoError(t, err)
		assert.Equal(t, 1, mock.CallCount())
	})

	t.Run("missing", func(t *testing.T) {
		reg, mock, err := newRegistry()
		if !assert.NoError(t, err) {
			return
		}

		err = reg.RequireFeature(ctx, "missing")
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedFeature)
		assert.Equal(t, 1, mock.CallCount())
	})

	t.Run("missing-subfeature", func(t *testing.T) {
		reg, mock, err := newRegistry()
		if !assert.NoError(t, err) {
			return
		}

		err = reg.RequireFeature(ctx, "nested", "absent")
		assert.Error(t, err)
		assert.ErrorIs(t, err, ErrUnsupportedFeature)
		assert.Equal(t, 1, mock.CallCount())
	})
}
