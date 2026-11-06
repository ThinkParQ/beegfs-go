package rst

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

const JobBuilderRstId = 0

// ProviderLookup finds the provider of an RST by its ID. The job builder takes one, so it can run
// against the live ClientStore in Remote and Sync, or against a fixed ProviderMap in the CLI and in
// tests.
type ProviderLookup interface {
	Get(id uint32) (Provider, bool)
}

// ProviderMap is a fixed set of providers keyed by RST ID. It never changes after it is built, so
// it needs no lock.
type ProviderMap map[uint32]Provider

func (m ProviderMap) Get(id uint32) (Provider, bool) {
	p, ok := m[id]
	return p, ok
}

// ClientStore is a thread safe wrapper around a map of RST IDs to their clients. Remote and Sync
// each hold one, and Remote pushes every change of its RST configuration to it through
// UpdateConfig while the service runs.
type ClientStore struct {
	clients map[uint32]Provider
	// configs holds the configuration each provider in clients was built from, normalized by
	// NormalizeConfig. The store compares against these rather than asking a provider for its
	// config, because a provider may hold its config in another form, and the mock provider answers
	// GetConfig only when a test set an expectation for it. The job builder has no entry.
	configs    map[uint32]*flex.RemoteStorageTarget
	mu         sync.RWMutex
	mountPoint filesystem.Provider
	// configured is set by the first successful UpdateConfig. From then on the state root is fixed.
	configured bool
	// stateRoot is the value the first UpdateConfig was given. It is kept so a later config can be
	// checked against it, not because anything here reads it back.
	stateRoot string
}

var _ ProviderLookup = &ClientStore{}

func NewClientStore(mountPoint filesystem.Provider) *ClientStore {
	return &ClientStore{
		clients:    make(map[uint32]Provider),
		configs:    make(map[uint32]*flex.RemoteStorageTarget),
		mountPoint: mountPoint,
	}
}

// Get looks up and returns the client for the associated RST if one exists, or false. Callers
// should not store the reference to the client as old clients are not invalidated when they are
// deleted or their configuration updated. Instead callers should always use Get() to obtain the
// latest client whenever they need to make a new series of requests to an RST. If a caller receives
// an error using a client it should not keep retrying with the old client but rather call Get()
// again to ensure it is working with the latest version. This ensures clients are thread-safe
// while minimizing the logic required in the ClientStore and caller.
func (s *ClientStore) Get(id uint32) (Provider, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.clients[id]
	return c, ok
}

// All returns the configuration of every RST the store holds, sorted by ID. It leaves out the job
// builder, which is not configured by anyone. The configurations are shared with the store, so
// callers must not modify them.
func (s *ClientStore) All() []*flex.RemoteStorageTarget {
	s.mu.RLock()
	defer s.mu.RUnlock()
	configs := make([]*flex.RemoteStorageTarget, 0, len(s.configs))
	for _, c := range s.configs {
		configs = append(configs, c)
	}
	sort.Slice(configs, func(i, j int) bool { return configs[i].GetId() < configs[j].GetId() })
	return configs
}

// UpdateConfig replaces the set of RSTs the store holds with rstConfigs. Remote calls it on startup
// and after every change an admin makes. Sync calls it each time Remote pushes its configuration. A
// context is required because building a provider may reach out to an external service, and the
// caller may need to cancel that.
//
// rstConfigs is the whole set, not a change. The store compares it with what it holds:
//   - A provider whose configuration is unchanged is kept, so work running on it is not disturbed.
//   - A provider whose configuration changed is rebuilt. Work already running keeps the old
//     provider until it finishes, as the Get() contract allows.
//   - An RST missing from rstConfigs is dropped.
//   - The job builder (ID 0) is created on the first call and kept after that. It looks providers
//     up through the store, so it always sees the current set.
//
// Every new provider is built before anything is swapped. If one fails to build, or the set is
// invalid, the store keeps what it held and returns an error.
//
// The state root cannot change after the first call, because open bulk operations keep their state
// under it. A different one returns ErrConfigUpdateNotAllowed.
func (s *ClientStore) UpdateConfig(ctx context.Context, rstConfigs []*flex.RemoteStorageTarget, stateRoot string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.configured && stateRoot != s.stateRoot {
		return fmt.Errorf("%w: the state root cannot change from %q to %q while bulk operations may be open", ErrConfigUpdateNotAllowed, s.stateRoot, stateRoot)
	}

	next := make(map[uint32]Provider, len(rstConfigs)+1)
	nextConfigs := make(map[uint32]*flex.RemoteStorageTarget, len(rstConfigs))
	for _, config := range rstConfigs {
		config = NormalizeConfig(config)
		id := config.GetId()
		if id == JobBuilderRstId {
			return fmt.Errorf("remote storage target ID %d is reserved for the job builder", JobBuilderRstId)
		}
		if _, ok := next[id]; ok {
			return fmt.Errorf("found multiple remote storage targets with the same ID: %d", id)
		}
		nextConfigs[id] = config
		if existing, ok := s.clients[id]; ok && proto.Equal(s.configs[id], config) {
			next[id] = existing
			nextConfigs[id] = s.configs[id]
			continue
		}
		// New keeps the config it is given, so the provider gets a copy of its own and the stored
		// copy cannot be changed through it.
		provider, err := New(ctx, proto.Clone(config).(*flex.RemoteStorageTarget), s.mountPoint, stateRoot)
		if err != nil {
			return fmt.Errorf("unable to set up remote storage target %d: %w", id, err)
		}
		next[id] = provider
	}

	if builder, ok := s.clients[JobBuilderRstId]; ok {
		next[JobBuilderRstId] = builder
	} else {
		next[JobBuilderRstId] = NewJobBuilderClient(ctx, s, s.mountPoint, stateRoot)
	}

	s.clients = next
	s.configs = nextConfigs
	s.stateRoot = stateRoot
	s.configured = true
	return nil
}

// Holds reports whether the store already holds exactly rstConfigs and stateRoot, compared the way
// UpdateConfig compares them. So it is true when UpdateConfig would change nothing. A store that was
// never configured holds nothing.
func (s *ClientStore) Holds(rstConfigs []*flex.RemoteStorageTarget, stateRoot string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.configured || stateRoot != s.stateRoot || len(rstConfigs) != len(s.configs) {
		return false
	}
	for _, config := range rstConfigs {
		held, ok := s.configs[config.GetId()]
		if !ok || !proto.Equal(held, NormalizeConfig(config)) {
			return false
		}
	}
	return true
}

// NormalizeConfig returns a copy of config with the defaults New would apply. Two configs that
// differ only in what New fills in compare equal once both are normalized.
// New sets an empty
// Policies message when none is given and keeps the config it was given. Comparing an incoming
// config with a provider's config therefore needs the same default, or every config without
// policies would look changed. The copy also keeps the provider from sharing the caller's message.
func NormalizeConfig(config *flex.RemoteStorageTarget) *flex.RemoteStorageTarget {
	config = proto.Clone(config).(*flex.RemoteStorageTarget)
	if config.Policies == nil {
		config.SetPolicies(&flex.RemoteStorageTarget_Policies{})
	}
	return config
}

// ValidateTarget checks the effective configuration of a target before Remote stores it. Every way
// a target enters Remote's database calls it: create, modify, a backend change that reaches the
// target, and the import from the TOML file. So a target meets the same rules however it arrives.
// effective has every inherited value filled in (see Resolve), because the provider is built from
// it. Rules:
//   - Its type is a registered TargetType. Mock is refused: its provider panics on any call it was
//     not told to expect, so a Mock target would crash the first sync node that ran work for it.
//     Tests opt in with AllowMockTargetsForTesting.
//   - Its name, when it has one, passes CheckName. The name is optional, because nothing looks a
//     target up by it: jobs, stubs and every command name a target by its ID. A backend's name is
//     required, because it is the key its targets refer to it by.
//   - It has every setting its type requires (CheckRequired). No provider checks these.
//   - Its ID is not the job builder's.
//   - A provider builds from it. New builds one, and it is discarded. No constructor contacts the
//     RST, so this is safe before the configuration is stored. effective is not modified.
func ValidateTarget(ctx context.Context, effective *flex.RemoteStorageTarget, mountPoint filesystem.Provider, stateRoot string) error {
	_, registered := TargetTypeOf(effective)
	allowedMock := effective.WhichType() == flex.RemoteStorageTarget_Mock_case && mockTargetsAllowed.Load()
	if !registered && !allowedMock {
		return fmt.Errorf("the %s target type cannot be configured, because no client supports it", TypeName(effective))
	}
	if name := effective.GetName(); name != "" {
		if err := CheckName("remote storage target", name); err != nil {
			return err
		}
	}
	if err := CheckRequired(effective); err != nil {
		return err
	}
	if effective.GetId() == JobBuilderRstId {
		return fmt.Errorf("remote storage target ID %d is reserved for the job builder", JobBuilderRstId)
	}
	_, err := New(ctx, NormalizeConfig(effective), mountPoint, stateRoot)
	return err
}

// mockTargetsAllowed lets ValidateTarget accept Mock targets. Only AllowMockTargetsForTesting sets
// it.
var mockTargetsAllowed atomic.Bool

// AllowMockTargetsForTesting lets ValidateTarget accept Mock targets until the returned function is
// called. Only tests call it, to drive Remote's target management with providers whose calls they
// set up.
func AllowMockTargetsForTesting() (restore func()) {
	mockTargetsAllowed.Store(true)
	return func() { mockTargetsAllowed.Store(false) }
}

// SetMockClientForTesting allows the caller to directly add mock client(s) to the internal map so
// expectations can be setup externally for testing. Any provider is accepted, so a test can also
// wrap a provider the store built.
func (s *ClientStore) SetMockClientForTesting(id uint32, client Provider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[id] = client
}

// RemoveForTesting drops the client of id, as if its RST had been removed from the configuration.
func (s *ClientStore) RemoveForTesting(id uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, id)
	delete(s.configs, id)
}
