package rst

import (
	"context"
	"sync"

	"github.com/thinkparq/gobee/filesystem"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/proto"
)

// ClientStore is a thread safe wrapper around a map of RST IDs to their clients. This allows the
// clients to be reconfigured/updated after the ClientStore is initialized.
type ClientStore struct {
	clients    map[string]Provider
	mu         sync.RWMutex
	mountPoint filesystem.Provider
}

func NewClientStore(mountPoint filesystem.Provider) *ClientStore {
	return &ClientStore{
		clients:    make(map[string]Provider),
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
func (s *ClientStore) Get(id string) (Provider, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.clients[id]
	return c, ok
}

// UpdateConfig will check if the current configuration can be updated to the proposed
// configuration, and return an error if the configuration update is invalid/not allowed.
// As applying new configuration may require reaching out to a RST, a context is required
// so any remote network requests can be cancelled by the caller if needed.
//
// IMPORTANT: Currently once the initial RST config is set, it cannot be dynamically updated.
func (s *ClientStore) UpdateConfig(ctx context.Context, rstConfigs []*flex.RemoteStorageTarget) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// If there are existing RSTs, verify the configuration did not change:
	if len(s.clients) != 0 {
		// The length won't match if subscribers were removed or added.
		if len(s.clients) != len(rstConfigs) {
			return ErrConfigUpdateNotAllowed
		}
		// If the length matches then verify all the configurations match.
		found := 0
		for _, newConfig := range rstConfigs {
			existingConfig, ok := s.clients[newConfig.Id]
			if ok {
				if !proto.Equal(existingConfig.GetConfig(), newConfig) {
					return ErrConfigUpdateNotAllowed
				}
				found++
			}
		}
		// If we didn't find all of the existing RSTs in the updated config, then one or more were
		// replaced in the new configuration.
		if found != len(s.clients) {
			return ErrConfigUpdateNotAllowed
		}
	} else {
		// Otherwise configure the RSTs.
		rstMap := make(map[string]Provider)
		for _, config := range rstConfigs {
			rst, err := New(ctx, config, s.mountPoint)
			if err != nil {
				return err
			}
			rstMap[config.Id] = rst
		}
		s.clients = rstMap
	}
	return nil
}

// SetMockClientForTesting allows the caller to directly add mock client(s) to the internal map so
// expectations can be setup externally for testing.
func (s *ClientStore) SetMockClientForTesting(id string, client *MockClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[id] = client
}
