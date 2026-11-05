/*
Package subscribermgr provides functionality for managing multiple subscribers.

It accepts a JSON configuration with one or more subscribers, then creates a handler for each
subscriber on each meta's event buffer. It also adds a cursor for each subscriber to each event
buffer so each subscriber has their own unique window into that buffer's ring space.
When the app is shutting down is also coordinates shutting down all handlers.
*/
package subscribermgr

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"slices"
	"sync"

	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/watch/internal/subscriber"
	"github.com/thinkparq/beegfs-go/watch/internal/types"
	"go.uber.org/zap"
)

type Manager struct {
	log *logger.Logger
	wg  *sync.WaitGroup
	// mu guards the fields below.
	mu            sync.Mutex
	handlerConfig HandlerConfig
	// subscribers is the subscriber configuration last applied.
	subscribers []subscriber.Config
	buffers     []*types.MultiCursorRingBuffer
	// handlers has one handler per subscriber and meta. Each one sends that meta's events to that
	// subscriber.
	handlers map[subscription]*Handler
}

// subscription is one subscriber receiving the events of one meta.
type subscription struct {
	subscriberID int
	buffer       *types.MultiCursorRingBuffer
}

// Verify all interfaces that depend on Manager are satisfied:
var _ configmgr.Listener = &Manager{}

// New returns a manager with no subscribers and no event buffers.
func New(log *logger.Logger, wg *sync.WaitGroup) *Manager {

	log = log.With(zap.String("component", path.Base(reflect.TypeFor[Manager]().PkgPath())))
	return &Manager{
		log:      log,
		handlers: make(map[subscription]*Handler),
		wg:       wg,
	}
}

// AddEventBuffer starts serving the event buffer of one meta. Before it returns, it adds a cursor
// for every configured subscriber and starts their handlers.
func (sm *Manager) AddEventBuffer(buf *types.MultiCursorRingBuffer) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if slices.Contains(sm.buffers, buf) {
		sm.log.Error("ignoring an event buffer that was already added (most likely this indicates a bug and a report should be filed)", zap.Uint64("ringID", buf.GetRingID()))
		return
	}
	sm.buffers = append(sm.buffers, buf)
	if err := sm.startHandlers(buf, sm.subscribers); err != nil {
		// UpdateConfiguration stores only configurations that built subscribers without an error.
		sm.log.Error("unable to start subscriber handlers for a new event buffer (most likely this indicates a bug and a report should be filed)", zap.Error(err))
	}
}

// AppConfig must implement this interface so we can extract only the part of
// the configuration applicable to this component in UpdateConfiguration()
// without requiring a cyclical import of AppConfig into this component.
type Configurer interface {
	GetSMConfig() (HandlerConfig, []subscriber.Config)
}

// UpdateConfiguration satisfies the ConfigListener interface and is used to
// dynamically update the handlers and subscribers they manage on every event
// buffer. The provided configuration should contain all subscribers including
// any changes to existing ones. Any subscribers found in the old configuration
// but not in the new will be removed.
func (sm *Manager) UpdateConfiguration(newConfig any) error {

	// Use type assertion to verify the newConfig interface variable is of the
	// correct type so we can use it to get the new configuration.
	configurer, ok := newConfig.(Configurer)
	if !ok {
		return fmt.Errorf("unable to get handler and subscriber configuration from the application configuration (most likely this indicates a bug and a report should be filed)")
	}
	newHandlerConfig, newSubscribersConfig := configurer.GetSMConfig()

	// Make sure the new subscriber configuration is valid before we start making changes.
	if _, err := subscriber.NewSubscribersFromConfig(newSubscribersConfig); err != nil {
		return err
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	toAdd, toRestart, toRemove := planSubscriberChanges(sm.handlerConfig, newHandlerConfig, sm.subscribers, newSubscribersConfig)

	// Removing a subscriber removes its cursor from every buffer. Cursors are removed only here,
	// so a configuration update never drops events.
	for _, c := range toRemove {
		sm.log.Info("removing subscriber", zap.Int("id", c.ID))
		for _, buf := range sm.buffers {
			sm.stopHandler(subscription{subscriberID: c.ID, buffer: buf})
			buf.RemoveCursor(c.ID)
		}
	}

	for _, c := range toRestart {
		sm.log.Info("updating subscriber configuration", zap.Int("id", c.ID))
	}
	for _, c := range toAdd {
		sm.log.Info("adding subscriber", zap.Int("id", c.ID))
	}

	toStart := make([]subscriber.Config, 0, len(toRestart)+len(toAdd))
	toStart = append(toStart, toRestart...)
	toStart = append(toStart, toAdd...)

	sm.handlerConfig = newHandlerConfig
	sm.subscribers = newSubscribersConfig
	for _, buf := range sm.buffers {
		if err := sm.startHandlers(buf, toStart); err != nil {
			return fmt.Errorf("unable to start subscriber handlers (most likely this indicates a bug and a report should be filed): %w", err)
		}
	}

	if len(toAdd) == 0 && len(toRemove) == 0 && len(toRestart) == 0 {
		sm.log.Debug("no change to subscriber configuration")
	}

	return nil
}

// starts a handler on buf for each subscriber in configs.
func (sm *Manager) startHandlers(buf *types.MultiCursorRingBuffer, configs []subscriber.Config) error {
	subscribers, err := subscriber.NewSubscribersFromConfig(configs)
	if err != nil {
		return err
	}
	for _, s := range subscribers {
		key := subscription{subscriberID: s.ID, buffer: buf}
		sm.stopHandler(key)
		h := newHandler(sm.log.Logger, s, buf, sm.handlerConfig)
		sm.handlers[key] = h
		go h.Handle(sm.wg)
	}
	return nil
}

// stopHandler stops the handler named by key, waits until it no longer uses its buffer cursor,
// and forgets it. The caller holds sm.mu lock.
func (sm *Manager) stopHandler(key subscription) {
	h, ok := sm.handlers[key]
	if !ok {
		return
	}
	h.Stop()
	<-h.done
	delete(sm.handlers, key)
}

// planSubscriberChanges decides what a configuration update does to each subscriber. It compares
// the applied configuration with the new one by subscriber ID. It returns:
//   - toAdd: subscribers only in the new configuration.
//   - toRestart: subscribers in both whose configuration or the handler configuration changed.
//   - toRemove: subscribers only in the applied configuration.
func planSubscriberChanges(appliedHandlerConfig, newHandlerConfig HandlerConfig, applied, newConfigs []subscriber.Config) (toAdd, toRestart, toRemove []subscriber.Config) {

	appliedByID := make(map[int]subscriber.Config)
	newIDs := make(map[int]bool)

	for _, a := range applied {
		appliedByID[a.ID] = a
	}

	for _, n := range newConfigs {
		newIDs[n.ID] = true
		before, ok := appliedByID[n.ID]
		switch {
		case !ok:
			toAdd = append(toAdd, n)
		case before != n || appliedHandlerConfig != newHandlerConfig:
			toRestart = append(toRestart, n)
		}
	}

	for _, a := range applied {
		if !newIDs[a.ID] {
			toRemove = append(toRemove, a)
		}
	}

	return toAdd, toRestart, toRemove
}

// Manage handles shutting down all subscribers when the app is shutting down.
// We set things up this way so we can use different contexts for the overall
// manager versus individual subscribers. This allows us to selectively shutdown
// all or individual subscribers as needed.
func (sm *Manager) Manage(ctx context.Context, wg *sync.WaitGroup) {

	defer wg.Done()

	<-ctx.Done()
	sm.log.Info("shutting down subscriber handlers because the app is shutting down")
	sm.mu.Lock()
	defer sm.mu.Unlock()
	for _, h := range sm.handlers {
		h.Stop()
	}

}
