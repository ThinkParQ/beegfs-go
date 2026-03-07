/*
Package subscribermgr provides functionality for managing multiple subscribers.

It accepts a JSON configuration with one or more subscribers, then creates a handler for each subscriber.
It also adds cursors to the metaEventBuffer so each subscriber has their own unique window into the shared ring buffer space.
When the app is shutting down is also coordinates shutting down all handlers.
*/
package subscribermgr

import (
	"context"
	"fmt"
	"path"
	"reflect"
	"sync"

	"github.com/thinkparq/beegfs-go/common/configmgr"
	"github.com/thinkparq/beegfs-go/watch/internal/subscriber"
	"github.com/thinkparq/beegfs-go/watch/internal/types"
	"go.uber.org/zap"
)

type Manager struct {
	log             *zap.Logger
	handlers        []*Handler
	metaEventBuffer *types.MultiCursorRingBuffer
	wg              *sync.WaitGroup
}

// Verify all interfaces that depend on Manager are satisfied:
var _ configmgr.Listener = &Manager{}

func New(log *zap.Logger, metaEventBuffer *types.MultiCursorRingBuffer, wg *sync.WaitGroup) *Manager {

	log = log.With(zap.String("component", path.Base(reflect.TypeFor[Manager]().PkgPath())))
	return &Manager{
		log:             log,
		handlers:        make([]*Handler, 0),
		metaEventBuffer: metaEventBuffer,
		wg:              wg,
	}
}

// AppConfig must implement this interface so we can extract only the part of
// the configuration applicable to this component in UpdateConfiguration()
// without requiring a cyclical import of AppConfig into this component.
type Configurer interface {
	GetSMConfig() (HandlerConfig, []subscriber.Config)
}

// UpdateConfiguration satisfies the ConfigListener interface and is used to
// dynamically update the handlers and subscribers they manage. The provided
// configuration should contain all subscribers including any changes to
// existing ones. Any subscribers found in the old configuration but not in the
// new will be removed.
func (sm *Manager) UpdateConfiguration(newConfig any) error {

	// Use type assertion to verify the newConfig interface variable is of the
	// correct type so we can use it to get the new configuration.
	configurer, ok := newConfig.(Configurer)
	if !ok {
		return fmt.Errorf("unable to get handler and subscriber configuration from the application configuration (most likely this indicates a bug and a report should be filed)")
	}
	newHandlerConfig, newSubscribersConfig := configurer.GetSMConfig()

	// Make sure the new subscriber configuration is valid before we start making changes.
	newSubscribers, err := subscriber.NewSubscribersFromConfig(newSubscribersConfig)
	if err != nil {
		return err
	}

	toAdd, toRemove, toVerify := evaluateAddedAndRemovedSubscribers(sm.handlers, newSubscribers)
	noUpdates := true

	// We handle removing subscribers by keeping track of only those we want to keep.
	var handlersToKeep []*Handler
	for i, h := range sm.handlers {
		if toRemove[h.ID] {
			sm.log.Info("removing subscriber", zap.Int("id", h.ID))
			h.Stop()
			h.mu.Lock() // Lock the handler so we're sure its not still in use before deleting it.
			// We only want to remove cursors when subscribers are removed.
			// This ensures we don't drop events while updating subscriber config.
			sm.metaEventBuffer.RemoveCursor(h.ID)
		} else {
			// Check if the subscriber or handler configuration was modified.
			// With the current handler configuration it is likely we could swap
			// out the configuration without restarting the handler. However
			// we don't know what could get added in the future so we'll always
			// stop the handler to be safe.
			if h.Subscriber.Config != toVerify[h.ID].Config || h.config != newHandlerConfig {
				noUpdates = false
				sm.log.Info("updating subscriber configuration", zap.Int("id", h.ID))
				h.Stop()
				h.mu.Lock() // Lock the handler so we're sure its not still in use before swapping it out.
				// We don't need to unlock the mutex because it won't exist after we perform the swap.
				// Swap out the handler. Note is important we don't just swap out the subscriber.
				// Otherwise we'd have to worry about resetting the context and other state.
				sm.handlers[i] = newHandler(sm.log, toVerify[h.ID], sm.metaEventBuffer, newHandlerConfig)
				go sm.handlers[i].Handle(sm.wg)
			}
			handlersToKeep = append(handlersToKeep, sm.handlers[i])
		}
	}
	// Update handlers with only those we want to keep.
	sm.handlers = handlersToKeep

	for _, v := range toAdd {
		sm.log.Info("adding subscriber", zap.Int("id", v.ID))
		h := newHandler(sm.log, v, sm.metaEventBuffer, newHandlerConfig)
		sm.handlers = append(sm.handlers, h)
		go h.Handle(sm.wg)
	}

	if len(toAdd) == 0 && len(toRemove) == 0 && noUpdates {
		sm.log.Debug("no change to subscriber configuration")
	}

	return nil
}

// evaluateAddedAndRemovedSubscribers takes a slice of current subscriber
// handlers and a slice of new subscriber configuration. It returns a slice of
// IDs that need new handlers, and a slice of IDs that no longer need handlers.
// It also returns a slice of IDs that weren't added or removed that should be
// checked for configuration updates.
func evaluateAddedAndRemovedSubscribers(current []*Handler, new []*subscriber.Subscriber) (toAdd map[int]*subscriber.Subscriber, toRemove map[int]bool, toVerify map[int]*subscriber.Subscriber) {

	toAdd = make(map[int]*subscriber.Subscriber)
	toRemove = make(map[int]bool)
	toVerify = make(map[int]*subscriber.Subscriber)

	currentMap := make(map[int]bool)
	newMap := make(map[int]bool)

	for _, c := range current {
		currentMap[c.ID] = true
	}

	for _, n := range new {
		if !currentMap[n.ID] {
			toAdd[n.ID] = n
		} else {
			toVerify[n.ID] = n
		}
		newMap[n.ID] = true
	}

	for _, c := range current {
		if !newMap[c.ID] {
			toRemove[c.ID] = true
		}
	}

	return toAdd, toRemove, toVerify
}

// Manage handles shutting down all subscribers when the app is shutting down.
// We set things up this way so we can use different contexts for the overall
// manager versus individual subscribers. This allows us to selectively shutdown
// all or individual subscribers as needed.
func (sm *Manager) Manage(ctx context.Context, wg *sync.WaitGroup) {

	defer wg.Done()

	<-ctx.Done()
	sm.log.Info("shutting down subscriber handlers because the app is shutting down")
	for _, h := range sm.handlers {
		h.Stop()
	}

}
