package subscribermgr

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/thinkparq/beegfs-go/watch/internal/subscriber"
	"github.com/thinkparq/beegfs-go/watch/internal/types"
	"go.uber.org/zap"
)

// A Handler manages a the lifecycle of a connection to a single subscriber.
// It defines general connection handling semantics applicable across all subscriber types.
// Connections are attempted with an exponential backoff.
// Once connected it polls the metaEventBuffer and sends events to the subscriber as they are available.
// It also listens for responses from the subscriber and acknowledges events so buffer space can be freed.
// When the application is shutting down it handles gracefully disconnecting subscribers.
type Handler struct {
	ctx             context.Context
	cancel          context.CancelFunc
	log             *zap.Logger
	metaEventBuffer *types.MultiCursorRingBuffer
	config          HandlerConfig
	*subscriber.Subscriber
	// The mutex serves two purposes: (1) guarantee only one handler runs at a
	// time. (2) guarantee when dynamic configuration updates happen the old
	// version has finished shutting down before we swap out the handler or
	// delete it.
	mu sync.Mutex
	// lastSeqID is used to avoid sending duplicate events to subscribers after a restart. This is
	// necessary because subscribers may acknowledge the last event they received before that event
	// exists in the event buffer. Calling AckEvent() on an event that does not exist is essentially
	// a no-op and the ack cursor is unmodified meaning it cannot be used to determine the next
	// event expected by a subscriber.
	//
	// This approach avoids the need for a more complicated mechanism to collect the next event
	// expected by all subscribers and wait for the event buffer to be repopulated. Once the event
	// buffer is initialized and the subscribers ackCursor points at the correct event this field is
	// redundant but still updated for consistency in case it is useful elsewhere in the future.
	lastSeqID uint64
}

type HandlerConfig struct {
	MaxReconnectBackOff            int `mapstructure:"max-reconnect-backoff"`
	MaxWaitForResponseAfterConnect int `mapstructure:"max-wait-for-response-after-connect"`
	PollFrequency                  int `mapstructure:"poll-frequency"`
}

func newHandler(log *zap.Logger, subscriber *subscriber.Subscriber, metaEventBuffer *types.MultiCursorRingBuffer, config HandlerConfig) *Handler {
	log = log.With(zap.Int("subscriberID", subscriber.ID), zap.String("subscriberName", subscriber.Name))

	ctx, cancel := context.WithCancel(context.Background())

	// Add cursor is idempotent so always ensure there is a cursor for this subscriber.
	metaEventBuffer.AddCursor(subscriber.ID)

	return &Handler{
		ctx:             ctx,
		cancel:          cancel,
		log:             log,
		metaEventBuffer: metaEventBuffer,
		config:          config,
		Subscriber:      subscriber,
	}
}

// Handles the connection with a particular Subscriber.
// It determines the next state a subscriber should transition to in response to external and internal factors.
// It is the only place that should update the state of the subscriber.
func (h *Handler) Handle(wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	h.mu.Lock()
	defer h.mu.Unlock()

	for {
		select {
		case <-h.ctx.Done():
			h.log.Debug("successfully shutdown subscriber")
			// At this point we should already be disconnected, or disconnecting if there was an error.
			// In the future we may want to consider a mechanism if we were unable to disconnect, to retry a few times.
			// If the app is shutting down those resources should be cleaned up.
			// For now we'll just go ahead and shutdown.
			return
		default:

			// We look at the result of the last loop to tell us what needs to happen next.
			// If we're disconnected we should connect.
			// If we're connected we should start handling the connection.
			// Otherwise we presume we need to disconnect for some reason.

			if state := h.GetState(); state == subscriber.DISCONNECTED {
				h.SetState(subscriber.CONNECTING)
				if h.connectLoop() {
					h.SetState(subscriber.CONNECTED)

					// We need to start listening for responses from the subscriber before we do anything.
					// Some subscribers may block on receiving new events until they can send responses.
					// Some subscribers may also tell us the last event they successfully received so we can avoid sending duplicate events.
					doneReceiving, cancelReceive := h.receiveLoop()

					// Start sending events to this subscriber:
					doneSending, cancelSend := h.sendLoop()

					// If either the receive or send goroutines are done, we should fully disconnect:
					select {
					case <-doneReceiving:
						cancelSend()
						<-doneSending
					case <-doneSending:
						cancelReceive()
						<-doneReceiving
					case <-h.ctx.Done():
						cancelSend()
						<-doneSending
						cancelReceive()
						<-doneReceiving
					}
					h.SetState(subscriber.DISCONNECTING)
				}
			}

			// If the connection was lost for any reason, we should first disconnect before we reconnect or shutdown:
			if h.doDisconnect() {
				h.SetState(subscriber.DISCONNECTED)
			}
		}
	}
}

// doDisconnect() attempts to disconnect the subscriber.
// It only attempts once returning true on a clean disconnect and false otherwise.
// It subscribers disconnect method should be idempotent, so it can be called repeatedly.
// It should also return true if there is no active connection to this subscriber.
// It is up to the caller to determine how many times to recall doDisconnect() if it returns false.
// A new connection should not be attempted until doDisconnect() returns true.
func (h *Handler) doDisconnect() bool {
	h.log.Info("disconnecting subscriber")
	err := h.Disconnect()
	if err != nil {
		h.log.Error("encountered one or more errors disconnecting subscriber (ignoring)", zap.Error(err))
		return false
	}
	h.log.Info("disconnected subscriber")
	return true
}

// connectLoop() attempts to connect to a subscriber.
// If the subscriber is not ready or there is an error it will attempt to reconnect with an exponential backoff.
// If it returns false there was an unrecoverable error and the caller should first call doDisconnect() before reconnecting.
func (h *Handler) connectLoop() bool {
	h.log.Info("connecting to subscriber")
	var reconnectBackOff float64 = 1
	for {
		select {
		case <-h.ctx.Done():
			h.log.Info("not attempting to connect to subscriber because the handler is shutting down")
			return false
		case <-time.After(time.Second * time.Duration(reconnectBackOff)): // We use this instead of time.Ticker so we can change the duration.
			retry, err := h.Connect()
			if err != nil {
				if !retry {
					h.log.Error("unable to connect to subscriber (unable to retry)", zap.Error(err))
					return false
				}

				// We'll retry to connect with an exponential back off. We'll add some jitter to avoid load spikes.
				reconnectBackOff *= 2 + rand.Float64()
				if reconnectBackOff > float64(h.config.MaxReconnectBackOff) {
					reconnectBackOff = float64(h.config.MaxReconnectBackOff) - rand.Float64()
				}

				h.log.Warn("unable to connect to subscriber (retrying)", zap.Error(err), zap.Any("retry_in_seconds", reconnectBackOff))
				continue
			}

			h.log.Info("connected to subscriber")
			return true
		}
	}
}

func (h *Handler) receiveLoop() (<-chan struct{}, context.CancelFunc) {

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	recvStream := h.Receive()
	h.log.Info("receiving responses from subscriber")
	// When we connect, some subscribers may acknowledge the last event they successfully received before a disconnect.
	// TODO: Consider if the time we wait should be configurable based per subscriber.
	select {
	case response, ok := <-recvStream:
		if ok {
			// Note an error or a legitimate remote disconnect could result in a REMOTE_DISCONNECT.
			h.log.Info("subscriber acknowledged last event received before disconnect", zap.Any("sequenceID", response.CompletedSeq))
			err := h.metaEventBuffer.AckEvent(h.ID, response.CompletedSeq)
			if err != nil {
				// Ignore errors, an error is expected if the event buffer is not fully repopulated.
				// This is logged in case we start seeing errors unexpected under other circumstances.
				h.log.Debug("error updating subscriber ack cursor with last event before disconnect", zap.Error(err))
			}
			h.lastSeqID = response.CompletedSeq
		}
		// TODO: Test what happens if we have a REMOTE_DISCONNECT here. Are we safe to just ignore it? It would be better to explicitly handle.
	case <-time.After(time.Duration(h.config.MaxWaitForResponseAfterConnect) * time.Second):
		h.log.Info("subscriber did not acknowledge last event received before disconnect, resending events from the last known acknowledged event", zap.Any("lastSeqID", h.lastSeqID))
	}

	// Move the send cursor back to the last acknowledged event.
	// This may result in duplicate events being sent if the subscriber didn't tell us the last event they received.
	h.metaEventBuffer.ResetSendCursor(h.ID)
	loggedAckError := false

	go func() {
		defer close(done)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				h.log.Debug("stopping receiving responses because the handler is shutting down")
				return
			case response, ok := <-recvStream:
				if !ok {
					// Note an error or a legitimate remote disconnect could result in a REMOTE_DISCONNECT.
					h.log.Info("stopping receiving responses because the remote subscriber disconnected")
					return
				}
				//h.log.Debug("received response from subscriber", zap.Any("response", response))
				err := h.metaEventBuffer.AckEvent(h.ID, response.CompletedSeq)
				if err != nil {
					if !loggedAckError {
						// Ignore errors, an error is expected if the event buffer is not fully
						// repopulated, or if the subscriber is setup to acknowledge the most recent
						// event on a timer and all sent events are already acknowledged. This is
						// logged to avoid masking errors in other circumstances.
						h.log.Debug("unable to acknowledge the specified sequence ID (further occurrences will not be logged until this operation succeeds)", zap.Error(err), zap.String("hint", "if Watch just started this likely indicates the event buffer is not fully repopulated yet"))
						loggedAckError = true
					}
				} else {
					loggedAckError = false
				}
				h.lastSeqID = response.CompletedSeq

				// TODO: https://linear.app/thinkparq/issue/BF-29/acknowledge-events-sent-to-all-subscribers-back-to-the-metadata-server
				// Also consider if we need to better handle what we do with recvStream when we break out of the connectedLoop.
				// Probably nothing because there is no expectation subscribers ack every event back to BeeWatch, or BW ack every event to meta.
				// If we're able to reconnect then we'll start reading the recvStream again.
				// If we're shutting down it doesn't matter since BeeWatch doesn't store any state on-disk.
				// Whatever ack'h events back to meta will need to handle if a subscriber is removed, knowing to disregard events it hasn't ack'd.
			}
		}
	}()

	return done, cancel
}

// sendLoop() handles sending events to the subscriber. It will do this until
// the connection breaks for any reason (gracefully or otherwise). Once it
// returns the connection must be disconnected and reconnected before sendLoop()
// is called again. It does not return an error because the caller should react
// the same in all scenarios.
func (h *Handler) sendLoop() (<-chan struct{}, context.CancelFunc) {

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	h.log.Info("beginning normal event stream")
	loggedAckError := false

	go func() {
		defer close(done)
		defer cancel()
		for {
			select {
			case <-ctx.Done():
				h.log.Debug("stopping sending events because the handler is shutting down")
				return
			case <-time.After(time.Duration(h.config.PollFrequency) * time.Second):
				// Poll on a configurable period for new events to be added to the buffer.
			sendEvents:
				for {
					select {
					case <-ctx.Done():
						h.log.Debug("forcibly stop sending events because the handler is shutting down")
						return
					default:
						// We could alternatively use a conditional variable (sync.Cond) instead of polling.
						// At this time there doesn't appear to be a significant drawback to this approach.
						// Mainly because starting to send events in not latency sensitive.
						event, err := h.metaEventBuffer.GetEvent(h.ID)
						if err != nil {
							h.log.Error("unable to get the next event in the local buffer", zap.Error(err))
							break sendEvents
						}

						// Continue sending events until there are no more events to send:
						if event != nil {
							// Don't send duplicate events.
							if event.SeqId > h.lastSeqID || (event.SeqId == 0 && h.lastSeqID == 0) {
								if err := h.Send(event); err != nil {
									h.log.Error("unable to send event", zap.Error(err), zap.Any("event", event.SeqId))
									return
								}
							} else {
								// As an optimization, keep trying to ack the lastSeqID so once the
								// buffer is repopulated we don't need to process/discard all events
								// and can just immediately fast forward the ack cursor.
								err := h.metaEventBuffer.AckEvent(h.ID, h.lastSeqID)
								if err != nil {
									if !loggedAckError {
										h.log.Debug("ignoring event already sent to the subscriber and attempting to fast forward the ack cursor (further occurrences will not be logged until this operation succeeds)", zap.Error(err), zap.String("hint", "if Watch just started this likely indicates the event buffer is not fully repopulated yet"))
										loggedAckError = true
									}
								} else {
									loggedAckError = false
								}
							}
						} else {
							break sendEvents
						}
					}
				}
			}
		}
	}()
	return done, cancel
}

// Stop is called to cancel the context associated with a particular handler.
// This will cause the Go routine handling the subscriber to attempt to cleanly disconnect.
func (h *Handler) Stop() {
	h.log.Info("shutting down subscriber")
	h.cancel()
}
