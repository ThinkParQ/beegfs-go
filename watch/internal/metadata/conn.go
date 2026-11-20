package metadata

import (
	"errors"
	"net"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Sends and receives FileEvent messages to the metadata service.
type fileEventConnHandler struct {
	conn net.Conn
	mu   sync.Mutex
	ser  Serializer
	des  Deserializer
	log  *zap.Logger
}

func (c *fileEventConnHandler) addMetaIDToLogs(metaID uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log = c.log.With(zap.Uint32("metaID", metaID))
}

func (c *fileEventConnHandler) closeAtShutdown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.log.Info("attempting to close metadata connection because the app is shutting down")
	if err := c.conn.Close(); err != nil {
		c.log.Error("unable to close metadata connection", zap.Error(err))
	}
}

// Attempts to send the requested message.
func (c *fileEventConnHandler) send(message Serializable) bool {
	c.log.Debug("sending message", zap.Any("type", message.MsgID()))
	m := c.ser.Assemble(message)
	_, err := c.conn.Write(m)
	if err != nil {
		// TODO: We may want to handle this and skip printing an error.
		// Handle if we're gracefully shutting down and the socket was closed.
		// if errors.Is(err, net.ErrClosed) {
		// 	c.log.Debug("disconnected from metadata service")
		// 	return false
		// }
		c.log.Error("error sending message to metadata server", zap.Any("type", message.MsgID()), zap.Error(err))
		return false
	}
	return true
}

// Attempts to receive the selected message.
func (c *fileEventConnHandler) recv(message Deserializable) bool {
	return c.recvBefore(message, time.Time{})
}

func (c *fileEventConnHandler) recvReply(message Deserializable, timeout time.Duration) bool {
	return c.recvBefore(message, time.Now().Add(timeout))
}

func (c *fileEventConnHandler) recvBefore(message Deserializable, replyDeadline time.Time) bool {

	defer func() {
		if r := recover(); r != nil {
			c.log.Error("received unexpected message", zap.Binary("binary", c.des.connBuf), zap.ByteString("byteString", c.des.connBuf))
		}
	}()

	c.log.Debug("waiting to receive message", zap.Any("message", message.MsgID()))
	for {
		// Try to guarantee we won't ever try to disconnect while we're reading an event from the
		// Metadata service. The connection is locked to prevent the Manager from closing it while
		// we're in the process of reading/deserializing an event. We use a read deadline to
		// periodically release the lock when no events are being read. This prevents read from
		// blocking the Manager from ever gracefully shutting down the connection. This approach is
		// intended to prevent dropped events when gracefully shutting down the app. Based on
		// testing resetting the read deadline and relocking the mutex between reading each event
		// has a negligible performance impact on events per second.
		readDeadline := time.Now().Add(1 * time.Second)
		if !replyDeadline.IsZero() && replyDeadline.Before(readDeadline) {
			readDeadline = replyDeadline
		}
		c.mu.Lock()
		c.conn.SetReadDeadline(readDeadline)
		bytesRead, err := c.conn.Read(c.des.Load())
		c.mu.Unlock()
		if err != nil {
			// Handle if we just exceeded the deadline and gave up the lock temporarily.
			if errors.Is(err, os.ErrDeadlineExceeded) {
				if bytesRead != 0 {
					// In theory we don't need to worry about reading a partial message when setting
					// a read deadline because we're using the unixpacket type (SOCK_SEQPACKET), so
					// each read corresponds to a full message. If this ever did happen we would
					// loose the event.
					c.log.Error("reading from the metadata server exceeded the read deadline but still returned a partial message (this is probably a bug)", zap.Any("message", message.MsgID()), zap.Any("bytesRead", bytesRead))
					return false
				}
				if !replyDeadline.IsZero() && !time.Now().Before(replyDeadline) {
					c.log.Warn("closing the metadata connection because the metadata service did not reply in time", zap.Any("message", message.MsgID()))
					return false
				}
				continue
			}
			// Handle if we're gracefully shutting down and the socket was closed.
			if errors.Is(err, net.ErrClosed) {
				c.log.Debug("disconnected from metadata service")
				return false
			}
			c.log.Error("unable to receive message", zap.Any("message", message.MsgID()), zap.Error(err))
			return false
		}
		if err = c.des.Disassemble(message, bytesRead); err != nil {
			c.log.Error("unexpected error deserializing message", zap.Any("message", message.MsgID()), zap.Error(err))
			return false
		}
		c.log.Debug("received message", zap.Any("type", message.MsgID()), zap.Any("message", message))
		return true
	}
}
