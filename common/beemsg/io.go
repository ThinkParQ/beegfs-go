package beemsg

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"

	"github.com/thinkparq/gobee/beemsg/beeserde"
	"github.com/thinkparq/gobee/beemsg/msg"
)

// Executes function f while blocking. Returns when f is completed or the context is cancelled.
func goWithContext(ctx context.Context, f func() error) error {
	ch := make(chan error, 1)

	// Execute the provided function
	go func() {
		defer close(ch)
		ch <- f()
	}()

	// Wait for the function to complete or the contexts Done() channel closes
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Serializes and creates a complete on-the-wire BeeMsg (header + body).
func AssembleBeeMsg(in msg.SerializableMsg) ([]byte, error) {
	buf := make([]byte, 0, 256)

	header := NewHeader(in.MsgId())

	ser := beeserde.NewSerializer(buf)
	header.Serialize(&ser)
	in.Serialize(&ser)

	if len(ser.Errors.Errors) > 0 {
		return nil, fmt.Errorf("message serialization failed: %w", &ser.Errors)
	}

	// The actual serialized message length is only known after serialization of the body, so we
	// overwrite the serialized header value with the actual length here
	if err := OverwriteMsgLen(buf[0:HeaderLen], uint32(ser.Buf.Len())); err != nil {
		return nil, err
	}

	// MsgFeatureFlags is defined during serialization, therefore we overwrite the serialized header
	// value here
	if err := OverwriteMsgFeatureFlags(buf[0:HeaderLen], ser.MsgFeatureFlags); err != nil {
		return nil, err
	}

	return ser.Buf.Bytes(), nil
}

// Deserializes and outputs a complete BeeMsg (header + body). The input slices must contain the
// complete BeeMsg header and body of the expected output msg type - nothing more.
func DisassembleBeeMsg(bufHeader []byte, bufBody []byte, out msg.DeserializableMsg) error {
	// Check that the deserialization target is a pointer
	if reflect.ValueOf(out).Type().Kind() != reflect.Pointer {
		return fmt.Errorf("attempt to deserialize into a non-pointer")
	}

	header := Header{}
	desHeader := beeserde.NewDeserializer(bufHeader, 0)
	header.Deserialize(&desHeader)

	if len(desHeader.Errors.Errors) > 0 {
		return fmt.Errorf("BeeMsg header deserialization failed: %w", &desHeader.Errors)
	}

	// Ensure we read the expected message
	if header.MsgID != out.MsgId() {
		return fmt.Errorf("got BeeMsg with ID %d, expected ID %d", header.MsgID, out.MsgId())
	}

	desBody := beeserde.NewDeserializer(bufBody, header.MsgFeatureFlags)
	out.Deserialize(&desBody)

	if len(desBody.Errors.Errors) > 0 {
		return fmt.Errorf("BeeMsg deserialization failed: %w", &desHeader.Errors)
	}

	return nil
}

// Assembles a complete on-the-wire BeeMsg (header + body) and writes it to an io.Writer, usually a
// stream or a socket.
func WriteTo(ctx context.Context, w io.Writer, in msg.SerializableMsg) error {
	b, err := AssembleBeeMsg(in)
	if err != nil {
		return err
	}

	err = goWithContext(ctx, func() error {
		_, err := bytes.NewBuffer(b).WriteTo(w)
		return err
	})
	if err != nil {
		return fmt.Errorf("writing BeeMsg failed: %w", err)
	}

	return nil
}

// Reads and disassembles a complete on-the-wire BeeMsg (header + body) from an io.Reader, usually a
// stream or socket.
func ReadFrom(ctx context.Context, r io.Reader, out msg.DeserializableMsg) error {
	bufHeader := make([]byte, HeaderLen)

	// Read in the header
	err := goWithContext(ctx, func() error {
		_, err := io.ReadFull(r, bufHeader)
		return err
	})
	if err != nil {
		return fmt.Errorf("reading BeeMsg header failed: %w", err)
	}

	// Extract the whole message length from the serialized header
	msgLen, err := ExtractMsgLen(bufHeader)
	if err != nil {
		return fmt.Errorf("extracting msgLen failed: %w", err)
	}

	bufBody := make([]byte, msgLen-HeaderLen)

	// Read in the body
	err = goWithContext(ctx, func() error {
		_, err := io.ReadFull(r, bufBody)
		return err
	})
	if err != nil {
		return fmt.Errorf("reading BeeMsg failed: %w", err)
	}

	return DisassembleBeeMsg(bufHeader, bufBody, out)
}

// Assembles a complete on-the-wire BeeMsg (header + body) and writes it to an io.ReadWriter,
// usually a stream or a socket. Afterwards the response is read and disassembled into the type of
// out, unless it is set to nil - in this case the reading is skipped.
func WriteRead(ctx context.Context, rw io.ReadWriter,
	in msg.SerializableMsg, out msg.DeserializableMsg) error {
	if err := WriteTo(ctx, rw, in); err != nil {
		return err
	}

	if out != nil {
		return ReadFrom(ctx, rw, out)
	}

	return nil
}
