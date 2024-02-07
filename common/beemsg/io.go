package beemsg

import (
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

// Writes a msg to a io.Writer
func WriteTo(ctx context.Context, w io.Writer, in msg.SerializableMsg) error {
	header := NewHeader(in.MsgId())

	ser := beeserde.NewSerializer()
	header.Serialize(&ser)
	in.Serialize(&ser)

	if len(ser.Errors.Errors) > 0 {
		return fmt.Errorf("message serialization failed: %w", &ser.Errors)
	}

	// The actual serialized message length is only known after serialization of the body, so we
	// overwrite the serialized header value with the actual length here
	if err := overwriteMsgLen(ser.Buf.Bytes(), uint32(ser.Buf.Len())); err != nil {
		return err
	}

	// MsgFeatureFlags is defined during serialization, therefore we overwrite the serialized header
	// value here
	if err := overwriteMsgFeatureFlags(ser.Buf.Bytes(), ser.MsgFeatureFlags); err != nil {
		return err
	}

	err := goWithContext(ctx, func() error {
		_, err := ser.Buf.WriteTo(w)
		return err
	})
	if err != nil {
		return fmt.Errorf("writing msg failed: %w", err)
	}

	return nil
}

// Reads a specific msg from a io.Reader
func ReadFrom(ctx context.Context, r io.Reader, out msg.DeserializableMsg) error {
	// Check that the deserialization target is a pointer
	if reflect.ValueOf(out).Type().Kind() != reflect.Pointer {
		return fmt.Errorf("attempt to deserialize into a non-pointer")
	}

	header := Header{}

	// Read and deserialize the header
	bufHeader := make([]byte, HeaderLen)

	err := goWithContext(ctx, func() error {
		_, err := io.ReadFull(r, bufHeader)
		return err
	})
	if err != nil {
		return fmt.Errorf("reading msg failed: %w", err)
	}

	desHeader := beeserde.NewDeserializer(bufHeader, 0)
	header.Deserialize(&desHeader)

	if len(desHeader.Errors.Errors) > 0 {
		return fmt.Errorf("header deserialization failed: %w", &desHeader.Errors)
	}

	// Ensure we read the expected message
	if header.MsgID != out.MsgId() {
		return fmt.Errorf("received msg with ID %d, expected ID %d", header.MsgID, out.MsgId())
	}

	// Read and deserialize the body
	bufMsg := make([]byte, header.MsgLen-HeaderLen)

	err = goWithContext(ctx, func() error {
		_, err := io.ReadFull(r, bufMsg)
		return err
	})
	if err != nil {
		return fmt.Errorf("reading msg failed: %w", err)
	}

	desMsg := beeserde.NewDeserializer(bufMsg, header.MsgFeatureFlags)
	out.Deserialize(&desMsg)

	if len(desMsg.Errors.Errors) > 0 {
		return fmt.Errorf("message deserialization failed: %w", &desHeader.Errors)
	}

	return nil
}

// Writes a msg to a ReadWriter and optionally reads the response if resp is not nil
func WriteRead(ctx context.Context, rw io.ReadWriter, msg msg.SerializableMsg, resp msg.DeserializableMsg) error {
	if err := WriteTo(ctx, rw, msg); err != nil {
		return err
	}

	if resp != nil {
		return ReadFrom(ctx, rw, resp)
	}

	return nil
}
