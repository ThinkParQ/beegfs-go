package util

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
)

const maxMsgLen = 4 * 1024 * 1024

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

// Assembles a complete on-the-wire BeeMsg (header + body) and writes it to an io.Writer, usually a
// stream or a socket.
func WriteTo(ctx context.Context, w io.Writer, in msg.SerializableMsg) error {
	if fc, ok := w.(*FramedConn); ok {
		b, err := AssembleBeeMsg(in)
		if err != nil {
			return err
		}

		if err := goWithContext(ctx, func() error { return fc.WriteMsg(b) }); err != nil {
			return fmt.Errorf("%w: %w", ErrBeeMsgWrite, err)
		}
		return nil
	}

	b, err := AssembleLegacyBeeMsg(in)
	if err != nil {
		return err
	}

	err = goWithContext(ctx, func() error {
		_, err := bytes.NewBuffer(b).WriteTo(w)
		return err
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBeeMsgWrite, err)
	}

	return nil
}

// Reads and disassembles a complete on-the-wire BeeMsg (header + body) from an io.Reader, usually a
// stream or socket.
func ReadFrom(ctx context.Context, r io.Reader, out msg.DeserializableMsg) error {
	if fc, ok := r.(*FramedConn); ok {
		var b []byte
		err := goWithContext(ctx, func() error {
			var err error
			b, err = fc.ReadMsg(maxMsgLen)
			return err
		})
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBeeMsgRead, err)
		}
		if len(b) < msg.HeaderLen {
			return fmt.Errorf("%w: message shorter than its header", ErrBeeMsgRead)
		}

		return DisassembleBeeMsg(b[:msg.HeaderLen], b[msg.HeaderLen:], out)
	}

	bufHeader := make([]byte, msg.LegacyHeaderLen)

	// Read in the header
	err := goWithContext(ctx, func() error {
		_, err := io.ReadFull(r, bufHeader)
		return err
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBeeMsgRead, err)
	}

	// Extract the whole message length from the serialized header
	msgLen, err := msg.ExtractLegacyMsgLen(bufHeader)
	if err != nil {
		return fmt.Errorf("extracting msgLen failed: %w", err)
	}

	bufBody := make([]byte, msgLen-msg.LegacyHeaderLen)

	// Read in the body
	err = goWithContext(ctx, func() error {
		_, err := io.ReadFull(r, bufBody)
		return err
	})
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBeeMsgRead, err)
	}

	return DisassembleLegacyBeeMsg(bufHeader, bufBody, out)
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
