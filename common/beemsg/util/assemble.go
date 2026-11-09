package util

import (
	"fmt"
	"reflect"

	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
)

// Serializes and creates a complete on-the-wire BeeMsg (header + body).
func AssembleBeeMsg(in msg.SerializableMsg) ([]byte, error) {
	buf := make([]byte, 0, 256)

	header := msg.NewHeader(in.MsgId())

	ser := beeserde.NewSerializer(buf)
	header.Serialize(&ser)
	err := ser.Finish()
	if err != nil {
		return nil, fmt.Errorf("BeeMsg header serialization failed: %w", err)
	}

	in.Serialize(&ser)
	err = ser.Finish()
	if err != nil {
		return nil, fmt.Errorf("BeeMsg body serialization failed: %w", err)
	}

	finalBuf := ser.Buf.Bytes()

	if err := msg.OverwriteMsgFeatureFlags(finalBuf[0:msg.HeaderLen], ser.MsgFeatureFlags); err != nil {
		return nil, err
	}

	return finalBuf, nil
}

// Serializes and creates a complete on-the-wire legacy BeeMsg (header + body).
func AssembleLegacyBeeMsg(in msg.SerializableMsg) ([]byte, error) {
	buf := make([]byte, 0, 256)

	header := msg.NewHeader(in.MsgId())

	ser := beeserde.NewSerializer(buf)
	header.SerializeLegacy(&ser)
	err := ser.Finish()
	if err != nil {
		return nil, fmt.Errorf("BeeMsg header serialization failed: %w", err)
	}

	in.Serialize(&ser)
	err = ser.Finish()
	if err != nil {
		return nil, fmt.Errorf("BeeMsg body serialization failed: %w", err)
	}

	// Once buf is provided to NewSerializer (which creates a bytes.Buffer) the caller should not
	// use buf again directly. This is because bytes.Buffer will grow the buffer as needed, and by
	// this point the original buf may not point to the same slice anymore. Instead get the final
	// buffer and make any necessary modifications before returning.
	finalBuf := ser.Buf.Bytes()

	// The actual serialized message length is only known after serialization of the body, so we
	// overwrite the serialized header value with the actual length here
	if err := msg.OverwriteLegacyMsgLen(finalBuf[0:msg.LegacyHeaderLen], uint32(ser.Buf.Len())); err != nil {
		return nil, err
	}

	// MsgFeatureFlags is defined during serialization, therefore we overwrite the serialized header
	// value here
	if err := msg.OverwriteLegacyMsgFeatureFlags(finalBuf[0:msg.LegacyHeaderLen], ser.MsgFeatureFlags); err != nil {
		return nil, err
	}

	return finalBuf, nil
}

// Deserializes and outputs a complete BeeMsg (header + body). The input slices must contain the
// complete BeeMsg header and body of the expected output msg type - nothing more.
func DisassembleBeeMsg(bufHeader []byte, bufBody []byte, out msg.DeserializableMsg) error {
	return disassemble(bufHeader, bufBody, out, (*msg.Header).Deserialize)
}

// Deserializes and outputs a complete legacy BeeMsg (header + body). The input slices must contain
// the complete BeeMsg header and body of the expected output msg type - nothing more.
func DisassembleLegacyBeeMsg(bufHeader []byte, bufBody []byte, out msg.DeserializableMsg) error {
	return disassemble(bufHeader, bufBody, out, (*msg.Header).DeserializeLegacy)
}

func disassemble(bufHeader []byte, bufBody []byte, out msg.DeserializableMsg,
	deserializeHeader func(*msg.Header, *beeserde.Deserializer)) error {
	// Check that the deserialization target is a pointer
	if reflect.ValueOf(out).Type().Kind() != reflect.Pointer {
		return fmt.Errorf("attempt to deserialize into a non-pointer")
	}

	header := msg.Header{}
	desHeader := beeserde.NewDeserializer(bufHeader, 0)
	deserializeHeader(&header, &desHeader)

	err := desHeader.Finish()
	if err != nil {
		return fmt.Errorf("BeeMsg header deserialization failed: %w", err)
	}

	// Ensure we read the expected message
	if header.MsgID != out.MsgId() {
		return fmt.Errorf("got BeeMsg with ID %d, expected ID %d", header.MsgID, out.MsgId())
	}

	desBody := beeserde.NewDeserializer(bufBody, header.MsgFeatureFlags)
	out.Deserialize(&desBody)

	err = desBody.Finish()
	if err != nil {
		return fmt.Errorf("BeeMsg body deserialization failed: %w", err)
	}

	return nil
}
