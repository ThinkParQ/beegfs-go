package beeserde

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/thinkparq/gobee/types"
)

// Passed to the serialization methods. Contains the serialization buffer and additional
// information needed for some messages.
type Serializer struct {
	Buf             bytes.Buffer
	MsgFeatureFlags uint16
	Errors          types.MultiError
}

// Create new serializer
func NewSerializer() Serializer {
	return Serializer{}
}

// SERIALIZER

// A BeeGFS serializable type
type Serializable interface {
	// Defines how to serialize the type.
	// Must match its deserialization procedure. Might be defined in other
	// locations, e.g. the C++ or Rust codebase.
	Serialize(*Serializer)
}

// Serializes a primitive integer value in little endian order.
func SerializeInt(ser *Serializer, value any) {
	if err := binary.Write(&ser.Buf, binary.LittleEndian, value); err != nil {
		ser.Errors.Errors = append(ser.Errors.Errors, err)
	}
}

// Serializes a raw slice of bytes
func SerializeBytes(ser *Serializer, value []byte) {
	if _, err := ser.Buf.Write(value); err != nil {
		ser.Errors.Errors = append(ser.Errors.Errors, err)
	}
}

// Serializes a slice of bytes into a C string
func SerializeCStr(ser *Serializer, value []byte, alignTo uint) {
	SerializeInt(ser, uint32(len(value)))
	SerializeBytes(ser, value)

	if err := ser.Buf.WriteByte(0); err != nil {
		ser.Errors.Errors = append(ser.Errors.Errors, err)
	}

	if alignTo != 0 {
		// Total amount of bytes written for this CStr modulo alignTo - results in the number
		// of pad bytes to skip due to alignment
		padding := (uint(len(value)) + 4 + 1) % alignTo

		if padding != 0 {
			// Yes, this does actually not achieve alignment - but we have to mimic the C++ code
			Zeroes(ser, alignTo-padding)
		}
	}
}

type seqSerializer struct {
	initialPos int
	countPos   int
}

func (t *seqSerializer) begin(ser *Serializer, includeTotalSize bool) {
	t.initialPos = ser.Buf.Len()

	if includeTotalSize {
		SerializeInt(ser, uint32(0xFFFFFFFF))
	}

	t.countPos = ser.Buf.Len()
	SerializeInt(ser, uint32(0xFFFFFFFF))
}

func (t *seqSerializer) finish(ser *Serializer, count int) {
	b := ser.Buf.Bytes()

	if t.initialPos != t.countPos {
		written := len(b) - t.initialPos
		binary.LittleEndian.PutUint32(b[t.initialPos:t.initialPos+4], uint32(written))
	}

	binary.LittleEndian.PutUint32(b[t.countPos:t.countPos+4], uint32(count))
}

// Serializes a slice of any data into a Seq
func SerializeSeq[T any](ser *Serializer, value []T, includeTotalSize bool, f func(T)) {
	ss := seqSerializer{}
	ss.begin(ser, includeTotalSize)

	for _, e := range value {
		f(e)
	}

	ss.finish(ser, len(value))
}

// Serializes map of any data into a Map
func SerializeMap[K comparable, V any](ser *Serializer, value map[K]V, includeTotalSize bool, fKey func(K), fValue func(V)) {
	ss := seqSerializer{}
	ss.begin(ser, includeTotalSize)

	for k, v := range value {
		fKey(k)
		fValue(v)
	}

	ss.finish(ser, len(value))
}

// Fills in n zeroes
func Zeroes(ser *Serializer, n uint) {
	for i := uint(0); i < n; i++ {
		if err := ser.Buf.WriteByte(0); err != nil {
			ser.Errors.Errors = append(ser.Errors.Errors, err)
		}
	}
}

// DESERIALIZER

// Passed to the deserialization methods. Contains the serialization buffer and additional
// information needed for some messages.
type Deserializer struct {
	Buf             bytes.Buffer
	MsgFeatureFlags uint16
	Errors          types.MultiError
}

// Create new serializer
func NewDeserializer(s []byte, msgFeatureFlags uint16) Deserializer {
	return Deserializer{Buf: *bytes.NewBuffer(s), MsgFeatureFlags: msgFeatureFlags}
}

// A BeeGFS deserializable type
type Deserializable interface {
	// Defines how to deserialize the type.
	// Must match its serialization procedure. Might be defined in other
	// locations, e.g. the C++ or Rust codebase.
	Deserialize(*Deserializer)
}

// Deserializes a primitive integer value in little endian order.
func DeserializeInt(des *Deserializer, into any) {
	if reflect.ValueOf(into).Type().Kind() != reflect.Pointer {
		des.Errors.Errors = append(des.Errors.Errors, fmt.Errorf("attempt to deserialize int into non-pointer"))
	}

	if err := binary.Read(&des.Buf, binary.LittleEndian, into); err != nil {
		des.Errors.Errors = append(des.Errors.Errors, err)
	}
}

// Deserializes into a slice of bytes
func DeserializeBytes(des *Deserializer, into *[]byte) {
	if _, err := des.Buf.Read(*into); err != nil {
		des.Errors.Errors = append(des.Errors.Errors, err)
	}
}

// Deserializes a C string into a slice of bytes
func DeserializeCStr(des *Deserializer, into *[]byte, alignTo uint) {
	var len uint32
	DeserializeInt(des, &len)

	*into = make([]byte, len)
	DeserializeBytes(des, into)

	if _, err := des.Buf.ReadByte(); err != nil {
		des.Errors.Errors = append(des.Errors.Errors, err)
	}

	if alignTo != 0 {
		// Total amount of bytes read for this CStr modulo alignTo - results in the number
		// of pad bytes to skip due to alignment
		padding := (uint(len) + 4 + 1) % alignTo

		if padding != 0 {
			// Yes, this does actually not achieve alignment - but we have to mimic the C++ code
			Skip(des, alignTo-padding)
		}
	}
}

func deserializeSeqLen(des *Deserializer, includeTotalSize bool) uint32 {
	if includeTotalSize {
		Skip(des, 4)
	}

	len := uint32(0)
	DeserializeInt(des, &len)

	return len
}

// Deserializes a Seq of any data into a slice
func DeserializeSeq[T any](des *Deserializer, into *[]T, includeTotalSize bool, f func(*T)) {
	len := deserializeSeqLen(des, includeTotalSize)

	for i := 0; i < int(len); i++ {
		var v T
		f(&v)
		*into = append(*into, v)
	}
}

// Deserializes a Seq of any data into a slice
func DeserializeMap[K comparable, V any](des *Deserializer, into *map[K]V, includeTotalSize bool, fKey func(*K), fValue func(*V)) {
	len := deserializeSeqLen(des, includeTotalSize)

	for i := 0; i < int(len); i++ {
		var key K
		var value V
		fKey(&key)
		fValue(&value)
		(*into)[key] = value
	}
}

// Skips n bytes
func Skip(des *Deserializer, n uint) {
	for i := uint(0); i < n; i++ {
		if _, err := des.Buf.ReadByte(); err != nil {
			des.Errors.Errors = append(des.Errors.Errors, err)
		}
	}
}
