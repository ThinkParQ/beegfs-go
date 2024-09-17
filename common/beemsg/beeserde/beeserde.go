// beeserde provides the Go implementation of the BeeSerde Serializer, mainly used for
// (de-)serializing BeeMsges.
package beeserde

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
)

var nullByteSlice = []byte{0}

// SERIALIZER

// Represents the serialization state and is passed to the Serialize() methods. Contains the target
// buffer for serialization and additional information needed for some messages.
type Serializer struct {
	// The buffer where the serialized data goes into
	Buf bytes.Buffer
	// Some BeeMsges use a field with the same name on the header to control the serialization
	// process, e.g. conditionally serialize some data. This field allows a message
	// serialization definition to provide the expected value depending on the actually serialized
	// data. This field should only be overwritten from an implementation of Serializable for an
	// actual message, never from inner types. When constructing a BeeMsg, this field is expected
	// to be read out after the call to Serialize() and be put into the BeeMsg header.
	MsgFeatureFlags uint16
	// Any error during serialization goes in here. This field must be checked after using the
	// serializer by calling Serializer.Finish(), otherwise errors might go through unnoticed. If
	// this is set, all following operations will do nothing. Currently serializable types are
	// intentionally not able to set this error.
	err error
}

// Creates a new serializer that serializes into the provided byte slice. The slice must not be
// touched until serialization is finished. The user must call Serializer.Finish() when done to
// check for errors during serialization.
func NewSerializer(b []byte) Serializer {
	return Serializer{Buf: *bytes.NewBuffer(b)}
}

// A BeeSerde serializable type, mainly intended for BeeMsg communication
type Serializable interface {
	// Defines how to serialize the message or type.
	// Must match its deserialization procedure. If the type is used as a BeeMsg (or an inner value
	// of one), it must match the definitions defined in other locations, e.g. the C++ or Rust codebase.
	// Note that failing to match the other languages definition can potentially go through undetected
	// and might cause undefined behavior (or cause crashes, if you are lucky). The correctness can
	// currently only be tested manually, so be very cautious.
	Serialize(*Serializer)
}

// Fail is how Serializable types indicate when serialization should fail for some reason that
// cannot be detected by the SerializeX functions. Note this should be used sparingly as generally
// serialization should not be concerned with application logic. Valid use cases include handling
// enums where strict enforcement of valid variants is required, or when it is not necessary to
// reimplement serialization logic in Go for deprecated functionality that is no longer supported.
func (ser *Serializer) Fail(err error) {
	ser.err = err
}

// To be called after serialization is done - checks that there was no error.
func (ser *Serializer) Finish() error {
	if ser.err != nil {
		return ser.err
	}

	return nil
}

// Serializes a primitive integer value into an "Int". An "Int" is one of the on-the-wire types of
// BeeSerde and represents integer types as well as bool. This function accepts any value, but must
// be used with integers. The caller must explicitly provide the exact integer type (e.g. uint16,
// int32 - NOT int or uint).
func SerializeInt(ser *Serializer, value any) {
	if ser.err != nil {
		return
	}

	if err := binary.Write(&ser.Buf, binary.LittleEndian, value); err != nil {
		ser.err = err
		return
	}
}

// Serializes a raw slice of bytes. This can be used to implement any special serialization logic
// and is used by other Serialize functions. Note that it does NOT serialize size information,
// so to read out the correct amount of bytes on the deserializing side, the size has to be known
// beforehand.
func SerializeBytes(ser *Serializer, value []byte) {
	if ser.err != nil {
		return
	}

	if _, err := ser.Buf.Write(value); err != nil {
		ser.err = err
		return
	}
}

// Serializes a slice of bytes into a "CStr". A "CStr" is one of the on-the-wire types of BeeSerde
// and contains a field of size information, the raw bytes and one null byte at the end. The
// alignTo argument conditionally adds some zero bytes at the end and must match the deserialization
// definition (it is usually 0 or 4).
func SerializeCStr(ser *Serializer, value []byte, alignTo uint) {
	if ser.err != nil {
		return
	}

	SerializeInt(ser, uint32(len(value)))
	if ser.err != nil {
		return
	}
	SerializeBytes(ser, value)
	if ser.err != nil {
		return
	}

	if err := ser.Buf.WriteByte(0); err != nil {
		ser.err = err
		return
	}

	if alignTo != 0 {
		// Total amount of bytes written for this CStr (data + length field + null byte) modulo
		// alignTo - results in the number of excess bytes beyond the alignment point
		padding := (uint(len(value)) + 4 + 1) % alignTo

		if padding != 0 {
			Zeroes(ser, alignTo-padding)
		}
	}
}

// Helper type for serializing sequences.
type seqSerializer struct {
	// Stores where the serialized sequence begins
	initialPos int
	// Stores where the number of elements in the sequence are stored
	countPos int
}

// Begin sequence serialization
func (t *seqSerializer) begin(ser *Serializer, includeTotalSize bool) {
	t.initialPos = ser.Buf.Len()

	// If the total Size shall be included, put a placeholder
	if includeTotalSize {
		SerializeInt(ser, uint32(0xFFFFFFFF))
	}

	t.countPos = ser.Buf.Len()
	// Put a placeholder for the number of elements in the sequence
	SerializeInt(ser, uint32(0xFFFFFFFF))
}

// Finish sequence serialization
func (t *seqSerializer) finish(ser *Serializer, count int) {
	b := ser.Buf.Bytes()

	// If the total size shall be included, overwrite the placeholder with the actual amount of
	// written bytes
	if t.initialPos != t.countPos {
		written := len(b) - t.initialPos
		binary.LittleEndian.PutUint32(b[t.initialPos:t.initialPos+4], uint32(written))
	}

	// Overwrite the number of elements placeholder
	binary.LittleEndian.PutUint32(b[t.countPos:t.countPos+4], uint32(count))
}

// Serializes a slice of any data into a "Seq". A "Seq" is one of the on-the-wire types of BeeSerde
// and represents any sequence container type like vectors, lists, and so on. The includeTotalSize
// argument defines whether the total size of the serialized sequence should be included. This has
// no real use and is usually false (for C++ vectors and lists), but one cannot rely on that. When
// defining an existing BeeMsg, the C++ code has to be thoroughly checked on whether this must be
// set or not. The function f defines how to serialize each element of the sequence. By calling
// SerializeSeq from within there, nested sequences can be serialized.
func SerializeSeq[T any](ser *Serializer, value []T, includeTotalSize bool, f func(T)) {
	if ser.err != nil {
		return
	}

	// Setup the sequence helper
	ss := seqSerializer{}
	ss.begin(ser, includeTotalSize)

	// Serialize the inner elements
	for _, e := range value {
		if ser.err != nil {
			return
		}

		f(e)
	}

	// Overwrite the placeholders
	ss.finish(ser, len(value))
}

// Serializes a slice of []bytes into a special sequence. This is a specialization that is used on the
// C++ side by default for std::list<std::string>, std::vector<std::string> and std::set<std::string>.
// This skips the length fields that CStr has and totally relies on a terminating null byte for
// deserialization. For that reason, the input is checked for containing null bytes which will cause
// an error and abort serialization (it would break the deserializer).
// Note that we refer to "strings" here despite accepting []byte because on the C++ it is used for
// std::string, which is not the same as a string in Go.
func SerializeStringSeq(ser *Serializer, value [][]byte) {
	if ser.err != nil {
		return
	}

	// Setup the sequence helper
	ss := seqSerializer{}
	ss.begin(ser, true)

	// Serialize the strings
	for _, s := range value {
		if ser.err != nil {
			return
		}

		if bytes.Contains([]byte(s), nullByteSlice) {
			ser.Fail(fmt.Errorf("SerializeStringSeq input must not contain any null bytes"))
		}

		SerializeBytes(ser, s)
		if ser.err != nil {
			return
		}

		if err := ser.Buf.WriteByte(0x00); err != nil {
			ser.err = err
			return
		}
	}

	// Overwrite the placeholders
	ss.finish(ser, len(value))
}

// Serializes map of any data into a "Map". A "Map" is one of the on-the-wire types of BeeSerde
// and represents all kinds of key-value collections like HashMaps. The includeTotalSize argument
// defines whether the total size of the serialized map should be included. This has no real use
// and is usually true (for C++ std::map), but one cannot rely on that. When defining an existing
// BeeMsg, the C++ code has to be thoroughly checked on whether this must be set or not.
func SerializeMap[K comparable, V any](ser *Serializer, value map[K]V, includeTotalSize bool, fKey func(K), fValue func(V)) {
	if ser.err != nil {
		return
	}

	// Map serialization is built on top of sequence serialization 	// Setup the sequence helper
	ss := seqSerializer{}
	ss.begin(ser, includeTotalSize)

	// Serialize the inner elements
	for k, v := range value {
		if ser.err != nil {
			return
		}

		fKey(k)
		fValue(v)
	}

	// Overwrite the placeholders
	ss.finish(ser, len(value))
}

// Serializes n zero bytes. Sometimes used for padding or just for a fixed amount.
func Zeroes(ser *Serializer, n uint) {
	if ser.err != nil {
		return
	}

	for i := uint(0); i < n; i++ {
		if err := ser.Buf.WriteByte(0); err != nil {
			ser.err = err
			return
		}
	}
}

// DESERIALIZER

// Passed to the deserialization methods. Contains the serialization buffer and additional
// information needed for some messages.
type Deserializer struct {
	// The buffer containing the to-be-deserialized data
	Buf bytes.Buffer
	// Some BeeMsges use a field with the same name on the header to control the deserialization
	// process, e.g. conditionally deserialize some data. This field can be accessed from
	// deserialization definition and be used to deserialize the message correctly.
	// When constructing a BeeMsg, this field is expected to be set before the call to
	// Deserialize() from the respective field from the received header.
	MsgFeatureFlags uint16
	// Any error during deserialization goes in here. This field must be checked after using the
	// deserializer by calling Deserializer.Finish(), otherwise errors might go through unnoticed.
	// If this is set, all following operations will do nothing to prevent "infinite" loops. This
	// approach avoids the need to include err != nil checks all over the deserialization logic.
	// Deserializable types can set this using the Fail() method.
	err error
}

// Creates a new deserializer using the given byte slice as input. The user must call
// Deserializer.Finish() when done to check for errors during deserialization.
func NewDeserializer(s []byte, msgFeatureFlags uint16) Deserializer {
	return Deserializer{Buf: *bytes.NewBuffer(s), MsgFeatureFlags: msgFeatureFlags}
}

// A BeeSerde deserializable type, mainly intended for BeeMsg communication
type Deserializable interface {
	// Defines how to deserialize the message or type.
	// Must match its serialization procedure. If the type is used as a BeeMsg (or an inner value
	// of one), it must match the definitions defined in other locations, e.g. the C++ or Rust codebase.
	// Note that failing to match the other languages definition can potentially go through undetected
	// and might cause undefined behavior (or cause crashes, if you are lucky). The correctness can
	// currently only be tested manually, so be very cautious.
	Deserialize(*Deserializer)
}

// Fail is how Deserializable types indicate when deserialization should fail for some reason that
// cannot be detected by the DeserializeX functions. Note this should be used sparingly as generally
// deserialization should not be concerned with application logic. Valid use cases include handling
// enums where strict enforcement of valid variants is required, or when it is not necessary to
// reimplement deserialization logic in Go for deprecated functionality that is no longer supported.
func (des *Deserializer) Fail(err error) {
	des.err = err
}

// To be called after deserialization is done - checks if the whole buffer and only the whole
// buffer has been consumed and that there was no error.
func (des *Deserializer) Finish() error {
	if des.err != nil {
		return des.err
	}

	if des.Buf.Len() != 0 {
		return fmt.Errorf("did not consume the whole buffer, %d bytes are left", des.Buf.Len())
	}

	return nil
}

// Deserializes into a primitive integer value from an "Int". An "Int" is one of the on-the-wire
// types of BeeSerde and represents integer types as well as bool. This function accepts any value,
// but must be used with integers. The caller must explicitly provide the exact integer type (e.g.
// uint16, int32 - NOT int or uint).
func DeserializeInt(des *Deserializer, into any) {
	if des.err != nil {
		return
	}

	// A caller could accidentally pass in a value instead of a pointer, in which case the
	// deserialized data would just be lost. Therefore, we check here that we actually got a
	// pointer.
	if reflect.ValueOf(into).Type().Kind() != reflect.Pointer {
		des.err = fmt.Errorf("attempt to deserialize int into non-pointer")
		return
	}

	if err := binary.Read(&des.Buf, binary.LittleEndian, into); err != nil {
		des.err = err
		return
	}
}

// Deserializes a raw slice of bytes of length len(into). This can be used to implement any special
// deserialization logic and is used by other Deserialize functions. Note that into must be set to
// the correct length beforehand and the source buffer must contain enough data to completely fill
// it.
func DeserializeBytes(des *Deserializer, into *[]byte) {
	if des.err != nil {
		return
	}

	if _, err := des.Buf.Read(*into); err != nil {
		des.err = err
		return
	}
}

// Deserializes into a slice of bytes from a "CStr". A "CStr" is one of the on-the-wire types of
// BeeSerde and contains a field of size information, the raw bytes and one null byte at the end.
// into must point to a valid byte structure and will be initialized/resized from within this
// function. The alignTo argument conditionally expects some zero bytes at the end and must match
// the serialization definition (it is usually 0 or 4).
func DeserializeCStr(des *Deserializer, into *[]byte, alignTo uint) {
	if des.err != nil {
		return
	}

	var len uint32
	DeserializeInt(des, &len)
	if des.err != nil {
		return
	}

	*into = make([]byte, len)
	DeserializeBytes(des, into)
	if des.err != nil {
		return
	}

	if _, err := des.Buf.ReadByte(); err != nil {
		des.err = err
		return
	}

	if alignTo != 0 {
		// Total amount of bytes read for this CStr (data + length field + null byte) modulo
		// alignTo - results in the number of excess bytes beyond the alignment point
		padding := (uint(len) + 4 + 1) % alignTo

		if padding != 0 {
			Skip(des, alignTo-padding)
		}
	}
}

// Helper function to retrieve the length of a "Seq"
func deserializeSeqLen(des *Deserializer, includeTotalSize bool) uint32 {
	// If the total size is included, just throw it away. We don't need it.
	if includeTotalSize {
		Skip(des, 4)
	}

	len := uint32(0)
	DeserializeInt(des, &len)

	return len
}

// Deserializes into a slice of any data from a "Seq". A "Seq" is one of the on-the-wire types
// of BeeSerde and represents any sequence container type like vectors, lists, and so on. The
// includeTotalSize argument defines whether the total size of the serialized sequence is included.
// This has no real use and is usually false (for C++ vectors and lists), but one cannot rely on
// that. When defining an existing BeeMsg, the C++ code has to be thoroughly checked on whether this
// must be set or not. The function f defines how to deserialize each element of the sequence. By
// calling DeserializeSeq from within there, nested sequences can be deserialized.
func DeserializeSeq[T any](des *Deserializer, into *[]T, includeTotalSize bool, f func(*T)) {
	// A "Seq" is made of an optional size field, a length field and then all the elements in order

	if des.err != nil {
		return
	}

	l := deserializeSeqLen(des, includeTotalSize)

	// Deserialize the elements of the sequence
	for i := 0; i < int(l); i++ {
		if des.err != nil {
			return
		}

		var v T
		f(&v)
		*into = append(*into, v)
	}
}

// Deserializes a special sequence containing "strings" into a slice of []byte. This is
// a specialization that is used on the C++ side by default for std::list<std::string>,
// std::vector<std::string> and std::set<std::string>. This skips the length fields that CStr
// has and totally relies on a terminating null bytes. This usually works although std::string
// can technically contain null bytes and therefor might show up here as input and break the
// deserialization. If somethings goes wrong here, keep the possibility in mind. Note that we refer
// to "strings" here despite accepting []byte, because on the C++ it is used for std::string, which
// is not the same as a string in Go.
func DeserializeStringSeq(des *Deserializer, into *[][]byte) {
	if des.err != nil {
		return
	}

	l := deserializeSeqLen(des, true)

	// Deserialize the strings of the sequence
	for i := 0; i < int(l); i++ {
		if des.err != nil {
			return
		}

		var s []byte
		s, err := des.Buf.ReadBytes(0x00)
		if err != nil {
			des.err = err
			return
		}

		// Exclude the null byte
		*into = append(*into, s[0:len(s)-1])
	}
}

// Deserializes into a map of any data from a "Map". A "Map" is one of the on-the-wire types of
// BeeSerde and represents all kinds of key-value collections like HashMaps. The includeTotalSize
// argument defines whether the total size of the serialized map is included. This has no real use
// and is usually true (for C++ std::map), but one cannot rely on that. When defining an existing
// BeeMsg, the C++ code has to be thoroughly checked on whether this must be set or not.
func DeserializeMap[K comparable, V any](des *Deserializer, into *map[K]V, includeTotalSize bool, fKey func(*K), fValue func(*V)) {
	// A "Map" is made of an optional size field, a length field and then its key-value pairs (key
	// first, then value)

	if des.err != nil {
		return
	}

	l := deserializeSeqLen(des, includeTotalSize)

	for i := 0; i < int(l); i++ {
		if des.err != nil {
			return
		}

		var key K
		var value V
		fKey(&key)
		fValue(&value)
		(*into)[key] = value
	}
}

// Skips n bytes. Sometimes used for padding or ignoring fields.
func Skip(des *Deserializer, n uint) {
	if des.err != nil {
		return
	}

	for i := uint(0); i < n; i++ {
		if _, err := des.Buf.ReadByte(); err != nil {
			des.err = err
			return
		}
	}
}
