package beeserde

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type ints struct {
	u8  uint8
	u16 uint16
	u32 uint32
	u64 uint64
	i8  int8
	i16 int16
	i32 int32
	i64 int64
}

func (t *ints) Serialize(s *Serializer) {
	SerializeInt(s, t.u8)
	SerializeInt(s, t.u16)
	SerializeInt(s, t.u32)
	SerializeInt(s, t.u64)
	SerializeInt(s, t.i8)
	SerializeInt(s, t.i16)
	SerializeInt(s, t.i32)
	SerializeInt(s, t.i64)
}

func (t *ints) Deserialize(d *Deserializer) {
	DeserializeInt(d, &t.u8)
	DeserializeInt(d, &t.u16)
	DeserializeInt(d, &t.u32)
	DeserializeInt(d, &t.u64)
	DeserializeInt(d, &t.i8)
	DeserializeInt(d, &t.i16)
	DeserializeInt(d, &t.i32)
	DeserializeInt(d, &t.i64)
}

func TestInt(t *testing.T) {
	s := NewSerializer([]byte{})

	in := ints{
		u8:  1,
		u16: 2,
		u32: 3,
		u64: 4,
		i8:  5,
		i16: 6,
		i32: 7,
		i64: 8,
	}
	in.Serialize(&s)
	assert.NoError(t, s.Finish())

	d := NewDeserializer(s.Buf.Bytes(), 0)
	out := ints{}
	out.Deserialize(&d)

	assert.NoError(t, d.Finish())
	assert.Equal(t, in, out)
}

func TestCStr(t *testing.T) {
	s := NewSerializer([]byte{})

	SerializeCStr(&s, []byte("Hello Go!"), 0)
	SerializeCStr(&s, []byte("Hello Go again!"), 4)
	SerializeCStr(&s, []byte("Hello Go again and again!"), 5)
	SerializeCStr(&s, []byte("Hello Go again and again!"), 8)
	assert.NoError(t, s.Finish())

	out := []byte{}
	d := NewDeserializer(s.Buf.Bytes(), 0)
	DeserializeCStr(&d, &out, 0)
	assert.Equal(t, []byte("Hello Go!"), out)
	DeserializeCStr(&d, &out, 4)
	assert.Equal(t, []byte("Hello Go again!"), out)
	DeserializeCStr(&d, &out, 5)
	assert.Equal(t, []byte("Hello Go again and again!"), out)
	DeserializeCStr(&d, &out, 8)
	assert.Equal(t, []byte("Hello Go again and again!"), out)

	assert.NoError(t, d.Finish())
}

// Explicitly test the
func TestCStrAlignment(t *testing.T) {
	s1 := NewSerializer([]byte{})
	SerializeCStr(&s1, []byte{}, 1)
	assert.Equal(t, 5, s1.Buf.Len())

	s2 := NewSerializer([]byte{})
	SerializeCStr(&s2, []byte{}, 2)
	assert.Equal(t, 6, s2.Buf.Len())

	s3 := NewSerializer([]byte{})
	SerializeCStr(&s3, []byte("aa"), 7)
	assert.Equal(t, 7, s3.Buf.Len())

	s4 := NewSerializer([]byte{})
	SerializeCStr(&s4, []byte("aaa"), 7)
	assert.Equal(t, 14, s4.Buf.Len())

	s5 := NewSerializer([]byte{})
	SerializeCStr(&s5, []byte("aaaa"), 4)
	assert.Equal(t, 12, s5.Buf.Len())
}

func TestStringSeq(t *testing.T) {
	s := NewSerializer([]byte{})

	in := [][]byte{[]byte("String1"), []byte("String2"), []byte("LongerString")}
	SerializeStringSeq(&s, in)
	assert.NoError(t, s.Finish())

	d := NewDeserializer(s.Buf.Bytes(), 0)
	out := [][]byte{}
	DeserializeStringSeq(&d, &out)

	assert.NoError(t, d.Finish())
	assert.Equal(t, in, out)

	// Check that forbidden null bytes cause an error
	nullByteInput := [][]byte{[]byte("String1\x00A"), []byte("String2"), []byte("LongerString")}
	SerializeStringSeq(&s, nullByteInput)
	assert.Error(t, s.Finish())
}

func TestNestedSeq(t *testing.T) {
	s := NewSerializer([]byte{})

	in := [][]uint32{{1, 2, 3}, {4, 5, 6, 7, 8}, {0xFFFFFFFE, 0xFFFFFFFF}}
	SerializeSeq(&s, in, true, func(in []uint32) {
		SerializeSeq(&s, in, false, func(in uint32) {
			SerializeInt(&s, in)
		})
	})
	assert.NoError(t, s.Finish())

	d := NewDeserializer(s.Buf.Bytes(), 0)
	out := [][]uint32{}
	DeserializeSeq(&d, &out, true, func(out *[]uint32) {
		DeserializeSeq(&d, out, false, func(out *uint32) {
			DeserializeInt(&d, out)
		})
	})

	assert.NoError(t, d.Finish())
	assert.Equal(t, in, out)
}

func TestNestedMap(t *testing.T) {
	s := NewSerializer([]byte{})

	in := map[int8]map[uint16]uint64{
		-10: {
			1000: 12345,
		},
		77: {
			65535: 0,
			1:     1234,
			2:     1235,
		},
	}

	SerializeMap(&s, in, true, func(in int8) {
		SerializeInt(&s, in)
	}, func(in map[uint16]uint64) {
		SerializeMap(&s, in, false, func(in uint16) {
			SerializeInt(&s, in)
		}, func(in uint64) {
			SerializeInt(&s, in)
		})
	})
	assert.NoError(t, s.Finish())

	d := NewDeserializer(s.Buf.Bytes(), 0)
	out := map[int8]map[uint16]uint64{}
	DeserializeMap(&d, &out, true, func(out *int8) {
		DeserializeInt(&d, out)
	}, func(out *map[uint16]uint64) {
		*out = map[uint16]uint64{}

		DeserializeMap(&d, out, false, func(out *uint16) {
			DeserializeInt(&d, out)
		}, func(out *uint64) {
			DeserializeInt(&d, out)
		})
	})

	assert.NoError(t, d.Finish())
	assert.Equal(t, in, out)
}

func TestErrorOnNonPointerDeserialization(t *testing.T) {
	d := NewDeserializer([]byte{1, 2, 3, 4}, 0)
	DeserializeInt(&d, uint32(0))

	assert.Error(t, d.Finish())
}
