package ser

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

func (t *ints) Serialize(sd *SerDes) {
	SerializeInt(sd, t.u8)
	SerializeInt(sd, t.u16)
	SerializeInt(sd, t.u32)
	SerializeInt(sd, t.u64)
	SerializeInt(sd, t.i8)
	SerializeInt(sd, t.i16)
	SerializeInt(sd, t.i32)
	SerializeInt(sd, t.i64)
}

func (t *ints) Deserialize(sd *SerDes) {
	DeserializeInt(sd, &t.u8)
	DeserializeInt(sd, &t.u16)
	DeserializeInt(sd, &t.u32)
	DeserializeInt(sd, &t.u64)
	DeserializeInt(sd, &t.i8)
	DeserializeInt(sd, &t.i16)
	DeserializeInt(sd, &t.i32)
	DeserializeInt(sd, &t.i64)
}

func TestInt(t *testing.T) {
	sd := SerDes{}

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
	in.Serialize(&sd)

	out := ints{}
	out.Deserialize(&sd)

	assert.Empty(t, sd.Errors)
	assert.Equal(t, in, out)
}

func TestCStr(t *testing.T) {
	sd := SerDes{}

	SerializeCStr(&sd, []byte("Hello Go!"), 0)
	SerializeCStr(&sd, []byte("Hello Go again!"), 4)
	SerializeCStr(&sd, []byte("Hello Go again and again!"), 5)
	SerializeCStr(&sd, []byte("Hello Go again and again!"), 8)

	out := []byte{}

	DeserializeCStr(&sd, &out, 0)
	assert.Equal(t, []byte("Hello Go!"), out)
	DeserializeCStr(&sd, &out, 4)
	assert.Equal(t, []byte("Hello Go again!"), out)
	DeserializeCStr(&sd, &out, 5)
	assert.Equal(t, []byte("Hello Go again and again!"), out)
	DeserializeCStr(&sd, &out, 8)
	assert.Equal(t, []byte("Hello Go again and again!"), out)

	assert.Empty(t, sd.Errors)
}

// Explicitly test the
func TestCStrAlignment(t *testing.T) {
	s1 := NewSerializer()
	SerializeCStr(&s1, []byte{}, 1)
	assert.Equal(t, 5, s1.Buf.Len())

	s2 := NewSerializer()
	SerializeCStr(&s2, []byte{}, 2)
	assert.Equal(t, 6, s2.Buf.Len())

	s3 := NewSerializer()
	SerializeCStr(&s3, []byte("aa"), 7)
	assert.Equal(t, 7, s3.Buf.Len())

	s4 := NewSerializer()
	SerializeCStr(&s4, []byte("aaa"), 7)
	assert.Equal(t, 14, s4.Buf.Len())

	s5 := NewSerializer()
	SerializeCStr(&s5, []byte("aaaa"), 4)
	assert.Equal(t, 12, s5.Buf.Len())
}

func TestNestedSeq(t *testing.T) {
	sd := SerDes{}

	in := [][]uint32{{1, 2, 3}, {4, 5, 6, 7, 8}, {0xFFFFFFFE, 0xFFFFFFFF}}
	SerializeSeq[[]uint32](&sd, in, true, func(in []uint32) {
		SerializeSeq[uint32](&sd, in, false, func(in uint32) {
			SerializeInt(&sd, in)
		})
	})

	out := [][]uint32{}
	DeserializeSeq[[]uint32](&sd, &out, true, func(out *[]uint32) {
		DeserializeSeq[uint32](&sd, out, false, func(out *uint32) {
			DeserializeInt(&sd, out)
		})
	})

	assert.Empty(t, sd.Errors)
	assert.Equal(t, in, out)
}

func TestNestedMap(t *testing.T) {
	sd := SerDes{}

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

	SerializeMap[int8, map[uint16]uint64](&sd, in, true, func(in int8) {
		SerializeInt(&sd, in)
	}, func(in map[uint16]uint64) {
		SerializeMap[uint16, uint64](&sd, in, false, func(in uint16) {
			SerializeInt(&sd, in)
		}, func(in uint64) {
			SerializeInt(&sd, in)
		})
	})

	out := map[int8]map[uint16]uint64{}

	DeserializeMap[int8, map[uint16]uint64](&sd, &out, true, func(out *int8) {
		DeserializeInt(&sd, out)
	}, func(out *map[uint16]uint64) {
		*out = map[uint16]uint64{}

		DeserializeMap[uint16, uint64](&sd, out, false, func(out *uint16) {
			DeserializeInt(&sd, out)
		}, func(out *uint64) {
			DeserializeInt(&sd, out)
		})
	})

	assert.Empty(t, sd.Errors)
	assert.Equal(t, in, out)
}

func TestErrorOnNonPointerDeserialization(t *testing.T) {
	sd := SerDes{}
	DeserializeInt(&sd, uint32(0))

	assert.Error(t, &sd.Errors)
}
