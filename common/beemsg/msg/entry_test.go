package msg

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
)

func TestStripePatternSerialize(t *testing.T) {

	stripePattern1 := StripePattern{
		Length:            26,
		Type:              beegfs.StripePatternRaid0,
		HasPoolID:         true,
		Chunksize:         524288,
		StoragePoolID:     1,
		DefaultNumTargets: 1,
		TargetIDs:         nil,
	}

	s1 := beeserde.NewSerializer([]byte{})
	stripePattern1.Serialize(&s1)
	assert.NoError(t, s1.Finish())
	d1 := beeserde.NewDeserializer(s1.Buf.Bytes(), 0)
	outStripePattern1 := StripePattern{}
	outStripePattern1.Deserialize(&d1)
	assert.NoError(t, d1.Finish())
	assert.Equal(t, stripePattern1, outStripePattern1)

	stripePattern2 := StripePattern{
		Length:            34,
		Type:              beegfs.StripePatternBuddyMirror,
		HasPoolID:         true,
		Chunksize:         1048576,
		StoragePoolID:     5,
		DefaultNumTargets: 16,
		TargetIDs:         []uint16{201, 101, 102, 201},
	}

	s2 := beeserde.NewSerializer([]byte{})
	stripePattern2.Serialize(&s2)
	assert.NoError(t, s1.Finish())
	d2 := beeserde.NewDeserializer(s2.Buf.Bytes(), 0)
	outStripePattern2 := StripePattern{}
	outStripePattern2.Deserialize(&d2)
	assert.NoError(t, d2.Finish())
	assert.Equal(t, stripePattern2, outStripePattern2)
}

func TestRemoteStorageTargetSerialize(t *testing.T) {
	var RST RemoteStorageTarget
	val := reflect.ValueOf(RST)
	assert.Equal(t, 6, val.NumField(), "The RemoteStorageTargets struct has changed. Update its Serialize() and Deserialize() methods to support the new major/minor version.")
	typ := reflect.TypeOf(RST)

	assert.Equal(t, reflect.Uint8, typ.Field(0).Type.Kind(), "Field type changed (was the order or type of fields in the struct changed?)")
	assert.Equal(t, reflect.Uint8, typ.Field(1).Type.Kind(), "Field type changed (was the order or type of fields in the struct changed?)")
	assert.Equal(t, reflect.Uint16, typ.Field(2).Type.Kind(), "Field type changed (was the order or type of fields in the struct changed?)")
	assert.Equal(t, reflect.Uint16, typ.Field(3).Type.Kind(), "Field type changed (was the order or type of fields in the struct changed?)")
	assert.Equal(t, reflect.Uint16, typ.Field(4).Type.Kind(), "Field type changed (was the order or type of fields in the struct changed?)")
	assert.Equal(t, reflect.Slice, typ.Field(5).Type.Kind(), "Field type changed (was the order or type of fields in the struct changed?)")
}
