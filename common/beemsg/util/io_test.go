package util

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/v8/common/beemsg/beeserde"
)

type testMsg struct {
	fieldA uint32
	fieldB []byte

	// helper field for testing MsgFeatureFlags being correctly processed
	flags uint16
}

func (msg *testMsg) MsgId() uint16 {
	return 12345
}

var serdeFail bool = false

func (msg *testMsg) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeInt(s, msg.fieldA)
	beeserde.SerializeCStr(s, msg.fieldB, 0)

	if serdeFail {
		s.Fail(fmt.Errorf("expected fail"))
	}

	s.MsgFeatureFlags = msg.flags
}

func (msg *testMsg) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeInt(d, &msg.fieldA)
	beeserde.DeserializeCStr(d, &msg.fieldB, 0)

	if serdeFail {
		d.Fail(fmt.Errorf("expected fail"))
	}

	msg.flags = d.MsgFeatureFlags
}

// Test writing a message to a io.Writer and reading it from a io.Reader
func TestReadWrite(t *testing.T) {
	in := testMsg{fieldA: 123, fieldB: []byte{1, 2, 3}, flags: 50000}
	buf := bytes.Buffer{}
	out := testMsg{}
	ctx, cancel := context.WithCancel(context.Background())

	// No error
	assert.NoError(t, WriteTo(ctx, &buf, &in))
	assert.NoError(t, ReadFrom(ctx, &buf, &out))
	assert.Equal(t, in, out)

	// Deserialization error
	assert.NoError(t, WriteTo(ctx, &buf, &in))
	serdeFail = true
	err := ReadFrom(ctx, &buf, &out)
	assert.Error(t, err)
	// Deserialization error should NOT contain ErrBeeMsgWrite
	assert.NotErrorIs(t, err, ErrBeeMsgWrite)

	// Serialization error should NOT contain ErrBeeMsgWrite
	assert.NotErrorIs(t, WriteTo(ctx, &buf, &in), ErrBeeMsgWrite)

	// Cancelling the context will error out of the connection related parts, therefore the write error
	// must contain ErrBeeMsgWrite
	cancel()
	serdeFail = false

	assert.ErrorIs(t, WriteTo(ctx, &buf, &in), ErrBeeMsgWrite)
	assert.NotErrorIs(t, ReadFrom(ctx, &buf, &out), ErrBeeMsgWrite)
}

func TestGoWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	err := goWithContext(ctx, func() error {
		return nil
	})
	assert.NoError(t, err)

	err = goWithContext(ctx, func() error {
		return fmt.Errorf("some error")
	})

	assert.Error(t, err)
	assert.NoError(t, ctx.Err())

	cancel()
	err = goWithContext(ctx, func() error {
		return nil
	})

	assert.Error(t, err)
	assert.Equal(t, context.Canceled, ctx.Err())
}

// Dummy io.Writer and io.Reader implementation that just sleeps for one second
type blockingReadWriter struct{}

func (w *blockingReadWriter) Write(p []byte) (int, error) {
	time.Sleep(time.Second)
	return 0, nil
}

func (w *blockingReadWriter) Read(p []byte) (int, error) {
	time.Sleep(time.Second)
	return 0, nil
}

// Test that Write() aborts when ctx hits timeout
func TestWriteTimeout(t *testing.T) {
	in := testMsg{fieldA: 123, fieldB: []byte{1, 2, 3}}
	buf := blockingReadWriter{}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	res := WriteTo(ctx, &buf, &in)

	assert.Equal(t, context.DeadlineExceeded, ctx.Err())
	assert.NotEqual(t, nil, res)
}

// Test that Read() aborts when ctx hits timeout
func TestReadTimeout(t *testing.T) {
	out := testMsg{}
	buf := blockingReadWriter{}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	res := ReadFrom(ctx, &buf, &out)

	assert.Equal(t, context.DeadlineExceeded, ctx.Err())
	assert.NotEqual(t, nil, res)
}
