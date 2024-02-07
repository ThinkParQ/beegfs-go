package beemsg

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/gobee/beemsg/beeserde"
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

func (msg *testMsg) Serialize(sd *beeserde.SerDes) {
	beeserde.SerializeInt(sd, msg.fieldA)
	beeserde.SerializeCStr(sd, msg.fieldB, 0)

	sd.MsgFeatureFlags = msg.flags
}

func (msg *testMsg) Deserialize(sd *beeserde.SerDes) {
	beeserde.DeserializeInt(sd, &msg.fieldA)
	beeserde.DeserializeCStr(sd, &msg.fieldB, 0)

	msg.flags = sd.MsgFeatureFlags
}

// Test writing a message to a io.Writer and reading it from a io.Reader
func TestReadWrite(t *testing.T) {
	in := testMsg{fieldA: 123, fieldB: []byte{1, 2, 3}, flags: 50000}
	buf := bytes.Buffer{}
	out := testMsg{}
	ctx := context.Background()

	assert.NoError(t, WriteTo(ctx, &buf, &in))
	assert.NoError(t, ReadFrom(ctx, &buf, &out))

	assert.Equal(t, in, out)
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
