package filesystem

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWalkMultiplexer_ForwardsFromMultipleInputs(t *testing.T) {
	mux := NewWalkMultiplexer(context.Background(), 2)
	inputA := make(chan *StreamPathResult, 1)
	inputB := make(chan *StreamPathResult, 1)
	waitForInputs := mux.AddWalks([]<-chan *StreamPathResult{inputA, inputB})

	inputA <- &StreamPathResult{Path: "/path-a"}
	inputB <- &StreamPathResult{Path: "/path-b"}
	close(inputA)
	close(inputB)

	waitForInputs()
	mux.Close()

	received := map[string]bool{}
	for result := range mux.Output() {
		require.NotNil(t, result)
		received[result.Path] = true
	}

	assert.Equal(t, map[string]bool{
		"/path-a": true,
		"/path-b": true,
	}, received)
}

func TestWalkMultiplexer_CloseWaitsForInputsToDrain(t *testing.T) {
	mux := NewWalkMultiplexer(context.Background(), 1)
	input := make(chan *StreamPathResult, 1)
	waitForInput := mux.AddWalks([]<-chan *StreamPathResult{input})

	closeDone := make(chan struct{})
	go func() {
		mux.Close()
		close(closeDone)
	}()

	assertNotClosed(t, closeDone)

	input <- &StreamPathResult{Path: "/test-path"}
	close(input)
	waitForInput()

	select {
	case result, ok := <-mux.Output():
		require.True(t, ok)
		require.NotNil(t, result)
		assert.Equal(t, "/test-path", result.Path)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for multiplexed walk result")
	}

	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for multiplexer close")
	}

	_, ok := <-mux.Output()
	assert.False(t, ok)
}

func TestWalkMultiplexer_AddWalksAfterCloseIsNoop(t *testing.T) {
	mux := NewWalkMultiplexer(context.Background(), 1)
	mux.Close()

	input := make(chan *StreamPathResult, 1)
	input <- &StreamPathResult{Path: "/ignored"}
	close(input)

	waitForInput := mux.AddWalks([]<-chan *StreamPathResult{input})
	waitForInput()

	_, ok := <-mux.Output()
	assert.False(t, ok)
}

func TestWalkMultiplexer_ContextCancellationStopsInputs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	mux := NewWalkMultiplexer(ctx, 1)
	input := make(chan *StreamPathResult)
	waitForInput := mux.AddWalks([]<-chan *StreamPathResult{input})

	cancel()

	waitDone := make(chan struct{})
	go func() {
		waitForInput()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for input goroutine after context cancellation")
	}

	closeDone := make(chan struct{})
	go func() {
		mux.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for close after context cancellation")
	}
}

func TestWalkMultiplexer_MinimumOutputBufferSize(t *testing.T) {
	mux := NewWalkMultiplexer(context.Background(), 0)
	input := make(chan *StreamPathResult, 1)
	waitForInput := mux.AddWalks([]<-chan *StreamPathResult{input})

	input <- &StreamPathResult{Path: "/buffered"}
	close(input)

	waitDone := make(chan struct{})
	go func() {
		waitForInput()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for input with minimum output buffer")
	}

	mux.Close()
	result, ok := <-mux.Output()
	require.True(t, ok)
	require.NotNil(t, result)
	assert.Equal(t, "/buffered", result.Path)
}

func assertNotClosed(t *testing.T, ch <-chan struct{}) {
	t.Helper()

	select {
	case <-ch:
		t.Fatal("channel closed before expected")
	case <-time.After(10 * time.Millisecond):
	}
}
