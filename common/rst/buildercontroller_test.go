package rst

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"golang.org/x/sync/errgroup"
)

func TestRequestBuildWalkMultiplexer_CloseWaitsForInputsToDrain(t *testing.T) {
	mux := newRequestBuildWalkMultiplexer(context.Background(), 1)
	input := make(chan *filesystem.StreamPathResult, 1)
	waitForInput := mux.AddWalks([]<-chan *filesystem.StreamPathResult{input})

	closeDone := make(chan struct{})
	go func() {
		mux.Close()
		close(closeDone)
	}()

	input <- &filesystem.StreamPathResult{Path: "/test-path"}
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

func TestRequestBuildWalkMultiplexer_ForwardsFromMultipleInputs(t *testing.T) {
	mux := newRequestBuildWalkMultiplexer(context.Background(), 2)
	inputA := make(chan *filesystem.StreamPathResult, 1)
	inputB := make(chan *filesystem.StreamPathResult, 1)
	waitForInputs := mux.AddWalks([]<-chan *filesystem.StreamPathResult{inputA, inputB})

	inputA <- &filesystem.StreamPathResult{Path: "/path-a"}
	inputB <- &filesystem.StreamPathResult{Path: "/path-b"}
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

func TestRequestBuildWorkerPool_StartAndWaitAggregatesResult(t *testing.T) {
	ctx := context.Background()
	g, gCtx := errgroup.WithContext(ctx)
	walkCh := make(chan *filesystem.StreamPathResult)
	close(walkCh)

	pool := &requestBuildWorkerPool{
		group:     g,
		ctx:       gCtx,
		parentCtx: ctx,
		workerBase: &requestBuilderWorker{
			walkCh: walkCh,
			result: &requestBuilderWorkerResult{
				Submitted: 3,
				Errors:    1,
				Conflicts: 2,
			},
		},
		getSubmissionQueueDepth: func() int { return 8 },
	}

	pool.Start()
	result, err := pool.Wait()
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, &requestBuilderWorkerResult{
		Submitted: 3,
		Errors:    1,
		Conflicts: 2,
	}, result)
	assert.Equal(t, 1, pool.workersStarted)
	require.Len(t, pool.workers, 1)
}

func TestRequestBuildWorkerPool_ScalesWhenSubmissionQueueStaysLow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, gCtx := errgroup.WithContext(ctx)
	walkCh := make(chan *filesystem.StreamPathResult)

	pool := &requestBuildWorkerPool{
		group:     g,
		ctx:       gCtx,
		parentCtx: ctx,
		workerBase: &requestBuilderWorker{
			walkCh: walkCh,
			result: &requestBuilderWorkerResult{},
		},
		getSubmissionQueueDepth: func() int { return 0 },
	}

	pool.Start()

	require.Eventually(t, func() bool {
		return pool.workersStarted >= 2
	}, time.Second, 20*time.Millisecond)

	close(walkCh)

	result, err := pool.Wait()
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.GreaterOrEqual(t, pool.workersStarted, 2)
	assert.Len(t, pool.workers, pool.workersStarted)
}
