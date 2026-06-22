package filesystem

import (
	"context"
	"sync"
)

type WalkMultiplexer struct {
	ctx           context.Context
	mergeCh       chan *StreamPathResult
	mergeChClosed bool
	mu            sync.Mutex
	done          *sync.Cond
	activeInputs  int
	closed        bool
}

func NewWalkMultiplexer(ctx context.Context, bufferSize int) *WalkMultiplexer {
	mergeCh := make(chan *StreamPathResult, max(1, bufferSize))
	multiplexer := &WalkMultiplexer{ctx: ctx, mergeCh: mergeCh}
	multiplexer.done = sync.NewCond(&multiplexer.mu)
	return multiplexer
}

func (m *WalkMultiplexer) Output() <-chan *StreamPathResult {
	return m.mergeCh
}

func (m *WalkMultiplexer) AddWalks(walkChs []<-chan *StreamPathResult) func() {
	if len(walkChs) == 0 {
		return func() {}
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return func() {}
	}
	m.activeInputs += len(walkChs)
	m.mu.Unlock()

	var wg sync.WaitGroup
	for _, ch := range walkChs {
		wg.Add(1)
		go func(ch <-chan *StreamPathResult) {
			defer wg.Done()
			defer m.addWalksDone()

			for {
				select {
				case <-m.ctx.Done():
					return
				case walkPath, ok := <-ch:
					if !ok {
						return
					}

					select {
					case <-m.ctx.Done():
						return
					case m.mergeCh <- walkPath:
					}
				}
			}
		}(ch)
	}

	return func() {
		wg.Wait()
	}
}

func (m *WalkMultiplexer) addWalksDone() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.activeInputs--
	if m.closed && m.activeInputs == 0 && !m.mergeChClosed {
		close(m.mergeCh)
		m.mergeChClosed = true
		m.done.Broadcast()
	}
}

func (m *WalkMultiplexer) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true

	if m.activeInputs == 0 && !m.mergeChClosed {
		close(m.mergeCh)
		m.mergeChClosed = true
		m.done.Broadcast()
	}

	for !m.mergeChClosed {
		m.done.Wait()
	}
}
