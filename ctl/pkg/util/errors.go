package util

import "sync"

// CombineErrChans merges multiple receiver error channels into one. It is lightweight and cannot
// deadlock since it does not read from the channels.
//
// The returned channel emits every error from each channel and is closed once all channels have
// been drained. If channels has no error channels or is nil then an already-closed channel will be
// returned
func CombineErrChans(channels ...<-chan error) <-chan error {
	var inputs []<-chan error
	for _, ch := range channels {
		if ch != nil {
			inputs = append(inputs, ch)
		}
	}

	if len(inputs) == 0 {
		closed := make(chan error)
		close(closed)
		return closed
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(inputs))
	wg.Add(len(inputs))
	for _, ch := range inputs {
		go func(c <-chan error) {
			defer wg.Done()
			for err := range c {
				errs <- err
			}
		}(ch)
	}

	go func() {
		wg.Wait()
		close(errs)
	}()

	return errs
}
