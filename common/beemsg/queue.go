package beemsg

import (
	"sync"
)

// A generic, thread safe queue that stores elements of type any
type queue struct {
	mutex sync.Mutex
	data  []any
}

// Tries to pop an element from the front of the queue. Returns nil if the queue is empty
func (store *queue) tryGet() any {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if len(store.data) == 0 {
		return nil
	}

	e := store.data[0]
	store.data = store.data[1:]

	return e
}

// Pushes an element to the back of the queue
func (store *queue) put(e any) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	store.data = append(store.data, e)
}
