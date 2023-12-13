package beemsg

import (
	"sync"
)

type queue struct {
	mutex sync.Mutex
	data  []any
}

func (store *queue) tryGet() any {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	if len(store.data) == 0 {
		return nil
	}

	conn := store.data[0]
	store.data = store.data[1:]

	return conn
}

func (store *queue) put(conn any) {
	store.mutex.Lock()
	defer store.mutex.Unlock()

	store.data = append(store.data, conn)
}
