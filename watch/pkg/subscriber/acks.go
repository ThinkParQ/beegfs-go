package subscriber

import "fmt"

type Checkpointer interface {
	Store(metaID uint32, seqID uint64) error
	Retrieve() (map[uint32]uint64, error)
}

func NewDiskStore(path string) (*DiskStore, error) {
	return &DiskStore{}, nil
}

type DiskStore struct {
}

func (d *DiskStore) Store(metaID uint32, seqID uint64) error {
	return fmt.Errorf("not implemented")
}

func (d *DiskStore) Retrieve() (map[uint32]uint64, error) {
	return nil, fmt.Errorf("not implemented")
}
