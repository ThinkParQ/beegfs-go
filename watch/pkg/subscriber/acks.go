package subscriber

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"sync"
)

type Checkpointer interface {
	Store(metaID uint32, seqID uint64) error
	Retrieve() (map[uint32]uint64, error)
}

// diskFormatVersion is the current on-disk checkpoint format version. Increment this if the format
// changes in a way that is not backwards compatible with older readers.
const diskFormatVersion = 1

// diskCheckpoint is the top-level on-disk structure for the checkpoint file.
type diskCheckpoint struct {
	Version uint        `json:"version"`
	Entries []diskEntry `json:"entries"`
}

// diskEntry is the on-disk representation of a single metadata server checkpoint.
type diskEntry struct {
	MetaID uint32 `json:"meta_id"`
	SeqID  uint64 `json:"seq_id"`
}

// NewDiskStore returns a DiskStore backed by the file at path. If the file exists it is read and
// parsed; a missing file is treated as an empty checkpoint (valid first-run state). An error is
// returned only when the file exists but cannot be read or parsed. If needed it will create
// directory components of path.
func NewDiskStore(path string) (*DiskStore, error) {
	state := make(map[uint32]uint64)

	err := os.MkdirAll(filepath.Dir(path), os.FileMode(0700))
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("reading checkpoint file: %w", err)
	}

	if len(data) > 0 {
		var cp diskCheckpoint
		if err := json.Unmarshal(data, &cp); err != nil {
			return nil, fmt.Errorf("parsing checkpoint file: %w", err)
		}
		if cp.Version != diskFormatVersion {
			return nil, fmt.Errorf("unsupported checkpoint format version %d (expected %d)", cp.Version, diskFormatVersion)
		}
		for _, e := range cp.Entries {
			state[e.MetaID] = e.SeqID
		}
	}

	return &DiskStore{
		path:  path,
		state: state,
	}, nil
}

// DiskStore persists acknowledged sequence IDs to a JSON file. It is safe for concurrent use.
type DiskStore struct {
	path  string
	mu    sync.Mutex
	state map[uint32]uint64
}

// Store updates the checkpoint for metaID to seqID and atomically flushes all checkpoints to disk.
func (d *DiskStore) Store(metaID uint32, seqID uint64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.state[metaID] = seqID

	cp := diskCheckpoint{
		Version: diskFormatVersion,
		Entries: make([]diskEntry, 0, len(d.state)),
	}
	for id, seq := range d.state {
		cp.Entries = append(cp.Entries, diskEntry{MetaID: id, SeqID: seq})
	}

	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("serializing checkpoint: %w", err)
	}

	// Write to a temp file in the same directory so the rename is on the same filesystem and
	// therefore atomic. This avoids a window where the checkpoint file is absent or partially
	// written.
	tmp, err := os.CreateTemp(filepath.Dir(d.path), ".checkpoint-*.tmp")
	if err != nil {
		return fmt.Errorf("creating temp checkpoint file: %w", err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("writing checkpoint: %w", err)
	}

	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return fmt.Errorf("syncing checkpoint: %w", err)
	}

	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("closing checkpoint: %w", err)
	}

	if err := os.Rename(tmpName, d.path); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("renaming checkpoint: %w", err)
	}

	return nil
}

// Retrieve returns a copy of the current in-memory checkpoint state.
func (d *DiskStore) Retrieve() (map[uint32]uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	result := make(map[uint32]uint64, len(d.state))
	maps.Copy(result, d.state)
	return result, nil
}
