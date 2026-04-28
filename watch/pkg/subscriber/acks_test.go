package subscriber

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestDiskStore_MissingFile(t *testing.T) {
	dir := t.TempDir()
	ds, err := NewDiskStore(filepath.Join(dir, "acks.json"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got, err := ds.Retrieve()
	if err != nil {
		t.Fatalf("Retrieve error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty map, got %v", got)
	}
}

func TestDiskStore_StoreRetrieveRoundtrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "acks.json")
	ds, err := NewDiskStore(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := ds.Store(1, 42); err != nil {
		t.Fatal(err)
	}

	got, err := ds.Retrieve()
	if err != nil {
		t.Fatal(err)
	}
	if got[1] != 42 {
		t.Fatalf("expected seqID 42 for metaID 1, got %d", got[1])
	}
}

func TestDiskStore_MultipleMetaIDs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "acks.json")
	ds, err := NewDiskStore(path)
	if err != nil {
		t.Fatal(err)
	}

	if err := ds.Store(1, 10); err != nil {
		t.Fatal(err)
	}
	if err := ds.Store(2, 20); err != nil {
		t.Fatal(err)
	}
	if err := ds.Store(1, 99); err != nil { // overwrite meta 1
		t.Fatal(err)
	}

	got, err := ds.Retrieve()
	if err != nil {
		t.Fatal(err)
	}
	if got[1] != 99 {
		t.Fatalf("expected seqID 99 for metaID 1, got %d", got[1])
	}
	if got[2] != 20 {
		t.Fatalf("expected seqID 20 for metaID 2, got %d", got[2])
	}
}

func TestDiskStore_PersistsAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "acks.json")

	ds1, err := NewDiskStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := ds1.Store(3, 777); err != nil {
		t.Fatal(err)
	}

	// Simulate a restart by creating a new DiskStore from the same path.
	ds2, err := NewDiskStore(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := ds2.Retrieve()
	if err != nil {
		t.Fatal(err)
	}
	if got[3] != 777 {
		t.Fatalf("expected seqID 777 for metaID 3 after restart, got %d", got[3])
	}
}

func TestDiskStore_MalformedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "acks.json")
	if err := os.WriteFile(path, []byte("not json"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := NewDiskStore(path)
	if err == nil {
		t.Fatal("expected error for malformed file, got nil")
	}
}

func TestDiskStore_UnknownVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "acks.json")
	if err := os.WriteFile(path, []byte(`{"version":999,"entries":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := NewDiskStore(path)
	if err == nil {
		t.Fatal("expected error for unknown version, got nil")
	}
}

func TestDiskStore_ConcurrentStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "acks.json")
	ds, err := NewDiskStore(path)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := range uint32(10) {
		wg.Add(1)
		go func(metaID uint32) {
			defer wg.Done()
			for seq := range uint64(20) {
				if err := ds.Store(metaID, seq); err != nil {
					t.Errorf("Store(%d, %d): %v", metaID, seq, err)
				}
			}
		}(i)
	}
	wg.Wait()

	got, err := ds.Retrieve()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 {
		t.Fatalf("expected 10 entries, got %d", len(got))
	}
}
