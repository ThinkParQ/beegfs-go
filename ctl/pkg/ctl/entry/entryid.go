package entry

import (
	"fmt"
	"strings"
	"sync"
)

// packedEntryID is a compact, fixed-size Entry ID: AAAA-BBBB-CCCC (hex -> uint32). This is useful
// for working with entry IDs when a fixed size data structure is needed. This does not currently
// support special entry IDs such as root, disposal, and mdisposal.
type packedEntryID struct{ A, B, C uint32 }

func newPackedEntryID(entryID string) (packedEntryID, error) {
	a, remaining, ok := strings.Cut(entryID, "-")
	if !ok || len(a) == 0 || len(a) > 8 {
		return packedEntryID{}, fmt.Errorf("unable to pack entry id %q: unable to split first section", entryID)
	}
	b, c, ok := strings.Cut(remaining, "-")
	if !ok || len(b) == 0 || len(b) > 8 || len(c) == 0 || len(c) > 8 {
		return packedEntryID{}, fmt.Errorf("unable to pack entry id %q: unable to split second and third sections", entryID)
	}
	pa, ok := parseHexToUint32(a)
	if !ok {
		return packedEntryID{}, fmt.Errorf("unable to pack entry id %q: unable to parse first section to uint32", entryID)
	}
	pb, ok := parseHexToUint32(b)
	if !ok {
		return packedEntryID{}, fmt.Errorf("unable to pack entry id %q: unable to parse second section to uint32", entryID)
	}
	pc, ok := parseHexToUint32(c)
	if !ok {
		return packedEntryID{}, fmt.Errorf("unable to pack entry id %q: unable to parse third section to uint32", entryID)
	}
	return packedEntryID{pa, pb, pc}, nil
}

// parseHexToUint32 parses an ASCII hexadecimal string into a uint32 with zero heap allocations.
// This is specialized for hex and faster than strconv.ParseUint() for this narrow use case.
func parseHexToUint32(s string) (uint32, bool) {
	var v uint32
	for i := range len(s) {
		c := s[i]
		switch {
		case '0' <= c && c <= '9':
			v = (v << 4) | uint32(c-'0')
		case 'a' <= c && c <= 'f':
			v = (v << 4) | uint32(c-'a'+10)
		case 'A' <= c && c <= 'F':
			v = (v << 4) | uint32(c-'A'+10)
		default:
			return 0, false
		}
	}
	return v, true
}

// packedEntryMap is a fixed-capacity, thread-safe, FIFO-evicting map of packedEntryIDs.
type packedEntryMap struct {
	mu sync.RWMutex
	// ring buffer used to track insertion order for FIFO eviction when size >= capacity.
	ring []packedEntryID
	// index used for O(1) lookup of entries.
	index    map[packedEntryID]struct{}
	pos      int
	size     int
	capacity int
}

func newPackedEntryMap(capacity int) *packedEntryMap {
	if capacity <= 0 {
		capacity = 1
	}
	return &packedEntryMap{
		ring:     make([]packedEntryID, capacity),
		index:    make(map[packedEntryID]struct{}, capacity),
		capacity: capacity,
	}
}

func (s *packedEntryMap) Add(entryID string) (bool, error) {
	packedID, err := newPackedEntryID(entryID)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.index[packedID]; ok {
		return false, nil
	}

	if s.size >= s.capacity {
		// Evict oldest (at s.pos), then insert.
		evict := s.ring[s.pos]
		delete(s.index, evict)
	} else {
		s.size++
	}

	s.ring[s.pos] = packedID
	s.index[packedID] = struct{}{}
	s.pos = (s.pos + 1) % s.capacity
	return true, nil
}

func (s *packedEntryMap) Contains(entryID string) (bool, error) {
	packedID, err := newPackedEntryID(entryID)
	if err != nil {
		return false, err
	}
	s.mu.RLock()
	_, ok := s.index[packedID]
	s.mu.RUnlock()
	return ok, nil
}
