package entry

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPackedEntryID(t *testing.T) {

	tests := []struct {
		name        string
		input       string
		expected    packedEntryID
		expectedErr bool
	}{
		{
			name:  "valid entry id",
			input: "0-6894F6C0-2",
			expected: packedEntryID{
				A: 0,
				B: 1754592960,
				C: 2,
			},
			expectedErr: false,
		},
		{
			name:  "valid entry id",
			input: "5E-68797BD1-2",
			expected: packedEntryID{
				A: 94,
				B: 1752792017,
				C: 2,
			},
			expectedErr: false,
		},
		{
			name:        "unsupported entry id",
			input:       "root",
			expected:    packedEntryID{},
			expectedErr: true,
		},
		{
			name:        "unsupported entry id",
			input:       "disposal",
			expected:    packedEntryID{},
			expectedErr: true,
		},
		{
			name:        "unsupported entry id",
			input:       "mdisposal",
			expected:    packedEntryID{},
			expectedErr: true,
		},
		{
			name:        "invalid entry id",
			input:       "5E-123456789-2",
			expected:    packedEntryID{},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(t.Name(), func(t *testing.T) {
			packedEntryID, err := newPackedEntryID(tt.input)
			if tt.expectedErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.expected, packedEntryID)
		})
	}
}

func TestParseHexToUint32(t *testing.T) {

	tests := []struct {
		name        string
		input       string
		expected    uint32
		shouldParse bool
	}{
		{
			input:       "0",
			expected:    0,
			shouldParse: true,
		},
		{
			input:       "a",
			expected:    10,
			shouldParse: true,
		},
		{
			input:       "ab",
			expected:    171,
			shouldParse: true,
		},
		{
			input:       "6890F7F2",
			expected:    1754331122,
			shouldParse: true,
		},
		{
			input:       "abg",
			expected:    0,
			shouldParse: false,
		},
		{
			input:       "aGb",
			expected:    0,
			shouldParse: false,
		},
	}

	for _, tt := range tests {
		result, parsed := parseHexToUint32(tt.input)
		assert.Equal(t, tt.shouldParse, parsed)
		assert.Equal(t, tt.expected, result)
	}
}

func TestPackedEntryMap(t *testing.T) {

	packedEntries := newPackedEntryMap(3)
	added, err := packedEntries.Add("0-0-0")
	assert.NoError(t, err)
	assert.True(t, added)

	added, err = packedEntries.Add("0-0-1")
	assert.NoError(t, err)
	assert.True(t, added)

	added, err = packedEntries.Add("0-0-2")
	assert.NoError(t, err)
	assert.True(t, added)

	// Adding the same entry twice should return false:
	added, err = packedEntries.Add("0-0-2")
	assert.NoError(t, err)
	assert.False(t, added)

	// And not evict the oldest entry ID:
	contains, err := packedEntries.Contains("0-0-0")
	assert.NoError(t, err)
	assert.True(t, contains)

	// A fourth unique entry should push out the first:
	added, err = packedEntries.Add("0-0-3")
	assert.NoError(t, err)
	assert.True(t, added)

	contains, err = packedEntries.Contains("0-0-0")
	assert.NoError(t, err)
	assert.False(t, contains)

	// Verify the internal state of the packedEntryMap
	assert.Equal(t, map[packedEntryID]struct{}{
		{A: 0, B: 0, C: 1}: {},
		{A: 0, B: 0, C: 2}: {},
		{A: 0, B: 0, C: 3}: {},
	}, packedEntries.index, "unexpected index state")
	assert.Equal(t, []packedEntryID{
		{A: 0, B: 0, C: 3},
		{A: 0, B: 0, C: 1},
		{A: 0, B: 0, C: 2},
	}, packedEntries.ring, "unexpected ring state")
	assert.Equal(t, 1, packedEntries.pos, "unexpected position")
	assert.Equal(t, 3, packedEntries.size, "unexpected size")
	assert.Equal(t, 3, packedEntries.capacity, "unexpected capacity")

	// Section eviction cycle
	_, _ = packedEntries.Add("0-0-4")
	_, _ = packedEntries.Add("0-0-5")
	contains, _ = packedEntries.Contains("0-0-1")
	assert.False(t, contains) // evicted earlier
	contains, _ = packedEntries.Contains("0-0-5")
	assert.True(t, contains)

	// Invalid
	_, err = packedEntries.Contains("not-hex")
	assert.Error(t, err)
}

func TestPackedEntryMapCapZero(t *testing.T) {
	m := newPackedEntryMap(0)
	added, _ := m.Add("0-0-0")
	assert.True(t, added)
	added, _ = m.Add("0-0-0")
	assert.False(t, added)
	added, _ = m.Add("0-0-1")
	assert.True(t, added)
	ok, _ := m.Contains("0-0-0")
	assert.False(t, ok)
	ok, _ = m.Contains("0-0-1")
	assert.True(t, ok)
}
