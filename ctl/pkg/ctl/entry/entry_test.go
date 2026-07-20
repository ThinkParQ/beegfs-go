package entry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
)

func ptr[T any](v T) *T {
	return &v
}

// TestResolveRstPatternUpdate verifies the decision logic behind SetFileRstPattern: which rst
// fields are carried over from the current configuration, which are overwritten, and whether a
// SetFilePatternRequest needs to be sent at all.
func TestResolveRstPatternUpdate(t *testing.T) {
	tests := []struct {
		name         string
		current      msg.RemoteStorageTarget
		rstIds       []uint32
		cooldownSecs *uint16
		wantRSTIDs   []uint32
		wantCooldown uint16
		wantUpdate   bool
	}{
		{
			// The caller expressed no intent at all, so nothing should be sent.
			name:       "no intent is a no-op",
			current:    msg.RemoteStorageTarget{RSTIDs: []uint32{1}, CoolDownPeriod: 30},
			wantRSTIDs: []uint32{1},
			// Cooldown and IDs must be carried over untouched even though no request is sent.
			wantCooldown: 30,
			wantUpdate:   false,
		},
		{
			// This is the regression that motivated the rstIds != nil guard: setting only the
			// cooldown must not strip the file's configured rst IDs.
			name:         "cooldown only preserves existing rst ids",
			current:      msg.RemoteStorageTarget{RSTIDs: []uint32{1, 2}, CoolDownPeriod: 30},
			cooldownSecs: ptr[uint16](60),
			wantRSTIDs:   []uint32{1, 2},
			wantCooldown: 60,
			wantUpdate:   true,
		},
		{
			name:         "cooldown only on a file with no rst ids",
			current:      msg.RemoteStorageTarget{RSTIDs: []uint32{}, CoolDownPeriod: 30},
			cooldownSecs: ptr[uint16](60),
			wantRSTIDs:   []uint32{},
			wantCooldown: 60,
			wantUpdate:   true,
		},
		{
			name:         "cooldown matching the current value is a no-op",
			current:      msg.RemoteStorageTarget{RSTIDs: []uint32{1}, CoolDownPeriod: 30},
			cooldownSecs: ptr[uint16](30),
			wantRSTIDs:   []uint32{1},
			wantCooldown: 30,
			wantUpdate:   false,
		},
		{
			name:         "rst ids only preserves the existing cooldown",
			current:      msg.RemoteStorageTarget{RSTIDs: []uint32{1}, CoolDownPeriod: 30},
			rstIds:       []uint32{2},
			wantRSTIDs:   []uint32{2},
			wantCooldown: 30,
			wantUpdate:   true,
		},
		{
			name:         "rst ids matching the current value is a no-op",
			current:      msg.RemoteStorageTarget{RSTIDs: []uint32{1, 2}, CoolDownPeriod: 30},
			rstIds:       []uint32{2, 1},
			wantRSTIDs:   []uint32{1, 2},
			wantCooldown: 30,
			wantUpdate:   false,
		},
		{
			// A non-nil empty slice is how a caller asks to clear the rst IDs. This is what the
			// rollback in prepareUpdateFileRstPattern relies on to undo itself on a file that had
			// no rst IDs to begin with.
			name:         "empty slice clears the rst ids",
			current:      msg.RemoteStorageTarget{RSTIDs: []uint32{1}, CoolDownPeriod: 30},
			rstIds:       []uint32{},
			wantRSTIDs:   []uint32{},
			wantCooldown: 30,
			wantUpdate:   true,
		},
		{
			name:         "clearing rst ids that are already empty is a no-op",
			current:      msg.RemoteStorageTarget{RSTIDs: []uint32{}, CoolDownPeriod: 30},
			rstIds:       []uint32{},
			wantRSTIDs:   []uint32{},
			wantCooldown: 30,
			wantUpdate:   false,
		},
		{
			name:         "both rst ids and cooldown are applied together",
			current:      msg.RemoteStorageTarget{RSTIDs: []uint32{1}, CoolDownPeriod: 30},
			rstIds:       []uint32{2, 3},
			cooldownSecs: ptr[uint16](60),
			wantRSTIDs:   []uint32{2, 3},
			wantCooldown: 60,
			wantUpdate:   true,
		},
		{
			// Only one of the two fields actually differs, but the request still has to be sent
			// carrying both.
			name:         "an update is required when only one field differs",
			current:      msg.RemoteStorageTarget{RSTIDs: []uint32{1}, CoolDownPeriod: 30},
			rstIds:       []uint32{1},
			cooldownSecs: ptr[uint16](60),
			wantRSTIDs:   []uint32{1},
			wantCooldown: 60,
			wantUpdate:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Unrelated rst fields must survive untouched regardless of what was requested.
			tt.current.FilePolicies = 7
			tt.current.Reserved = 9

			got, gotUpdate := resolveRstPatternUpdate(tt.current, tt.rstIds, tt.cooldownSecs)

			assert.Equal(t, tt.wantUpdate, gotUpdate)
			assert.Equal(t, tt.wantRSTIDs, got.RSTIDs)
			assert.Equal(t, tt.wantCooldown, got.CoolDownPeriod)
			assert.Equal(t, uint16(7), got.FilePolicies)
			assert.Equal(t, uint16(9), got.Reserved)
		})
	}
}

// TestResolveRstPatternUpdateDoesNotMutateCurrent verifies the caller's configuration is left
// alone, since prepareUpdateFileRstPattern derives its rollback values from the same struct it
// passes in.
func TestResolveRstPatternUpdateDoesNotMutateCurrent(t *testing.T) {
	current := msg.RemoteStorageTarget{RSTIDs: []uint32{1, 2}, CoolDownPeriod: 30}

	_, updateRequired := resolveRstPatternUpdate(current, []uint32{3}, ptr[uint16](60))

	require.True(t, updateRequired)
	assert.Equal(t, []uint32{1, 2}, current.RSTIDs)
	assert.Equal(t, uint16(30), current.CoolDownPeriod)
}

// TestResolveRstPatternUpdateReachesServer verifies the resolved configuration actually carries rst
// intent on the wire. The server skips rst processing entirely when the serialized major version is
// still 0, which happens when every rst field is zero valued and RSTIDs is nil. Clearing the
// cooldown on a file with no rst IDs is the case that would otherwise be silently dropped.
func TestResolveRstPatternUpdateReachesServer(t *testing.T) {
	tests := []struct {
		name    string
		current msg.RemoteStorageTarget
		rstIds  []uint32
	}{
		{
			name:    "clearing the cooldown with empty rst ids",
			current: msg.RemoteStorageTarget{RSTIDs: []uint32{}, CoolDownPeriod: 30},
		},
		{
			name:    "clearing both the cooldown and the rst ids",
			current: msg.RemoteStorageTarget{RSTIDs: []uint32{1}, CoolDownPeriod: 30},
			rstIds:  []uint32{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, updateRequired := resolveRstPatternUpdate(tt.current, tt.rstIds, ptr[uint16](0))
			require.True(t, updateRequired)
			require.Zero(t, got.CoolDownPeriod)

			s := beeserde.NewSerializer([]byte{})
			got.Serialize(&s)
			require.NoError(t, s.Finish())

			// majorVersion is the first serialized field and is not exported, so assert on the
			// leading byte. A 0 here means the meta server would ignore the request.
			require.NotEmpty(t, s.Buf.Bytes())
			assert.Equal(t, byte(1), s.Buf.Bytes()[0], "serialized rst config would be skipped by the server")
		})
	}
}

// TestResolveRstPatternUpdateNilIsNotAClear guards the distinction the rollback depends on: nil
// means "leave unchanged" and must never be confused with the empty slice that means "clear".
func TestResolveRstPatternUpdateNilIsNotAClear(t *testing.T) {
	current := msg.RemoteStorageTarget{RSTIDs: []uint32{1, 2}, CoolDownPeriod: 30}

	got, updateRequired := resolveRstPatternUpdate(current, nil, ptr[uint16](60))

	require.True(t, updateRequired)
	assert.Equal(t, []uint32{1, 2}, got.RSTIDs)

	got, updateRequired = resolveRstPatternUpdate(current, []uint32{}, ptr[uint16](60))

	require.True(t, updateRequired)
	assert.Empty(t, got.RSTIDs)
	assert.NotNil(t, got.RSTIDs)
}

func TestRstIdsMatch(t *testing.T) {
	tests := []struct {
		name string
		old  []uint32
		new  []uint32
		want bool
	}{
		{name: "both nil", old: nil, new: nil, want: true},
		{name: "nil and empty", old: nil, new: []uint32{}, want: true},
		{name: "equal single", old: []uint32{1}, new: []uint32{1}, want: true},
		{name: "equal out of order", old: []uint32{1, 2, 3}, new: []uint32{3, 1, 2}, want: true},
		{name: "equal with duplicates", old: []uint32{1, 1, 2}, new: []uint32{2, 1, 1}, want: true},
		{name: "different values", old: []uint32{1}, new: []uint32{2}, want: false},
		{name: "different lengths", old: []uint32{1, 2}, new: []uint32{1}, want: false},
		{name: "same length different duplicates", old: []uint32{1, 1, 2}, new: []uint32{1, 2, 2}, want: false},
		{name: "populated and empty", old: []uint32{1}, new: []uint32{}, want: false},
		{name: "empty and populated", old: []uint32{}, new: []uint32{1}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, rstIdsMatch(tt.old, tt.new))
		})
	}
}
