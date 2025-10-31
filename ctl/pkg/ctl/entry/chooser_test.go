package entry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
)

func TestStartRebalancingJobs(t *testing.T) {

	tests := []struct {
		name              string
		entry             *GetEntryCombinedInfo
		srcTargets        map[uint16]struct{}
		srcGroups         map[uint16]struct{}
		dstTargets        []uint16
		dstGroups         []uint16
		wantErr           bool
		wantRebalanceType msg.RebalanceIDType
		wantUnmodifiedIDs []uint16
	}{
		{
			name: "Raid0 Rebalance Needed",
			entry: &GetEntryCombinedInfo{
				Entry: Entry{
					Pattern: patternConfig{
						StripePattern: msg.StripePattern{
							Type:      beegfs.StripePatternRaid0,
							TargetIDs: []uint16{2},
						},
					},
				},
			},
			srcTargets:        map[uint16]struct{}{2: {}},
			dstTargets:        []uint16{3, 4},
			wantRebalanceType: msg.RebalanceIDTypeTarget,
			wantUnmodifiedIDs: []uint16{},
		},
		{
			name: "Raid0 Rebalance Needed (out-of-order targets)",
			entry: &GetEntryCombinedInfo{
				Entry: Entry{
					Pattern: patternConfig{
						StripePattern: msg.StripePattern{
							Type:      beegfs.StripePatternRaid0,
							TargetIDs: []uint16{101, 103, 104, 102},
						},
					},
				},
			},
			srcTargets:        map[uint16]struct{}{103: {}, 104: {}, 203: {}, 204: {}},
			dstTargets:        []uint16{102, 202, 500},
			wantRebalanceType: msg.RebalanceIDTypeTarget,
			wantUnmodifiedIDs: []uint16{101, 102},
		},
		{
			name: "Raid0 Rebalance Needed (out-of-order targets / insufficient dstTargets)",
			entry: &GetEntryCombinedInfo{
				Entry: Entry{
					Pattern: patternConfig{
						StripePattern: msg.StripePattern{
							Type:      beegfs.StripePatternRaid0,
							TargetIDs: []uint16{101, 103, 104, 102},
						},
					},
				},
			},
			srcTargets:        map[uint16]struct{}{103: {}, 104: {}, 203: {}, 204: {}},
			dstTargets:        []uint16{102, 202},
			wantErr:           true,
			wantRebalanceType: msg.RebalanceIDTypeInvalid,
			wantUnmodifiedIDs: []uint16{},
		},
		{
			name: "Raid0 Rebalance Needed (insufficient dstTargets)",
			entry: &GetEntryCombinedInfo{
				Entry: Entry{
					Pattern: patternConfig{
						StripePattern: msg.StripePattern{
							Type:      beegfs.StripePatternRaid0,
							TargetIDs: []uint16{1, 2},
						},
					},
				},
			},
			srcTargets:        map[uint16]struct{}{1: {}, 2: {}},
			dstTargets:        []uint16{3},
			wantErr:           true,
			wantRebalanceType: msg.RebalanceIDTypeInvalid,
			wantUnmodifiedIDs: []uint16{},
		},
		{
			name: "Raid0 Rebalance Needed (insufficient dstTargets due to dstTargets already in stripe)",
			entry: &GetEntryCombinedInfo{
				Entry: Entry{
					Pattern: patternConfig{
						StripePattern: msg.StripePattern{
							Type:      beegfs.StripePatternRaid0,
							TargetIDs: []uint16{1, 2, 3, 4},
						},
					},
				},
			},
			srcTargets:        map[uint16]struct{}{1: {}},
			dstTargets:        []uint16{1, 2, 3, 4},
			wantErr:           true,
			wantRebalanceType: msg.RebalanceIDTypeInvalid,
			wantUnmodifiedIDs: []uint16{},
		},
		{
			name: "BuddyMirror Rebalance Needed",
			entry: &GetEntryCombinedInfo{
				Entry: Entry{
					Pattern: patternConfig{
						StripePattern: msg.StripePattern{
							Type:      beegfs.StripePatternBuddyMirror,
							TargetIDs: []uint16{4, 5, 6},
						},
					},
				},
			},
			srcGroups:         map[uint16]struct{}{5: {}, 6: {}},
			dstGroups:         []uint16{7, 8},
			wantRebalanceType: msg.RebalanceIDTypeGroup,
			wantUnmodifiedIDs: []uint16{4},
		},
		{
			name: "BuddyMirror Rebalance Needed (insufficient dstGroups)",
			entry: &GetEntryCombinedInfo{
				Entry: Entry{
					Pattern: patternConfig{
						StripePattern: msg.StripePattern{
							Type:      beegfs.StripePatternBuddyMirror,
							TargetIDs: []uint16{5, 6, 7},
						},
					},
				},
			},
			srcGroups:         map[uint16]struct{}{5: {}, 6: {}, 7: {}},
			dstGroups:         []uint16{8, 9},
			wantErr:           true,
			wantRebalanceType: msg.RebalanceIDTypeInvalid,
			wantUnmodifiedIDs: []uint16{},
		},
		{
			name: "No Rebalance Needed",
			entry: &GetEntryCombinedInfo{
				Entry: Entry{
					Pattern: patternConfig{
						StripePattern: msg.StripePattern{
							Type:      beegfs.StripePatternRaid0,
							TargetIDs: []uint16{9},
						},
					},
				},
			},
			srcTargets:        map[uint16]struct{}{10: {}},
			dstTargets:        []uint16{11},
			wantRebalanceType: msg.RebalanceIDTypeTarget,
			wantUnmodifiedIDs: []uint16{9},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			rebalanceIDType, srcIDs, dstIDs, unmodifiedIDs, err := getMigrationForEntry(tt.entry, tt.srcTargets, tt.srcGroups, tt.dstTargets, tt.dstGroups)
			if tt.wantErr {
				assert.Error(t, err, "expected an error getting migrations for entry")
				assert.Empty(t, srcIDs, "expected no srcIDs when no rebalance started")
				assert.Empty(t, dstIDs, "expected no dstIDs when no rebalance started")
			} else {
				assert.NoError(t, err, "unexpected error getting migrations for entry")
				assert.Len(t, srcIDs, len(dstIDs), "srcIDs and dstIDs should be the same length")
				assert.Equal(t, tt.wantUnmodifiedIDs, unmodifiedIDs, "unexpected unmodified IDs")
			}

			assert.Equal(t, tt.wantRebalanceType, rebalanceIDType, "unexpected rebalance type")
		})
	}
}
