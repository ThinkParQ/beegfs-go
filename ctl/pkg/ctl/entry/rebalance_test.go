package entry

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
)

func TestStartRebalancingJobs(t *testing.T) {
	defer func() { chunkRebalanceFunc = chunkRebalance }()
	tests := []struct {
		name          string
		entry         *GetEntryCombinedInfo
		srcTargets    map[uint16]struct{}
		srcGroups     map[uint16]struct{}
		dstTargets    []uint16
		dstGroups     []uint16
		dryRun        bool
		wantStarted   bool
		wantErr       bool
		wantCallCount int
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
			srcTargets:    map[uint16]struct{}{2: {}},
			dstTargets:    []uint16{3, 4},
			wantStarted:   true,
			wantCallCount: 1,
		},
		{
			name:   "Raid0 Rebalance Needed (dry run only)",
			dryRun: true,
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
			srcTargets:    map[uint16]struct{}{2: {}},
			dstTargets:    []uint16{3, 4},
			wantStarted:   true, // With a dry run this indicates a rebalance is needed.
			wantCallCount: 0,    // However the rebalance func should not be called.
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
			srcTargets:  map[uint16]struct{}{1: {}, 2: {}},
			dstTargets:  []uint16{3},
			wantStarted: false,
			wantErr:     true,
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
			srcTargets:  map[uint16]struct{}{1: {}},
			dstTargets:  []uint16{1, 2, 3, 4},
			wantStarted: false,
			wantErr:     true,
		},
		{
			name: "BuddyMirror Rebalance Needed",
			entry: &GetEntryCombinedInfo{
				Entry: Entry{
					Pattern: patternConfig{
						StripePattern: msg.StripePattern{
							Type:      beegfs.StripePatternBuddyMirror,
							TargetIDs: []uint16{5, 6},
						},
					},
				},
			},
			srcGroups:     map[uint16]struct{}{5: {}, 6: {}},
			dstGroups:     []uint16{7, 8},
			wantStarted:   true,
			wantCallCount: 1,
		},
		{
			name:   "BuddyMirror Rebalance Needed (dry run only)",
			dryRun: true,
			entry: &GetEntryCombinedInfo{
				Entry: Entry{
					Pattern: patternConfig{
						StripePattern: msg.StripePattern{
							Type:      beegfs.StripePatternBuddyMirror,
							TargetIDs: []uint16{5, 6},
						},
					},
				},
			},
			srcGroups:     map[uint16]struct{}{5: {}, 6: {}},
			dstGroups:     []uint16{7, 8},
			wantStarted:   true, // With a dry run this indicates a rebalance is needed.
			wantCallCount: 0,    // However the rebalance func should not be called.
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
			srcGroups:   map[uint16]struct{}{5: {}, 6: {}, 7: {}},
			dstGroups:   []uint16{8, 9},
			wantStarted: false,
			wantErr:     true,
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
			srcTargets:  map[uint16]struct{}{10: {}},
			dstTargets:  []uint16{11},
			wantStarted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			// Currently called should always be 0 or 1. If a function is called more than 1 time
			// its likely a bug.
			called := 0
			seenDestIDs := make(map[uint16]struct{})
			chunkRebalanceFunc = func(ctx context.Context, entry *GetEntryCombinedInfo, rebalanceIDType msg.RebalanceIDType, srcIDs, destIDs []uint16) error {
				// Verify startRebalancingJobs is making the correct calls to the rebalance func.
				assert.Len(t, srcIDs, len(destIDs), "srcIDs and destIDs should be the same length")
				for _, destID := range destIDs {
					require.NotContains(t, seenDestIDs, destID, "destination ID was already used")
					seenDestIDs[destID] = struct{}{}
				}
				called++
				return nil
			}

			started, err := startRebalancingJobs(context.Background(), tt.entry, tt.srcTargets, tt.srcGroups, tt.dstTargets, tt.dstGroups, tt.dryRun)
			if tt.wantErr {
				assert.Error(t, err, "expected an error starting rebalancing jobs")
			} else {
				assert.NoError(t, err, "unexpected error starting rebalancing jobs")
			}
			assert.Equal(t, tt.wantStarted, started, "unexpected start state for rebalancing jobs")
			assert.Equal(t, tt.wantCallCount, called, "chunkRebalanceFunc was not called the expected number of times")
		})
	}
}
