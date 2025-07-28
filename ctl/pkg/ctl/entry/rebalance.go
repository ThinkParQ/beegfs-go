package entry

import (
	"context"
	"fmt"
	"math/rand/v2"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

var chunkRebalanceFunc = chunkRebalance

// startRebalancingJobs handles starting chunk rebalancing jobs to migrate data away from the
// specified srcTargets/Groups to dstTargets/dstGroups. It returns true if at least one job was
// started (an error might also be returned), or if dryRun was set and at least once mirror/target
// needs rebalancing for this entry.
//
// IMPORTANT: dstTargets and dstGroups should not contain any duplicates but must be provided as
// slices to optimize copying the slice for randomization.
func startRebalancingJobs(
	ctx context.Context,
	entry *GetEntryCombinedInfo,
	srcTargets map[uint16]struct{},
	srcGroups map[uint16]struct{},
	dstTargets []uint16,
	dstGroups []uint16,
	dryRun bool,
) (bool, error) {

	var shuffledIDs []uint16
	var shuffledIdx int
	// getRandomID lazily initializes a shuffled slice of IDs from the destination IDs upon first
	// use. Each call returns a random destination ID that is not already in the provided pattern.
	getRandomID := func(fromDstIDs []uint16, idsAlreadyInStripePattern []uint16) (uint16, error) {
		if shuffledIDs == nil {
			shuffledIDs = make([]uint16, len(fromDstIDs))
			copy(shuffledIDs, fromDstIDs)
			rand.Shuffle(len(shuffledIDs), func(i, j int) {
				shuffledIDs[i], shuffledIDs[j] = shuffledIDs[j], shuffledIDs[i]
			})
		}
		// Alternatively we could allocate a map for inUseIDs to speed up lookup, but for small sets
		// (e.g., 4 targets in the default stripe pattern), a nested loop is likely faster and
		// avoids allocation. Also, in the common case where none of the shuffled IDs are already in
		// use, this runs in O(N).
	nextCandidate:
		for ; shuffledIdx < len(shuffledIDs); shuffledIdx++ {
			for _, inUseID := range idsAlreadyInStripePattern {
				if shuffledIDs[shuffledIdx] == inUseID {
					continue nextCandidate
				}
			}
			candidateID := shuffledIDs[shuffledIdx]
			// The loop won't increment the shuffledIdx with the early return. Do that manually
			// before returning.
			shuffledIdx++
			return candidateID, nil
		}
		// Because of the below length checks, this error should only occur if srcTargets/Groups
		// contains IDs already found in the entry's stripe pattern and are also in
		// dstTargets/Groups, which would be unusual for the caller to request.
		return 0, fmt.Errorf("list of destination IDs does not contain enough unique IDs not already in use for this entry (most likely there is overlap between the source and destination IDs)")
	}

	rebalanceStarted := false
	if entry.Entry.Pattern.Type == beegfs.StripePatternBuddyMirror {
		if len(entry.Entry.Pattern.TargetIDs) > len(dstGroups) {
			return rebalanceStarted, fmt.Errorf("insufficient buddy groups in the destination pool to rebalance entry (entry has %d groups / destination pool has %d groups)", len(entry.Entry.Pattern.TargetIDs), len(dstGroups))
		}
		for _, group := range entry.Entry.Pattern.TargetIDs {
			if _, ok := srcGroups[group]; ok {
				if dryRun {
					return true, nil
				}
				randomID, err := getRandomID(dstGroups, entry.Entry.Pattern.TargetIDs)
				if err != nil {
					return rebalanceStarted, err
				}
				if err := chunkRebalanceFunc(ctx, entry, group, randomID); err != nil {
					return rebalanceStarted, err
				}
				rebalanceStarted = true
			}
		}
		return rebalanceStarted, nil
	}

	if len(entry.Entry.Pattern.TargetIDs) > len(dstTargets) {
		return rebalanceStarted, fmt.Errorf("insufficient targets in the destination pool to rebalance entry (entry has %d targets / destination pool has %d targets)", len(entry.Entry.Pattern.TargetIDs), len(dstTargets))
	}
	for _, target := range entry.Entry.Pattern.TargetIDs {
		if _, ok := srcTargets[target]; ok {
			if dryRun {
				return true, nil
			}
			randomID, err := getRandomID(dstTargets, entry.Entry.Pattern.TargetIDs)
			if err != nil {
				return rebalanceStarted, err
			}
			if err := chunkRebalanceFunc(ctx, entry, target, randomID); err != nil {
				return rebalanceStarted, err
			}
			rebalanceStarted = true
		}
	}
	return rebalanceStarted, nil
}

func chunkRebalance(ctx context.Context, entry *GetEntryCombinedInfo, srcID uint16, destID uint16) error {
	log, _ := config.GetLogger()
	log.Debug("starting chunk rebalance", zap.String("path", entry.Path), zap.Any("srcID", srcID), zap.Any("destID", destID))

	store, err := config.NodeStore(ctx)
	if err != nil {
		return err
	}

	req := &msg.StartChunkBalanceMsg{
		TargetID:      srcID,
		DestinationID: destID,
		EntryInfo:     entry.Entry.origEntryInfoMsg,
		RelativePaths: &[]string{entry.Entry.Verbose.ChunkPath},
	}
	resp := &msg.StartChunkBalanceRespMsg{}

	if err = store.RequestTCP(ctx, entry.Entry.MetaOwnerNode.Uid, req, resp); err != nil {
		return err
	}

	if resp.Result != beegfs.OpsErr_SUCCESS {
		return resp.Result
	}

	return nil
}
