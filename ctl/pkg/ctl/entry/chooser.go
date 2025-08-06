package entry

import (
	"context"
	"fmt"
	"math/rand/v2"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/common/beemsg/msg"
)

func getRandomIDChooser() func(fromDstIDs []uint16, idsAlreadyInStripePattern []uint16) (uint16, error) {
	var shuffledIDs []uint16
	var shuffledIdx int
	// getRandomID lazily initializes a shuffled slice of IDs from the destination IDs upon first
	// use. Each call returns a random destination ID that is not already in the provided pattern.
	return func(fromDstIDs []uint16, idsAlreadyInStripePattern []uint16) (uint16, error) {
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
		return 0, fmt.Errorf("no more candidate IDs are available")
	}
}

// getMigrationForEntry determines if the entry has any targets in srcTargets (if unmirrored) or
// groups in srcGroups (if mirrored). For each target/group it finds, a random target/group from
// dstTargets / dstGroups will be selected where chunks should be migrated. It returns a list of
// srcIDs where data should be migrated away from and dstIDs where data should be migrated to. It
// returns a rebalanceType indicating if the src/dstIDs are targets or buddy groups. It also returns
// unmodifiedIDs that were in the entry stripe pattern but are not in srcTargets/Groups.
//
// IMPORTANT: dstTargets and dstGroups should not contain any duplicates but must be provided as
// slices to optimize copying the slice for randomization.
func getMigrationForEntry(
	ctx context.Context,
	entry *GetEntryCombinedInfo,
	srcTargets map[uint16]struct{},
	srcGroups map[uint16]struct{},
	dstTargets []uint16,
	dstGroups []uint16,
) (rebalanceType msg.RebalanceIDType, srcIDs []uint16, dstIDs []uint16, unmodifiedIDs []uint16, err error) {

	targetChooser := getRandomIDChooser()

	// Initialize with a capacity of 4 since this is the default number of targets.
	srcIDs = make([]uint16, 0, 4)
	dstIDs = make([]uint16, 0, 4)
	unmodifiedIDs = make([]uint16, 0, 4)
	var rebalanceIDType msg.RebalanceIDType = msg.RebalanceIDTypeInvalid
	if entry.Entry.Pattern.Type == beegfs.StripePatternBuddyMirror {
		rebalanceIDType = msg.RebalanceIDTypeGroup
		for _, group := range entry.Entry.Pattern.TargetIDs {
			if _, ok := srcGroups[group]; ok {
				randomID, err := targetChooser(dstGroups, entry.Entry.Pattern.TargetIDs)
				if err != nil {
					return msg.RebalanceIDTypeInvalid, nil, nil, nil, fmt.Errorf("insufficient destination groups to migrate entry away from the specified groups (entry is currently assigned to groups %v)", entry.Entry.Pattern.TargetIDs)
				}
				srcIDs = append(srcIDs, group)
				dstIDs = append(dstIDs, randomID)
			} else {
				unmodifiedIDs = append(unmodifiedIDs, group)
			}
		}
	} else if entry.Entry.Pattern.Type == beegfs.StripePatternRaid0 {
		rebalanceIDType = msg.RebalanceIDTypeTarget
		for _, target := range entry.Entry.Pattern.TargetIDs {
			if _, ok := srcTargets[target]; ok {
				randomID, err := targetChooser(dstTargets, entry.Entry.Pattern.TargetIDs)
				if err != nil {
					return msg.RebalanceIDTypeInvalid, nil, nil, nil, fmt.Errorf("insufficient destination targets to migrate entry away from the specified targets (entry is currently assigned to targets %v)", entry.Entry.Pattern.TargetIDs)
				}
				srcIDs = append(srcIDs, target)
				dstIDs = append(dstIDs, randomID)
			} else {
				unmodifiedIDs = append(unmodifiedIDs, target)
			}
		}
	} else {
		return msg.RebalanceIDTypeInvalid, nil, nil, nil, fmt.Errorf("unsupported pattern type: %s", entry.Entry.Pattern.Type)
	}

	return rebalanceIDType, srcIDs, dstIDs, unmodifiedIDs, nil
}
