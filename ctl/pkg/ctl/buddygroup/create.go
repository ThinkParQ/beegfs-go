package buddygroup

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
	pm "github.com/thinkparq/protobuf/go/management"
)

func Create(ctx context.Context, req *pm.CreateBuddyGroupRequest) (*pm.CreateBuddyGroupResponse, error) {
	client, err := config.ManagementClient()
	if err != nil {
		return nil, err
	}

	resp, err := client.CreateBuddyGroup(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

type AutoCreateConfig struct {
	NodeType     beegfs.NodeType
	IgnoreUneven bool
	IgnoreSpace  bool
}

// AutoCreate makes a best effort attempt at automatically creating buddy groups from all available
// nodes (cfg.nodeType == Meta) or targets (cfg.NodeType == Storage). It takes a couple of
// constraints into consideration:
//  1. Nodes/targets must not be part of an existing buddy group.
//  2. All nodes must have the same number of inodes available (Meta) and all targets must have
//     the same amount of space available (Storage mode). This constraint can be removed by passing
//     cfg.IgnoreSpace == true.
//  3. There must be an even number of nodes or targets, so all of them end up mirrored. This
//     constraint can be removed by passing cfg.IgnoreSpace == true.
//  4. After applying above filtering, there must be at least two nodes or targets available.
//     Otherwise, an error is returned.
//
// Constraints 1.-4. are checked on the full list of nodes/targets and if not met, will lead to
// an error being returned without an attempt to create any buddy groups. There are some more
// additional constraints that need to be checked on a per-node/target basis listed below:
//  5. In Storage mode, primary and secondary of a buddy group must be in the same storage pool.
//  6. Primary and secondary of a buddy group should not be on the same node. This is a soft
//     constraint that is relaxed automatically if necessary. Will return a warning if relaxed.
//
// There is an auxilliary function embedded to find suitable targets from the pre-filtered list
// (after checks 1.-4.). This function checks constraints 5. and 6. to find a suitable buddy for a
// selected primary. Because this can fail for certain nodes/targets, while succeeding for others,
// errors are treated as warnings that are returned to the caller. All "possible" buddy groups will
// be created and returned to the caller.
func AutoCreate(ctx context.Context, cfg AutoCreateConfig) ([]*pm.CreateBuddyGroupResponse, []error, error) {
	// Logging (especially debug logging) is useful here, because of the complex decision matrix
	log, err := config.GetLogger()
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get global logger")
	}

	// We need a node store to be able to get the owner of the root inode
	nodeStore, err := config.NodeStore(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get node store")
	}

	// We also need a list of existing buddy groups to filter out nodes/targets that are already part
	// of one
	buddygroups, err := GetBuddyGroups(ctx)
	if err != nil {
		return nil, nil, err
	}
	n := 0
	for _, b := range buddygroups {
		if b.NodeType == cfg.NodeType {
			buddygroups[n] = b
			n++
		} else {
			log.Debug(fmt.Sprintf("Dropping buddy group %s, because of its type", b.BuddyGroup.Alias))
		}
	}
	buddygroups = buddygroups[:n]

	// We mostly operate on targets, which also works for meta because they have exactly one target
	// per node. Technically, we always buddy mirror targets.
	targets, err := target.GetTargets(ctx)
	if err != nil {
		return nil, nil, err
	}

	// First, we do all the initial filtering of the target list (steps 1. and 2.)
	n = 0
	total := uint64(0)
targets:
	for _, t := range targets {
		if t.NodeType != cfg.NodeType {
			log.Debug(fmt.Sprintf("Dropping target %s, because of its type", t.Target.Alias))
			continue targets
		}
		for _, b := range buddygroups {
			if t.Target.Uid == b.PrimaryTarget.Uid || t.Target.Uid == b.SecondaryTarget.Uid {
				log.Debug(fmt.Sprintf("Dropping target %s, because of it's already part of a buddy group", t.Target.Alias))
				continue targets
			}
		}
		switch cfg.NodeType {
		case beegfs.Meta:
			if t.TotalInodes == nil {
				return nil, nil, fmt.Errorf("unable to determine available inodes. Please wait until meta nodes are fully registered")
			}
			if !cfg.IgnoreSpace && *t.TotalInodes != total {
				if total == 0 {
					total = *t.TotalInodes
					log.Debug(fmt.Sprintf("First node's available inodes: %d. All other nodes must match.", total))
				} else {
					log.Debug(fmt.Sprintf("Dropping target %s, because of its available inodes don't match", t.Target.Alias))
					continue targets
				}
			}

		case beegfs.Storage:
			if t.TotalSpaceBytes == nil {
				return nil, nil, fmt.Errorf("unable to determine target size. Please wait until storage targets are fully registered")
			}
			if !cfg.IgnoreSpace && *t.TotalSpaceBytes != total {
				if total == 0 {
					total = *t.TotalSpaceBytes
					log.Debug(fmt.Sprintf("First targets's available space: %d. All other targets must match.", total))
				} else {
					log.Debug(fmt.Sprintf("Dropping target %s, because of its size doesn't match", t.Target.Alias))
					continue targets
				}
			}
		}
		targets[n] = t
		n++
	}
	targets = targets[:n]

	// The resulting list of targets must contain at least two targets
	if len(targets) <= 1 {
		return nil, nil, fmt.Errorf("not enough pairs of eligible targets found to create buddy mirrors (found %d)", len(targets))
	}

	// The number of targets also needs to be even if not configured otherwise
	if len(targets)%2 != 0 {
		if cfg.IgnoreUneven {
			log.Debug("Found uneven number of targets/nodes. Proceeding anyway (--ignore-uneven)")
		} else {
			return nil, nil, fmt.Errorf("found uneven number of targets/nodes. Aborting. Use --ignore-uneven to override")
		}
	}

	// This auxilliary function takes care of finding suitable targets based on constraints 5. and
	// 6. Passing (nil, nil) for the arguments will return the first target in the list that hasn't
	// already been assigned to a buddy group.
	taken := []int{}
	findSuitableTarget := func(notNode *beegfs.EntityIdSet, storagePool *beegfs.EntityIdSet) (*target.GetTargets_Result, error) {
		for i := 0; i < len(targets); i++ {
			if slices.Contains(taken, i) {
				continue
			}
			t := targets[i]
			if t.NodeType == beegfs.Storage && storagePool != nil && t.StoragePool.Uid != storagePool.Uid {
				log.Debug(fmt.Sprintf("Ignoring target %v because it is not in Pool %v", t.Target, storagePool))
				continue
			}
			if notNode != nil && t.Node.Uid == notNode.Uid {
				log.Debug(fmt.Sprintf("Ignoring target %v because it is on node %v", t.Target, notNode))
				continue
			}
			taken = append(taken, i)
			return &t, nil
		}
		msg := "no target found"
		if storagePool != nil {
			msg += fmt.Sprintf(" that is in storage pool %v", storagePool)
		}
		if notNode != nil {
			msg += fmt.Sprintf(" and not on node %v", notNode)
		}
		return nil, errors.New(msg)
	}

	// We can now make use of findSuitableTarget to find primaries and suitable secondaries
	warnings := []error{}
	results := []*pm.CreateBuddyGroupResponse{}
	for i := 0; i < len(targets); i++ {
		pTarget, err := findSuitableTarget(nil, nil)
		if err != nil {
			// Nothing to do any more if we don't find a primary
			break
		} else {
			log.Debug(fmt.Sprintf("Chose primary target %v. Now looking for suitable secondary", pTarget.Target))
		}
		var storagePool *beegfs.EntityIdSet
		if pTarget.NodeType == beegfs.Storage {
			storagePool = pTarget.StoragePool
		} else {
			storagePool = nil
		}
		sTarget, err := findSuitableTarget(&pTarget.Node, storagePool)
		if err != nil {
			sTarget, err = findSuitableTarget(nil, storagePool)
			if err != nil {
				warnings = append(warnings, fmt.Errorf("unable to find suitable secondary for %v: %v", pTarget.Target, err))
				continue
			} else {
				warnings = append(warnings, fmt.Errorf("unable to find buddy target on different node. Using target on the same node"))
			}
		}
		// One extra check that is necessary for meta buddy groups. The node that has the root
		// inode can not be a secondary of a mirror group initially. If our secondary has the root
		// inode, we just swap primary and secondary.
		if cfg.NodeType == beegfs.Meta {
			if sTarget.Node.Uid == nodeStore.GetMetaRootNode().Uid {
				temp := pTarget
				pTarget = sTarget
				sTarget = temp
			}
		}

		// All checks are done and we have found a buddy pair. We now instruct the mgmtd to create
		// a newbuddy group.
		alias, err := beegfs.AliasFromString(fmt.Sprintf("buddy_group_%s_%d_%d", cfg.NodeType.String(), pTarget.Target.LegacyId.NumId, sTarget.Target.LegacyId.NumId))
		if err != nil {
			log.Error(fmt.Sprintf("unable to derive alias: %v", err))
			continue
		}
		id := uint32(0)
		al := string(alias)
		res, err := Create(ctx, &pm.CreateBuddyGroupRequest{
			NodeType:        cfg.NodeType.ToProto(),
			NumId:           &id,
			Alias:           &al,
			PrimaryTarget:   pTarget.Target.ToProto(),
			SecondaryTarget: sTarget.Target.ToProto(),
		})
		if err != nil {
			warnings = append(warnings, fmt.Errorf("error during buddy group creation: %v", err))
		}
		results = append(results, res)
	}
	return results, warnings, nil
}
