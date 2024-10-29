package stats

import (
	"context"
	"fmt"
	"sort"

	"github.com/thinkparq/beegfs-go/v8/common/beegfs"
	"github.com/thinkparq/beegfs-go/v8/common/beemsg/msg"
	"github.com/thinkparq/beegfs-go/v8/ctl/pkg/config"
)

const (
	// The first minRespStatsSize elements of the stats slice are reserved. To avoid a panic in case
	// we receive an unexpected response we validate the stats slice is at least this large then
	// allow access to the first minRespStatsSize elements by their indices.
	minRespStatsSize = 8
)

var (
	// list of operation names corresponding to their index in Client Operation list. When used as
	// column names in a table they must be all lower case, hence the duplicate definitions.
	MetaOpNames         = []string{"sum", "ack", "close", "entInf", "fndOwn", "mkdir", "create", "rddir", "refrEnt", "mdsInf", "rmdir", "rmLnk", "mvDirIns", "mvFiIns", "open", "ren", "sChDrct", "sAttr", "sDirPat", "stat", "statfs", "trunc", "symlnk", "unlnk", "lookLI", "statLI", "revalLI", "openLI", "createLI", "hardlnk", "flckAp", "flckEn", "flckRg", "dirparent", "listXA", "getXA", "rmXA", "setXA", "mirror"}
	MetaOpNamesLower    = []string{"sum", "ack", "close", "entinf", "fndown", "mkdir", "create", "rddir", "refrent", "mdsinf", "rmdir", "rmlnk", "mvdirins", "mvfiins", "open", "ren", "schdrct", "sattr", "sdirpat", "stat", "statfs", "trunc", "symlnk", "unlnk", "lookli", "statli", "revalli", "openli", "createli", "hardlnk", "flckap", "flcken", "flckrg", "dirparent", "listxa", "getxa", "rmxa", "setxa", "mirror"}
	StorageOpNames      = []string{"sum", "ack", "sChDrct", "getFSize", "sAttr", "statfs", "trunc", "close", "fsync", "ops-rd", "rd", "ops-wr", "wr", "gendbg", "hrtbeat", "remNode", "storInf", "unlnk"}
	StorageOpNamesLower = []string{"sum", "ack", "schdrct", "getfsize", "sattr", "statfs", "trunc", "close", "fsync", "ops-rd", "rd", "ops-wr", "wr", "gendbg", "hrtbeat", "remnode", "storinf", "unlnk"}
)

// Contains details of client/user and a list of number of operations done by them
type ClientOps struct {
	Id  uint64   // Id can be Client IP in client stats and user ID in user stats
	Ops []uint64 // number of operations performed by client/user. The index corresponds to the name of operations from MetaOpNames and StorageOpNames.
}

// Substracting corresponding index values of b from a.
func (a *ClientOps) Sub(b ClientOps) {
	for i, ops := range b.Ops {
		a.Ops[i] -= ops
	}
}

// Adding corresponding index values of b in a
func (a *ClientOps) Add(b ClientOps) {
	for i, ops := range b.Ops {
		a.Ops[i] += ops
	}
}

// Calculates interval stats by subtrating abolute latest stats and old stats
func Diff(latest []ClientOps, old []ClientOps) []ClientOps {
	result := make([]ClientOps, len(latest))

	for i, stats := range latest {
		s := stats.Ops
		result[i].Ops = append(make([]uint64, 0, len(s)), s...)
		result[i].Id = stats.Id
	}

	for i := range result {
		for _, oldStats := range old {
			if result[i].Id == oldStats.Id {
				result[i].Sub(oldStats)
				break
			}
		}
	}

	return result
}

// Aggregates operations from multiple servers based on client IP/user ID.
func sumEachOpFromAllServers(list [][]ClientOps) []ClientOps {
	aggregated := make(map[uint64]ClientOps)

	for _, stats := range list {
		for _, client := range stats {
			if existingStats, ok := aggregated[client.Id]; ok {
				existingStats.Add(client)
				aggregated[client.Id] = existingStats

			} else {
				aggregated[client.Id] = client
			}
		}
	}

	result := []ClientOps{}
	for _, stats := range aggregated {
		result = append(result, stats)
	}

	return result
}

// Adds number of operations done by all the clients/user
func SumAllOpsFromSingleServer(list []ClientOps) ([]uint64, error) {
	if len(list) < 1 {
		return nil, fmt.Errorf("list is empty")
	}

	summary := make([]uint64, len(list[0].Ops))

	for _, stats := range list {
		for i, ops := range stats.Ops {
			summary[i] += ops
		}
	}

	return summary, nil
}

// Queries the client/user stats from one server
func SingleNodeClients(ctx context.Context, id beegfs.EntityId, userStats bool) ([]ClientOps, error) {
	store, err := config.NodeStore(ctx)
	if err != nil {
		return []ClientOps{}, err
	}

	n, err := store.GetNode(id)
	if err != nil {
		return []ClientOps{}, err
	}

	totalStats := <-getClientStats(ctx, n, userStats)
	if totalStats.err != nil {
		return []ClientOps{}, totalStats.err
	}

	return totalStats.ClientOps, nil
}

// Queries the client/user stats for multiple servers and returns an aggregate of all stats
func PerNodeType(ctx context.Context, nt beegfs.NodeType, userStats bool) ([]ClientOps, error) {
	nodes, err := getNodeList(ctx, nt)
	if err != nil {
		return []ClientOps{}, err
	}

	channels := make([]<-chan getClientStatsResult, 0)
	for _, n := range nodes {
		channels = append(channels, getClientStats(ctx, n, userStats))
	}

	statsList := make([][]ClientOps, len(nodes))
	for _, ch := range channels {
		resp := <-ch
		if resp.err != nil {
			return []ClientOps{}, resp.err
		}

		statsList = append(statsList, resp.ClientOps)
	}

	result := sumEachOpFromAllServers(statsList)

	return result, nil

}

// Contains list of all client/user operations and their summation
type getClientStatsResult struct {
	ClientOps []ClientOps
	err       error
}

// Queries the absolute number of operations performed by a client/user since the server was started.
// The returned client ips/user ids are sorted in ascending order.
// The result also contains summary, client hostname/username.
func getClientStats(ctx context.Context, n beegfs.Node, userStats bool) <-chan getClientStatsResult {
	var ch = make(chan getClientStatsResult, 1)

	go func() {
		store, err := config.NodeStore(ctx)
		if err != nil {
			ch <- getClientStatsResult{err: err}
		}

		//  Set MSB to let server know it is the first request
		cookieIP := ^uint64(0)

		// Server sets moreData bit when complete stats are not sent in one request
		moreData := 1

		clientOpsList := make([]ClientOps, 0)

		for moreData == 1 {
			request := msg.GetClientStats{CookieIP: cookieIP}
			if userStats {
				request.PerUser = true
			}

			var resp = msg.GetClientStatsResp{}
			err = store.RequestTCP(ctx, n.Id, &request, &resp)
			if err != nil {
				ch <- getClientStatsResult{err: err}
				return
			} else if len(resp.Stats) < minRespStatsSize {
				ch <- getClientStatsResult{err: fmt.Errorf("response size was smaller than expected (probably this is a bug elsewhere)")}
				return
			}

			// The 0th index of response contains the number of operation for each client id/user id
			numOps := resp.Stats[0]

			// Server sets this bit when it is unable to send all the id operations in one response
			moreData = int(resp.Stats[1])

			// check version of client stats beemsg api
			if resp.Stats[2] != 1 {
				ch <- getClientStatsResult{err: fmt.Errorf("protocol version mismatch in received Message from %s", n.Alias)}
			}

			// First 8 index of stats array are reserved.
			// After first 8 position Every next slice of length [1 + numOps] contains 0th index as ip/userid and other are operation stats.
			// Validate if the returned response are in the above format
			subArrLen := int(numOps) + 1
			if len(resp.Stats[minRespStatsSize:])%subArrLen != 0 {
				ch <- getClientStatsResult{err: fmt.Errorf("reported length of ClientStats from %s OpsList doesn't match the actual length", n.Alias)}
			}

			opsSlice := resp.Stats[minRespStatsSize:]

			// Iterate over subarray of length [1 + numOps]
			for f, l := 0, subArrLen; l < len(resp.Stats); f, l = f+subArrLen, l+subArrLen {
				ops := ClientOps{
					Id:  opsSlice[f],
					Ops: opsSlice[f+1 : l],
				}

				clientOpsList = append(clientOpsList, ops)

				// Set cookieIP to the last ip/userid to get stats for the remaining ips.
				// This will only be used when server has set the moredata bit
				cookieIP = opsSlice[f]
			}
		}

		// Sort client ip/user id in ascending order
		sort.Slice(clientOpsList, func(i, j int) bool {
			return clientOpsList[i].Id < clientOpsList[j].Id
		})

		result := getClientStatsResult{
			ClientOps: clientOpsList,
		}

		ch <- result
	}()

	return ch
}
