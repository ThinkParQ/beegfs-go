package stats

import (
	"fmt"
	"net"
	"os"
	"os/user"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/dsnet/golib/unitconv"
	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/stats"
	"github.com/thinkparq/gobee/beegfs"
)

type clientStats_Config struct {
	node     beegfs.EntityId
	nodeType beegfs.NodeType
	interval time.Duration
	all      bool
	filter   string
	names    bool
	sum      bool
	limit    uint16
	perUser  bool
}

func newGenericClientStatsCmd(perUserDefault bool) *cobra.Command {
	cfg := clientStats_Config{node: beegfs.InvalidEntityId{}, nodeType: beegfs.Meta}

	var cmd = &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				id, err := beegfs.NewNodeParser().Parse(args[0])
				if err != nil {
					return err
				}
				cfg.node = id

			}

			return runClientStatsCmd(cmd, &cfg)
		},
	}

	cmd.Flags().DurationVar(&cfg.interval, "interval", 5*time.Second,
		"Interval for repeated stats retrieval in seconds.")
	cmd.Flags().BoolVar(&cfg.all, "all", false,
		"Print all values. By default, zero values are skipped.")
	cmd.Flags().Var(beegfs.NewNodeTypePFlag(&cfg.nodeType, beegfs.Meta, beegfs.Storage), "node-type",
		"The node type to query (meta, storage).")
	cmd.Flags().StringVar(&cfg.filter, "filter", "", "Show values for given clients/users.")
	cmd.Flags().BoolVar(&cfg.names, "names", false,
		"Show hostnames instead of IPs and usernames instead of numerical user IDs.")
	cmd.Flags().BoolVar(&cfg.sum, "sum", false,
		"Show aggregated values for all clients/users.")
	cmd.Flags().BoolVar(&cfg.perUser, "user", perUserDefault,
		"Show user stats.")
	cmd.Flags().Uint16Var(&cfg.limit, "limit", 0,
		"Limit number of clients/users in output.")

	return cmd
}

func newClientStatsCmd() *cobra.Command {
	s := newGenericClientStatsCmd(false)
	s.Use = "client"
	s.Short = "Show IO statistics for BeeGFS clients."
	s.Long = `Show IO statistics for BeeGFS clients.
  This command queries statistics for client requests from the servers and
  presents them in a sorted list, ordered by sum of requests per client.
  This allows identification of those clients, which are currently
  generating the most load on the servers.

  The initial batch of statistics for time index 0 shows the absolute number of
  operations since the servers were started, then the following batches only show
  values for the given interval.

Note: 
  Some client operation related messages (e.g. close file messages) are forwarded
  by metadata servers. Thus it is possible for servers to also appear in the client stats.

Example: Show per-client metadata access statistics, refresh every 5 seconds.
  $ beegfs stats client --node-type meta --interval 5s
`

	return s
}

func newUserStatsCmd() *cobra.Command {
	s := newGenericClientStatsCmd(true)
	s.Use = "user"
	s.Short = "Show IO statistics for BeeGFS users"
	s.Long = `Show IO statistics for BeeGFS users
  This command queries statistics for user requests from the servers and
  presents them in a sorted list, ordered by sum of requests per user.
  This allows identification of those users, which are currently
  generating the most load on the servers.

  The initial batch of statistics for time index 0 shows the absolute number of
  operations since the servers were started, then the following batches only show
  values for the given interval.
	 
Note: 
  Some client operation related messages (e.g. close file messages) are forwarded
  by metadata servers and not associated with a specific user. When this happens 
  the operations are associated with the user ID "-1".

Example: Show per-user storage access statistics, refresh every 5 seconds.
  $ beegfs stats user --node-type storage --interval 5s
`

	return s
}

func runClientStatsCmd(cmd *cobra.Command, cfg *clientStats_Config) error {
	old := []stats.ClientOps{}
	t := 0

	for {
		var current []stats.ClientOps
		var err error

		if _, ok := cfg.node.(beegfs.InvalidEntityId); ok {
			current, err = stats.PerNodeType(cmd.Context(), cfg.nodeType, cfg.perUser)
		} else {
			current, err = stats.SingleNodeClients(cmd.Context(), cfg.node, cfg.perUser)
		}

		if err != nil {
			return err
		}

		intervalStats := stats.Diff(current, old)
		old = current

		sum, err := stats.SumAllOpsFromSingleServer(intervalStats)
		if err != nil {
			return err
		}

		w := cmdfmt.NewTableWriter(os.Stdout)

		fmt.Fprintf(&w, "------- %ds -------\n", t)
		printOps(&w, intervalStats, cfg, sum)

		fmt.Fprintf(&w, "\n")

		if cfg.interval <= 0 {
			break
		}

		time.Sleep(cfg.interval)
		t += int(cfg.interval.Abs().Seconds())
	}

	return nil
}

func printOps(w *tabwriter.Writer, cs []stats.ClientOps, cfg *clientStats_Config, sum []uint64) {
	limit := len(cs)
	if cfg.limit > 0 {
		limit = min(int(cfg.limit), limit)
	}

	for _, c := range cs[:limit] {
		var name string
		if cfg.perUser {
			name = userIDToString(c.Id, cfg.names)
		} else {
			name = clientIPToString(c.Id, cfg.names)
		}

		if cfg.filter != "" && name != cfg.filter {
			continue
		}

		if c.Ops[0] < 1 {
			if !cfg.all {
				continue
			}
		}

		printOpsRow(w, name, c.Ops, cfg.nodeType, cfg.all)
	}

	if (sum != nil && cfg.sum && sum[0] > 1) || cfg.all {
		printOpsRow(w, "Summary", sum, cfg.nodeType, cfg.all)
	}

	w.Flush()
}

func userIDToString(id uint64, name bool) string {
	// servers return ~0 when userid can't be detected. Printing -1 instead
	if uint32(id) == ^uint32(0) {
		return "-1"
	}

	if name {
		userName, err := user.LookupId(strconv.Itoa(int(id)))
		if err == nil {
			return userName.Username
		}
	}

	return fmt.Sprintf("%d", id)
}

func clientIPToString(ip uint64, name bool) string {
	// servers return ~0 when userid can't be detected. Printing -1 instead
	if uint32(ip) == ^uint32(0) {
		return "-1"
	}

	oct1, oct2, oct3, oct4 := byte(ip), byte(ip>>8), byte(ip>>16), byte(ip>>24)
	stringIP := fmt.Sprintf("%d.%d.%d.%d", oct1, oct2, oct3, oct4)

	if name {
		hostname, err := net.LookupAddr(stringIP)
		if err == nil {
			return hostname[0]
		}
	}
	return stringIP
}

// Prints one client Ip/username, number of operations and operation name
func printOpsRow(w *tabwriter.Writer, name string, ops []uint64, nt beegfs.NodeType, raw bool) {
	var opNames []string
	if nt == beegfs.Meta {
		opNames = stats.MetaOpNames
	} else {
		opNames = stats.StorageOpNames
	}

	fmt.Fprintf(w, "%s:\t", name)

	for i, v := range ops {
		if v != 0 || raw {
			if opNames[i] == "rd" || opNames[i] == "wr" {
				fmt.Fprintf(w, "%s [%s]\t", unitconv.FormatPrefix(float64(v), unitconv.IEC, 0), opNames[i])
			} else {
				fmt.Fprintf(w, "%d [%s]\t", v, opNames[i])
			}
		}
	}

	fmt.Fprintf(w, "\n")
}
