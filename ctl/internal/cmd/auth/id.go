package auth

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/auth"
)

func newIdCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "id",
		Short: "Manage identities (requires an admin identity)",
		Args:  cobra.NoArgs,
	}

	cmd.AddCommand(
		newIdListCmd(),
		newIdPendingCmd(),
		newIdApproveCmd(),
		newIdRejectCmd(),
		newIdRevokeCmd(false),
		newIdRevokeCmd(true),
	)
	return cmd
}

func newIdListCmd() *cobra.Command {
	var pending bool

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List identities and key pairs waiting for approval",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runIdList(cmd, pending)
		},
	}

	cmd.Flags().BoolVar(&pending, "pending", false, "Only list key pairs waiting for approval.")
	return cmd
}

func newIdPendingCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "pending",
		Short: "List key pairs waiting for approval (same as list --pending)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runIdList(cmd, true)
		},
	}
}

func runIdList(cmd *cobra.Command, pendingOnly bool) error {
	var identities []auth.Identity
	if !pendingOnly {
		var err error
		identities, err = auth.ListIdentities(cmd.Context())
		if err != nil {
			return err
		}
	}

	keys, err := auth.GetPending(cmd.Context())
	if err != nil {
		return err
	}

	if len(identities) == 0 && len(keys) == 0 {
		if pendingOnly {
			cmdfmt.Printf("No pending key pairs\n")
		} else {
			cmdfmt.Printf("No identities\n")
		}
		return nil
	}

	printIdentities(identities, keys, auth.Conflicting(keys))
	return nil
}

func printIdentities(identities []auth.Identity, keys []auth.PendingKey, conflicting map[string]bool) {
	columns := []string{"identity", "key_pair_id", "state", "node", "name", "token", "address", "last_seen"}
	tbl := cmdfmt.NewPrintomatic(columns, columns)
	defer tbl.PrintRemaining()

	for _, i := range identities {
		state := "registered"
		if i.Revoked {
			state = "revoked"
		} else if i.Admin {
			state = "admin"
		}

		token := ""
		if i.HasToken {
			token = strconv.FormatInt(i.TokenID, 10)
		}

		keyIDs := i.KeyIDs
		if len(keyIDs) == 0 {
			keyIDs = []string{""}
		}
		for _, k := range keyIDs {
			tbl.AddItem(fmt.Sprintf("id:%d", i.ID), k, state, i.Node, i.Name, token, "", "")
		}
	}

	for _, k := range keys {
		state := "pending"
		if k.Rejected {
			state = "rejected"
		} else if conflicting[k.KeyPairID] {
			state = "conflict"
		}

		tbl.AddItem("", k.KeyPairID, state, k.ClaimedNode(), k.Name, "", k.Addr,
			k.LastSeen.Format(time.DateTime))
	}
}

func newIdApproveCmd() *cobra.Command {
	var all, servers, clients, yes bool

	cmd := &cobra.Command{
		Use:   "approve [<key-pair-id>] ...",
		Short: "Approve pending key pairs (requires an admin identity)",
		Long: `Approve pending key pairs (requires an admin identity).

With --all, the pending key pairs are listed and approved after confirmation. Only the listed key
pairs are approved. Rejected entries and entries claiming the same node as another entry are
skipped. Approve those by their key pair ID after checking it on the node.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !all {
				if servers || clients || yes {
					return errors.New("--servers, --clients and --yes require --all")
				}
				if len(args) == 0 {
					return errors.New("give at least one key pair ID or use --all")
				}
				if err := auth.Approve(cmd.Context(), args); err != nil {
					return err
				}
				cmdfmt.Printf("Approved %d key pair(s)\n", len(args))
				return nil
			}

			if len(args) > 0 {
				return errors.New("--all cannot be combined with key pair IDs")
			}

			keys, err := auth.GetPending(cmd.Context())
			if err != nil {
				return err
			}

			conflicting := auth.Conflicting(keys)
			selected := []auth.PendingKey{}
			skipped := 0
			for _, k := range keys {
				if servers != clients {
					if servers && k.Category != auth.ServerCategory {
						continue
					}
					if clients && k.Category != auth.ClientCategory {
						continue
					}
				}
				if k.Rejected || conflicting[k.KeyPairID] {
					skipped++
					continue
				}
				selected = append(selected, k)
			}

			if len(selected) == 0 {
				cmdfmt.Printf("No key pairs to approve (%d skipped)\n", skipped)
				return nil
			}

			printIdentities(nil, selected, conflicting)
			if skipped > 0 {
				cmdfmt.Printf("Skipping %d rejected or conflicting key pair(s)\n", skipped)
			}

			if !yes {
				fmt.Printf("Approve these %d key pair(s)? [y/N] ", len(selected))
				answer, _ := bufio.NewReader(os.Stdin).ReadString('\n')
				answer = strings.ToLower(strings.TrimSpace(answer))
				if answer != "y" && answer != "yes" {
					return errors.New("aborted")
				}
			}

			ids := make([]string, 0, len(selected))
			for _, k := range selected {
				ids = append(ids, k.KeyPairID)
			}
			if err := auth.Approve(cmd.Context(), ids); err != nil {
				return err
			}

			cmdfmt.Printf("Approved %d key pair(s)\n", len(ids))
			return nil
		},
	}

	cmd.Flags().BoolVar(&all, "all", false, "Approve all pending key pairs after listing them.")
	cmd.Flags().BoolVar(&servers, "servers", false, "With --all, only approve servers.")
	cmd.Flags().BoolVar(&clients, "clients", false, "With --all, only approve clients.")
	cmd.Flags().BoolVar(&yes, "yes", false, "With --all, approve without asking for confirmation.")
	return cmd
}

func newIdRejectCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "reject <key-pair-id> ...",
		Short: "Reject pending key pairs (requires an admin identity)",
		Long: `Reject pending key pairs (requires an admin identity).

A rejected key pair stays rejected. Its node stops trying to register.`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := auth.Reject(cmd.Context(), args); err != nil {
				return err
			}
			cmdfmt.Printf("Rejected %d key pair(s)\n", len(args))
			return nil
		},
	}
}

func newIdRevokeCmd(unrevoke bool) *cobra.Command {
	use, short, long, done := "revoke", "Revoke an identity",
		`Revoke an identity.

The identity is selected by one of its key pair IDs (sha256:...), its identity ID (id:<n>) or the
node it is mapped to (<type>:<id>, alias or uid:<uid>). Its key pairs can no longer connect or
register again until it is unrevoked. The node mapping is removed. Connections that are already
open stay until they close.`, "Revoked"
	if unrevoke {
		use, short, long, done = "unrevoke", "Unrevoke an identity",
			`Unrevoke an identity.

The identity is selected by one of its key pair IDs (sha256:...) or its identity ID (id:<n>). Its
key pairs can connect and register again. A node registers again on its next attempt.`, "Unrevoked"
	}

	return &cobra.Command{
		Use:   use + " <identity>",
		Short: short,
		Long:  long,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := auth.RevokeIdentity(cmd.Context(), args[0], unrevoke)
			if err != nil {
				return err
			}
			cmdfmt.Printf("%s identity id:%d\n", done, id)
			return nil
		},
	}
}
