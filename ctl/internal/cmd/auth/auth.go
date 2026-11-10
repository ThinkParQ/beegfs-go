package auth

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/auth"
	"golang.org/x/term"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Manage identities for the plain and encrypted connection protocols",
		Args:  cobra.NoArgs,
	}

	cmd.AddCommand(
		newRegisterCmd(),
		newGenerateTokenCmd(),
		newPendingCmd(),
		newApproveCmd(),
		newRejectCmd(),
	)
	return cmd
}

func newRegisterCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "register [<name>]",
		Short: "Register the key pair of this client with the management",
		Long: `Register the key pair of this client with the management.

An identity token is read from stdin. On a terminal, it is prompted for without echo. Leave it
empty if the management accepts new clients without a token.`,
		Args: cobra.MaximumNArgs(1),
		Annotations: map[string]string{
			"authorization.AllowAllUsers": "",
			"health.SkipAlerts":           "",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			name := ""
			if len(args) > 0 {
				name = args[0]
			}

			token, err := readToken()
			if err != nil {
				return err
			}

			res, err := auth.Register(cmd.Context(), name, token)
			if err != nil {
				return err
			}

			if res.Status == auth.RegisterPending {
				cmdfmt.Printf("Key pair %s is pending approval by an admin (beegfs auth approve %s)\n",
					res.KeyPairID, res.KeyPairID)
				return nil
			}

			if res.AlreadyRegistered {
				msg := fmt.Sprintf("Key pair %s is already registered as identity %d.", res.KeyPairID, res.IdentityID)
				if token != "" {
					msg += " The identity token was not used."
				}
				cmdfmt.Printf("%s\n", msg)
				return nil
			}

			cmdfmt.Printf("Registered key pair %s as identity %d\n", res.KeyPairID, res.IdentityID)
			return nil
		},
	}
}

func readToken() (string, error) {
	if term.IsTerminal(int(os.Stdin.Fd())) {
		fmt.Fprint(os.Stderr, "Identity token (empty for none): ")
		token, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", fmt.Errorf("reading identity token: %w", err)
		}
		return strings.TrimSpace(string(token)), nil
	}

	token, err := io.ReadAll(io.LimitReader(os.Stdin, 4096))
	if err != nil {
		return "", fmt.Errorf("reading identity token from stdin: %w", err)
	}
	return strings.TrimSpace(string(token)), nil
}

func newGenerateTokenCmd() *cobra.Command {
	var scope string
	var permanent bool
	var comment string

	cmd := &cobra.Command{
		Use:   "generate-token",
		Short: "Generate an identity token (requires an admin identity)",
		Long: `Generate an identity token (requires an admin identity).

The token is printed to stdout and is not shown again. It can be used once unless --permanent is
given. A client token registers client nodes, ctl and fsck. An admin token registers an admin
identity.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			var s auth.TokenScope
			switch scope {
			case "client":
				s = auth.ClientScope
			case "admin":
				s = auth.AdminScope
			default:
				return fmt.Errorf("invalid --scope %q, must be client or admin", scope)
			}

			token, err := auth.GenerateToken(cmd.Context(), s, permanent, comment)
			if err != nil {
				return err
			}

			fmt.Println(token)
			return nil
		},
	}

	cmd.Flags().StringVar(&scope, "scope", "client", "What the token registers: client or admin.")
	cmd.Flags().BoolVar(&permanent, "permanent", false, "Allow the token to register any number of identities.")
	cmd.Flags().StringVar(&comment, "comment", "", "A comment stored with the token.")
	return cmd
}

func newPendingCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "pending",
		Short: "List key pairs waiting for approval (requires an admin identity)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			keys, err := auth.GetPending(cmd.Context())
			if err != nil {
				return err
			}
			if len(keys) == 0 {
				cmdfmt.Printf("No pending key pairs\n")
				return nil
			}

			printPending(keys, auth.Conflicting(keys))
			return nil
		},
	}
}

func printPending(keys []auth.PendingKey, conflicting map[string]bool) {
	columns := []string{"key_pair_id", "category", "claimed_node", "name", "address", "state", "first_seen", "last_seen"}
	tbl := cmdfmt.NewPrintomatic(columns, columns)
	defer tbl.PrintRemaining()

	for _, k := range keys {
		state := "pending"
		if k.Rejected {
			state = "rejected"
		} else if conflicting[k.KeyPairID] {
			state = "conflict"
		}

		tbl.AddItem(
			k.KeyPairID,
			k.Category,
			k.ClaimedNode(),
			k.Name,
			k.Addr,
			state,
			k.FirstSeen.Format(time.DateTime),
			k.LastSeen.Format(time.DateTime),
		)
	}
}

func newApproveCmd() *cobra.Command {
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

			printPending(selected, conflicting)
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

func newRejectCmd() *cobra.Command {
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
