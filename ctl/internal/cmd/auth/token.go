package auth

import (
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/auth"
)

func newTokenCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "token",
		Short: "Manage identity tokens (requires an admin identity)",
		Args:  cobra.NoArgs,
	}

	cmd.AddCommand(
		newTokenListCmd(),
		newTokenGenerateCmd(),
		newTokenRevokeCmd(),
	)
	return cmd
}

func newTokenListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List identity tokens",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			tokens, err := auth.ListTokens(cmd.Context())
			if err != nil {
				return err
			}

			columns := []string{"id", "scope", "reusable", "state", "identities", "created", "used", "comment"}
			tbl := cmdfmt.NewPrintomatic(columns, columns)
			defer tbl.PrintRemaining()

			for _, t := range tokens {
				state := "unused"
				if t.Revoked {
					state = "revoked"
				} else if t.Used != nil {
					if t.Reusable {
						state = "in use"
					} else {
						state = "spent"
					}
				}

				used := ""
				if t.Used != nil {
					used = t.Used.Format(time.DateTime)
				}

				tbl.AddItem(t.ID, t.Scope, t.Reusable, state, t.Identities,
					t.Created.Format(time.DateTime), used, t.Comment)
			}
			return nil
		},
	}
}

func newTokenGenerateCmd() *cobra.Command {
	var scope string
	var reusable bool
	var comment string

	cmd := &cobra.Command{
		Use:   "generate",
		Short: "Generate an identity token (requires an admin identity)",
		Long: `Generate an identity token (requires an admin identity).

The token is printed to stdout and is not shown again. It can be used once unless --reusable is
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

			token, err := auth.GenerateToken(cmd.Context(), s, reusable, comment)
			if err != nil {
				return err
			}

			fmt.Println(token)
			return nil
		},
	}

	cmd.Flags().StringVar(&scope, "scope", "client", "What the token registers: client or admin.")
	cmd.Flags().BoolVar(&reusable, "reusable", false, "Allow the token to register any number of identities.")
	cmd.Flags().StringVar(&comment, "comment", "", "A comment stored with the token.")
	return cmd
}

func newTokenRevokeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "revoke <token-id> ...",
		Short: "Revoke identity tokens",
		Long: `Revoke identity tokens.

Unused tokens are deleted. Used reusable tokens are kept for audit but can no longer register new
identities. Used one-time tokens are already spent and cannot be revoked.`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ids := make([]int64, 0, len(args))
			for _, a := range args {
				id, err := strconv.ParseInt(a, 10, 64)
				if err != nil || id <= 0 {
					return fmt.Errorf("invalid token ID %q", a)
				}
				ids = append(ids, id)
			}

			if err := auth.RevokeTokens(cmd.Context(), ids); err != nil {
				return err
			}
			cmdfmt.Printf("Revoked %d identity token(s)\n", len(ids))
			return nil
		},
	}
}
