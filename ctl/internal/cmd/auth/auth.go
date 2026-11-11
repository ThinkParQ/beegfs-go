package auth

import (
	"fmt"
	"io"
	"os"
	"strings"

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
		newGenerateKeyCmd(),
		newRegisterCmd(),
		newIdCmd(),
		newTokenCmd(),
	)
	return cmd
}

func newRegisterCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "register [<name>]",
		Short: "Register the key pair of this client with the management",
		Long: `Register the key pair of this client with the management.

An identity token is read from stdin. On a terminal, it is prompted for without echo. Leave it
empty if the management accepts new clients without a token.

If no key file exists yet, one is generated first, like "beegfs auth generate-key" does.`,
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
			if res.GeneratedKey {
				cmdfmt.Printf("Generated key file %s\n", res.KeyFile)
			}
			if err != nil {
				return err
			}

			if res.Status == auth.RegisterPending {
				cmdfmt.Printf("Key pair %s is pending approval by an admin (beegfs auth id approve %s)\n",
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

func newGenerateKeyCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "generate-key [<path>]",
		Short: "Generate a new key pair file.",
		Long: `Generate a new key pair file.

Runs locally without contacting the management. The file is written to the given path, to
--conn-key-file or to ~/.config/beegfs/key.pem, in this order. An existing file is never
overwritten.`,
		Args: cobra.MaximumNArgs(1),
		Annotations: map[string]string{
			"authorization.AllowAllUsers": "",
			"health.SkipAlerts":           "",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			path := ""
			if len(args) > 0 {
				path = args[0]
			}

			path, id, err := auth.GenerateKey(path)
			if err != nil {
				return err
			}

			cmdfmt.Printf("Generated key pair %s at %s\n", id, path)
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
