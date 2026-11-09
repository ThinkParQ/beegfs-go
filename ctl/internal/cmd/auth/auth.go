package auth

import (
	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/auth"
)

func NewCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Manage identities for the plain and encrypted connection protocols",
		Args:  cobra.NoArgs,
	}

	cmd.AddCommand(newRegisterCmd())
	return cmd
}

func newRegisterCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "register <name>",
		Short: "Register the key pair of this client with the management under the given identity name",
		Args:  cobra.ExactArgs(1),
		Annotations: map[string]string{
			"authorization.AllowAllUsers": "",
			"health.SkipAlerts":           "",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			res, err := auth.Register(cmd.Context(), args[0])
			if err != nil {
				return err
			}

			cmdfmt.Printf("Registered key pair %s as identity %d\n", res.KeyPairID, res.IdentityID)
			return nil
		},
	}
}
