package target

import (
	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	backend "github.com/thinkparq/beegfs-go/ctl/pkg/ctl/target"
	pm "github.com/thinkparq/protobuf/go/management"
)

type resetRegistrationToken_Config struct {
	target beegfs.EntityId
}

func newResetRegistrationTokenCmd() *cobra.Command {
	cfg := resetRegistrationToken_Config{}

	cmd := &cobra.Command{
		Use:   "reset-registration-token <target>",
		Short: "Resets a target's registration token",
		Long: `Resets a target's registration token in management to allow reuse of its id.

This command must be used when an already registered target id shall be reused by a new
target/node. The main use case is when a mirrored targets/nodes disk is replaced for
resync. Without running this command, management will reject the new storage directory.
`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			spp := beegfs.NewEntityIdParser(16, beegfs.Storage, beegfs.Meta)
			p, err := spp.Parse(args[0])
			if err != nil {
				return err
			}
			cfg.target = p

			return runResetRegistrationTokenCmd(cmd, cfg)
		},
	}

	return cmd
}

func runResetRegistrationTokenCmd(cmd *cobra.Command, cfg resetRegistrationToken_Config) error {
	resp, err := backend.ResetRegistrationToken(cmd.Context(), &pm.ResetTargetRegistrationTokenRequest{
		Target: cfg.target.ToProto(),
	})
	if err != nil {
		return err
	}

	res, err := beegfs.EntityIdSetFromProto(resp.Target)
	if err != nil {
		cmdfmt.Printf("Reset registration token of target but received no id info from the management node\n")
	} else {
		cmdfmt.Printf("Reset registration token of target: %s\n", res)
	}

	return nil
}
