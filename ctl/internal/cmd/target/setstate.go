package target

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/v8/common/beegfs"
	backend "github.com/thinkparq/beegfs-go/v8/ctl/pkg/ctl/target"
	pm "github.com/thinkparq/protobuf/go/management"
)

type setState_Config struct {
	target  beegfs.EntityId
	state   beegfs.ConsistencyState
	execute bool
}

func newSetStateCmd() *cobra.Command {
	cfg := setState_Config{target: beegfs.InvalidEntityId{}}

	cmd := &cobra.Command{
		Use:    "set-state <target> <state>",
		Short:  "Forcefully set the state of a target",
		Hidden: true,
		Long: `Forcefully set the state of a target.

WARNING: This command is very dangerous and can cause serious data loss. It should not be used under normal circumstances. It is absolutely recommended to contact support before proceeding.

This is useful to manually set a target or node to the state "bad", or to resolve a situation in which both buddies in a buddy mirror group are in the state "needs-resync".
`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			tp := beegfs.NewEntityIdParser(16, beegfs.Storage, beegfs.Meta)
			t, err := tp.Parse(args[0])
			if err != nil {
				return err
			}
			cfg.target = t

			sp := beegfs.NewConsistencyStateParser(beegfs.Good, beegfs.Bad)
			s, err := sp.Parse(args[1])
			if err != nil {
				return err
			}
			cfg.state = s

			return runSetStateCmd(cmd, cfg)
		},
	}

	cmd.Flags().BoolVar(&cfg.execute, "yes", false, `This command is very dangerous and can cause serious data loss. It should not be used under normal circumstances. 
It is absolutely recommended to contact support before proceeding and thus requires explicit confirmation. 
Specify --yes for the command to actually take action.
`)

	return cmd
}

func runSetStateCmd(cmd *cobra.Command, cfg setState_Config) error {
	if !cfg.execute {
		fmt.Printf(`This command is very dangerous and can cause serious data loss. It should not be used under normal circumstances. 
It is absolutely recommended to contact support before proceeding and thus requires explicit confirmation. 
Specify --yes for the command to actually take action.
`)
		return nil
	}

	err := backend.SetState(cmd.Context(), &pm.SetTargetStateRequest{
		Target:           cfg.target.ToProto(),
		ConsistencyState: cfg.state.ToProto(),
	})

	return err
}
