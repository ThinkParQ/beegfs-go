package rst

import "github.com/spf13/cobra"

func NewRSTCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "remote",
		Aliases: []string{"remote-storage-target", "rst"},
		Short:   "Interact with Remote Storage Targets.",
	}

	cmd.AddCommand(newStatusCmd(), newPushCmd(), newPullCmd(), newJobCmd(), newListCmd())

	return cmd
}

func newJobCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "job",
		Short: "Update the state of job(s) running for a file in BeeGFS.",
	}
	cmd.AddCommand(newCancelCmd(), newCleanupCmd())
	return cmd
}
