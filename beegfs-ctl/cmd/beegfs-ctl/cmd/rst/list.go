package rst

import (
	"fmt"
	"os"
	"sort"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func newListCmd() *cobra.Command {
	cfg := rst.GetRSTCfg{}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List Remote Storage Targets and their configuration.",
		RunE: func(cmd *cobra.Command, args []string) error {
			return runListCmd(cmd, cfg)
		},
	}
	cmd.Flags().BoolVar(&cfg.ShowSecrets, "showSecrets", false, "If secret keys should be printed in cleartext or masked (default).")
	return cmd
}

func runListCmd(cmd *cobra.Command, cfg rst.GetRSTCfg) error {

	response, err := rst.GetRSTConfig(cmd.Context())
	if err != nil {
		return err
	}

	w := cmdfmt.NewTableWriter(os.Stdout)
	defer w.Flush()

	sort.Slice(response.Rsts, func(i, j int) bool {
		return response.Rsts[i].Id < response.Rsts[j].Id
	})

	fmt.Fprintf(&w, "ID\tName\tPolicies\tType\tConfiguration\n")
	for _, rst := range response.Rsts {
		fmt.Fprintf(&w, "%s\t", rst.Id)
		fmt.Fprintf(&w, "%s\t", rst.Name)
		fmt.Fprintf(&w, "%s\t", rst.Policies.String())
		switch rst.Type.(type) {
		case *flex.RemoteStorageTarget_S3_:
			fmt.Fprintf(&w, "S3\t")
			rst.GetS3().ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
				if string(fd.Name()) == "secret_key" && !cfg.ShowSecrets {
					fmt.Fprintf(&w, "%s: *****, ", fd.Name())
				} else {
					fmt.Fprintf(&w, "%s: %s, ", fd.Name(), v)
				}
				return true
			})
			fmt.Fprintf(&w, "\n")
		default:
			if !cfg.ShowSecrets {
				fmt.Fprintf(&w, "Unknown\tUnknown RST configuration is masked by default.\n")
			} else {
				fmt.Fprintf(&w, "Unknown\t%s\n", rst.GetType())
			}
		}
	}

	return nil
}
