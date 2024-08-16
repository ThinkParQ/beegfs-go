package rst

import (
	"fmt"
	"sort"
	"strings"

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
	cmd.Flags().BoolVar(&cfg.ShowSecrets, "show-secrets", false, "If secret keys should be printed in cleartext or masked (the default).")
	return cmd
}

func runListCmd(cmd *cobra.Command, cfg rst.GetRSTCfg) error {

	response, err := rst.GetRSTConfig(cmd.Context())
	if err != nil {
		return err
	}

	defaultColumns := []string{"id", "name", "policies", "type", "configuration"}

	tbl := cmdfmt.NewTableWrapper(defaultColumns, defaultColumns)
	defer tbl.PrintRemaining()
	sort.Slice(response.Rsts, func(i, j int) bool {
		return response.Rsts[i].Id < response.Rsts[j].Id
	})

	for _, rst := range response.Rsts {

		var rstType string
		var rstConfiguration string

		switch rst.Type.(type) {
		case *flex.RemoteStorageTarget_S3_:
			stringBuilder := strings.Builder{}
			rstType = "s3"
			rst.GetS3().ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
				if string(fd.Name()) == "secret_key" && !cfg.ShowSecrets {
					stringBuilder.WriteString(fmt.Sprintf("%s: *****, ", fd.Name()))
				} else {
					stringBuilder.WriteString(fmt.Sprintf("%s: %s, ", fd.Name(), v))
				}
				return true
			})
			// Get rid of the last comma+space in the printed configuration.
			rstConfiguration = stringBuilder.String()[:stringBuilder.Len()-2]
		default:
			if !cfg.ShowSecrets {
				rstType = "unknown"
				rstConfiguration = ("unknown configuration masked by default")
			} else {
				rstType = "unknown"
				rstConfiguration = fmt.Sprintf("%v", rst.GetType())
			}
		}

		tbl.Row(
			rst.GetId(),
			rst.GetName(),
			rst.GetPolicies().String(),
			rstType,
			rstConfiguration,
		)
	}

	return nil
}
