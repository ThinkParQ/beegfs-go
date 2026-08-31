package rst

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/flex"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func newListCmd() *cobra.Command {
	cfg := rst.GetRSTCfg{}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List Remote Storage Targets and their configuration",
		Args:  cobra.NoArgs,
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

	tbl := cmdfmt.NewPrintomatic(defaultColumns, defaultColumns)
	defer tbl.PrintRemaining()
	sort.Slice(response.Rsts, func(i, j int) bool {
		return response.Rsts[i].Id < response.Rsts[j].Id
	})

	for _, rst := range response.Rsts {
		if rst.GetId() == cRst.JobBuilderRstId {
			continue
		}

		var rstType string
		var rstConfiguration string

		switch rst.WhichType() {
		case flex.RemoteStorageTarget_S3_case:
			rstType = "s3"
			rstConfiguration = formatS3RSTConfiguration(rst.GetS3(), cfg.ShowSecrets)
		case flex.RemoteStorageTarget_Xtreemstore_case:
			rstType = "xtreemstore"
			xtreemstoreConfig := rst.GetXtreemstore()
			if xtreemstoreConfig == nil {
				rstConfiguration = "xtreemstore configuration is not set"
				break
			}
			rstConfiguration = formatS3RSTConfiguration(xtreemstoreConfig.GetS3(), cfg.ShowSecrets)
		default:
			if !cfg.ShowSecrets {
				rstType = "unknown"
				rstConfiguration = ("unknown configuration masked by default")
			} else {
				rstType = "unknown"
				rstConfiguration = fmt.Sprintf("%v", rst.GetType())
			}
		}

		tbl.AddItem(
			rst.GetId(),
			rst.GetName(),
			rst.GetPolicies().String(),
			rstType,
			rstConfiguration,
		)
	}

	return nil
}

func formatS3RSTConfiguration(s3Config *flex.RemoteStorageTarget_S3, showSecrets bool) string {
	if s3Config == nil {
		return "s3 configuration is not set"
	}

	stringBuilder := strings.Builder{}
	s3Config.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if string(fd.Name()) == "secret_key" && !showSecrets {
			fmt.Fprintf(&stringBuilder, "%s: *****, ", fd.Name())
		} else {
			fmt.Fprintf(&stringBuilder, "%s: %s, ", fd.Name(), v)
		}
		return true
	})

	if stringBuilder.Len() == 0 {
		return ""
	}
	// Get rid of the last comma+space in the printed configuration.
	return stringBuilder.String()[:stringBuilder.Len()-2]
}
