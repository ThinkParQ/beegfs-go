package rst

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	cRst "github.com/thinkparq/beegfs-go/common/rst"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/rst"
	"github.com/thinkparq/protobuf/go/flex"
)

// fakeAzureType is a target type declared only in this test. The CLI must offer it everywhere with
// no code of its own: a create subcommand, modify flags, list columns and changes.
type fakeAzureType struct{}

func (fakeAzureType) Name() string { return "azure" }
func (fakeAzureType) NewConfig() *flex.RemoteStorageTarget {
	return &flex.RemoteStorageTarget{Type: &flex.RemoteStorageTarget_Azure_{Azure: &flex.RemoteStorageTarget_Azure{}}}
}
func (fakeAzureType) NewProvider(context.Context, *flex.RemoteStorageTarget, filesystem.Provider, string) (cRst.Provider, error) {
	return nil, nil
}
func (fakeAzureType) Settings() []cRst.Setting {
	return []cRst.Setting{{
		Key: "account", Help: "The storage account.", Identity: true, RequiredOnCreate: true, Summary: true, Backend: cRst.BackendRequired,
		Get: func(c *flex.RemoteStorageTarget) string { return c.GetAzure().GetAccount() },
		Set: func(c *flex.RemoteStorageTarget, v string) error { c.GetAzure().SetAccount(v); return nil },
	}, {
		Key: "container", Help: "The container.", Identity: true, RequiredOnCreate: true, Backend: cRst.BackendForbidden,
		// The Azure message has no container field, so this test-only type keeps it in the bucket of
		// the message's S3 part.
		Get: func(c *flex.RemoteStorageTarget) string { return c.GetAzure().GetS3().GetBucket() },
		Set: func(c *flex.RemoteStorageTarget, v string) error {
			if !c.GetAzure().HasS3() {
				c.GetAzure().SetS3(&flex.RemoteStorageTarget_S3{})
			}
			c.GetAzure().GetS3().SetBucket(v)
			return nil
		},
	}}
}
func (fakeAzureType) VerifyBackend(context.Context, *flex.RemoteStorageTarget) (string, error) {
	return "", nil
}
func (fakeAzureType) Buckets() cRst.BucketManager { return nil }
func (fakeAzureType) Lists() []cRst.KeyedList     { return nil }
func (fakeAzureType) Example() string {
	return "  beegfs remote target create azure --name=blob --account=acme"
}
func (fakeAzureType) IdentityChanged(old, new *flex.RemoteStorageTarget) bool {
	return old.GetAzure().GetAccount() != new.GetAzure().GetAccount()
}

func TestNewTargetTypeNeedsNoCLICode(t *testing.T) {
	defer cRst.RegisterTargetTypeForTesting(fakeAzureType{})()

	create := newTargetCreateCmd()
	azureCmd, _, err := create.Find([]string{"azure"})
	require.NoError(t, err)
	backendCmd, _, err := newBackendCreateCmd().Find([]string{"azure"})
	require.NoError(t, err)
	assert.Equal(t, "azure", backendCmd.Name(), "backend create gets a subcommand for the new type")
	assert.NotNil(t, backendCmd.Flags().Lookup("account"))
	assert.Nil(t, backendCmd.Flags().Lookup("container"), "a setting the type forbids on a backend has no backend flag")
	assert.Contains(t, backendCmd.Long, "must set account. It cannot set container.", "the help states the type's backend roles")
	err = checkRequiredFlags(backendCmd, fakeAzureType{}, isBackendRequired, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"account"`)
	assert.NotContains(t, err.Error(), `"container"`)
	require.Equal(t, "azure", azureCmd.Name(), "create gets a subcommand for the new type")
	require.NotNil(t, azureCmd.Flags().Lookup("account"))
	err = checkRequiredFlags(azureCmd, fakeAzureType{}, isRequiredOnCreate, "")
	require.Error(t, err, "a setting required on create is required when no --backend is given")
	assert.Contains(t, err.Error(), `"account"`)
	assert.Nil(t, azureCmd.Flags().Lookup("bucket"), "create offers only the type's own flags")
	assert.Equal(t, fakeAzureType{}.Example(), azureCmd.Example, "the type's example is the subcommand's help example")

	modify := newTargetModifyCmd()
	require.NotNil(t, modify.Flags().Lookup("account"))
	assert.Contains(t, modify.Flags().Lookup("account").Usage, "(azure only)")
	assert.Contains(t, modify.Flags().Lookup("bucket").Usage, "(s3 and xtreemstore only)")

	target, err := rst.NewTarget("azure", 7)
	require.NoError(t, err)
	target, err = rst.ApplyTargetChanges(target, rst.TargetChanges{Settings: map[string]string{"account": "acme"}})
	require.NoError(t, err)
	assert.Equal(t, "acme", target.GetAzure().GetAccount())

	_, err = rst.ApplyTargetChanges(target, rst.TargetChanges{Settings: map[string]string{"bucket": "b"}})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not apply to azure targets")

	columns := newTargetColumns(false)
	assert.Contains(t, columns.all, "account")
	assert.Contains(t, columns.defaults, "account")
	row := columns.row(shownTarget{effective: target}, false, false)
	assert.Equal(t, "acme", row[4+indexOf(columns.settings, "account")])
	assert.Equal(t, "-", row[4+indexOf(columns.settings, "bucket")], "a column the type lacks shows -")
}

func indexOf(values []string, value string) int {
	for i, v := range values {
		if v == value {
			return i
		}
	}
	return -1
}
