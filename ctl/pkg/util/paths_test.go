package util

import (
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

// TestPathInputTypeSentinelAndUnmatched pins that the uninitialized sentinel and a value matching no
// variant render differently, so a PathInputMethod that was never run through
// DeterminePathInputMethod is distinguishable from one holding a value the enum does not cover.
func TestPathInputTypeSentinelAndUnmatched(t *testing.T) {
	assert.Equal(t, "stdin", PathInputStdin.String())
	assert.Equal(t, "invalid", PathInputInvalid.String())
	assert.Equal(t, "unknown(9)", PathInputType(9).String())
}

// TestResolveMountFromFirstPath covers the cases that need no BeeGFS mount. Resolving a real mount
// is left to manual tests.
func TestResolveMountFromFirstPath(t *testing.T) {
	t.Cleanup(viper.Reset)

	// A PathInputMethod that never ran through DeterminePathInputMethod has no first path.
	err := PathInputMethod{}.ResolveMountFromFirstPath()
	assert.ErrorContains(t, err, "path input method is invalid")

	// Stdin is never read early. Otherwise this would block on the test's stdin.
	pm, err := DeterminePathInputMethod([]string{"-"}, false, `\n`)
	require.NoError(t, err)
	assert.NoError(t, pm.ResolveMountFromFirstPath())

	// --mount already selects the filesystem, so nothing is checked, not even the input method.
	viper.Set(config.BeeGFSMountPointKey, config.BeeGFSMountPointNone)
	assert.NoError(t, PathInputMethod{}.ResolveMountFromFirstPath())
}
