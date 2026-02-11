package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildDir2IndexArgs_AppendsPaths(t *testing.T) {
	args := buildDir2IndexArgs("fs", "idx", []string{"-n", "4", "--max-level", "2"})
	assert.Equal(t, []string{
		"-n", "4",
		"--max-level", "2",
		"--plugin", dir2IndexPluginPath,
		"fs", "idx",
	}, args)
}

func TestBuildDir2IndexArgs_ForcesPluginPath(t *testing.T) {
	args := buildDir2IndexArgs("fs", "idx", []string{"--plugin", "/tmp/plugin.so", "-n", "4"})
	assert.Equal(t, []string{
		"-n", "4",
		"--plugin", dir2IndexPluginPath,
		"fs", "idx",
	}, args)
}

func TestBuildDir2IndexArgs_NoMetadataSkipsPlugin(t *testing.T) {
	args := buildDir2IndexArgs("fs", "idx", []string{"-B", "--plugin", "/tmp/plugin.so"})
	assert.Equal(t, []string{
		"-B",
		"fs", "idx",
	}, args)
}

func TestBuildTreeSummaryArgs_BuildsArgs(t *testing.T) {
	opts := treeSummaryOptions{
		threads: 8,
		debug:   true,
	}
	args := buildTreeSummaryArgs("index", opts)
	assert.Equal(t, []string{
		"-H",
		"-n", "8",
		"index",
	}, args)
}
