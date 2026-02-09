package index

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeBeeGFSLsDelim_WhitespaceErrors(t *testing.T) {
	delim, err := normalizeBeeGFSLsDelim(" ")
	assert.Error(t, err)
	assert.Equal(t, "", delim)
}

func TestNormalizeBeeGFSLsDelim_TabAllowed(t *testing.T) {
	delim, err := normalizeBeeGFSLsDelim("\t")
	assert.NoError(t, err)
	assert.Equal(t, "\t", delim)
}

func TestNormalizeBeeGFSLsDelim_NonWhitespaceAllowed(t *testing.T) {
	delim, err := normalizeBeeGFSLsDelim("|")
	assert.NoError(t, err)
	assert.Equal(t, "|", delim)
}

func TestNormalizeBeeGFSLsDelim_EmptyErrors(t *testing.T) {
	delim, err := normalizeBeeGFSLsDelim("")
	assert.Error(t, err)
	assert.Equal(t, "", delim)
}
