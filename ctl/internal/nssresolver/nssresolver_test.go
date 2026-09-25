package nssresolver

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Checks that resolve gives up on a helper that stops answering, and that the resolver then stays
// failed rather than reading a late answer as the reply to the next request.
func TestResolveTimesOut(t *testing.T) {
	orig := lookupTimeout
	lookupTimeout = 50 * time.Millisecond
	t.Cleanup(func() { lookupTimeout = orig })

	// A helper that reads requests and never answers.
	stdinR, stdinW, err := os.Pipe()
	require.NoError(t, err)
	stdoutR, stdoutW, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { stdinR.Close(); stdinW.Close(); stdoutR.Close(); stdoutW.Close() })

	r := &nssResolver{started: true, stdin: stdinW, stdout: stdoutR,
		enc: json.NewEncoder(stdinW), dec: json.NewDecoder(stdoutR)}

	_, err = r.resolve(Request{UIDs: []uint32{0}})
	require.ErrorContains(t, err, "no response within")

	_, again := r.resolve(Request{UIDs: []uint32{0}})
	assert.Equal(t, err, again)
}
