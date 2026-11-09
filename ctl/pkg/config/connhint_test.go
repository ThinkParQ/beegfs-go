package config

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConnHint(t *testing.T) {
	unavailable := func(msg string) error { return status.Error(codes.Unavailable, msg) }

	cases := []struct {
		err       error
		nonLegacy bool
		hint      string
	}{
		{unavailable("remote error: tls: no application protocol"), true, "uses a different connection protocol"},
		{unavailable("remote error: tls: no application protocol"), false, "uses the plain or encrypted connection protocol"},
		{unavailable("The peer's key pair ID doesn't match the expected one (E_PEER_MISMATCH)"), true, "or the management uses the legacy connection protocol"},
		{unavailable("tls: first record does not look like a TLS handshake"), true, "legacy connection protocol with TLS disabled"},
		{unavailable("tls: first record does not look like a TLS handshake"), false, "does not use TLS, check --tls-disable"},
		{unavailable("error reading server preface: EOF"), false, "--tls-disable"},
		{unavailable("error reading server preface: EOF"), true, ""},
		{status.Error(codes.Unauthenticated, "no application protocol"), true, ""},
		{errors.New("no application protocol"), true, ""},
	}

	for _, c := range cases {
		got := connHint(c.err, c.nonLegacy)
		if c.hint == "" {
			assert.Equal(t, c.err, got)
			continue
		}
		assert.Contains(t, got.Error(), c.hint)
		assert.Equal(t, codes.Unavailable, status.Code(got))
	}
}
