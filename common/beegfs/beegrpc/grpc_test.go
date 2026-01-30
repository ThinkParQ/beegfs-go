package beegrpc_test

import (
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/thinkparq/beegfs-go/common/beegfs/beegrpc"
	"google.golang.org/grpc"
)

func TestNewClientConnAcceptsValidIPv4(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on IPv4 loopback: %v", err)
	}
	defer lis.Close()

	server := grpc.NewServer()
	defer server.Stop()
	go server.Serve(lis)

	conn, err := beegrpc.NewClientConn(lis.Addr().String(), beegrpc.WithTLSDisable(true))
	if err != nil {
		t.Fatalf("expected IPv4 address to be accepted, got error: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
	})
}

func TestNewClientConnAcceptsValidIPv6(t *testing.T) {
	lis, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("skipping IPv6 test, loopback not available: %v", err)
		return
	}
	defer lis.Close()

	server := grpc.NewServer()
	defer server.Stop()
	go server.Serve(lis)

	conn, err := beegrpc.NewClientConn(lis.Addr().String(), beegrpc.WithTLSDisable(true))
	if err != nil {
		t.Fatalf("expected IPv6 address to be accepted, got error: %v", err)
	}
	t.Cleanup(func() {
		conn.Close()
	})
}

func TestNewClientConnRejectsInvalidAddresses(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name    string
		address string
	}{
		{name: "missingPortIPv4", address: "127.0.0.1"},
		{name: "missingBracketIPv6", address: "fd38:fc6f:dfe0:8432:a00:27ff:fe09:e0da"},
		{name: "missingClosingBracketIPv6", address: "[fd38:fc6f:dfe0:8432:a00:27ff:fe09:e0da:8010"},
		{name: "emptyAddress", address: ""},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conn, err := beegrpc.NewClientConn(tc.address, beegrpc.WithTLSDisable(true))
			if err == nil {
				if conn != nil {
					conn.Close()
				}
				t.Fatalf("expected address %q to be rejected but got no error", tc.address)
			}

			if conn != nil {
				t.Fatalf("expected nil connection on error for %q", tc.address)
			}

			if !strings.Contains(err.Error(), "invalid target address") {
				t.Fatalf("expected invalid target address error for %q, got: %v", tc.address, err)
			}

			if !strings.Contains(err.Error(), "for IPv6, use the '[address]:port' notation") {
				t.Fatalf("expected IPv6 usage hint to be present for %q, got: %v", tc.address, err)
			}

			var addrErr *net.AddrError
			if !errors.As(err, &addrErr) {
				t.Fatalf("expected %q to wrap *net.AddrError, got: %T", err, err)
			}

			if addrErr.Addr != tc.address {
				t.Fatalf("expected address %q in wrapped AddrError, got %q", tc.address, addrErr.Addr)
			}

			if !strings.Contains(err.Error(), addrErr.Err) {
				t.Fatalf("expected underlying addr error %q in returned error: %v", addrErr.Err, err)
			}
		})
	}
}

func TestNewClientConnRejectsURIScheme(t *testing.T) {
	t.Parallel()
	conn, err := beegrpc.NewClientConn("dns:///1.2.3.4:8000", beegrpc.WithTLSDisable(true))
	if err == nil {
		if conn != nil {
			conn.Close()
		}
		t.Fatalf("expected URI scheme to be rejected but got no error")
	}

	if conn != nil {
		t.Fatalf("expected nil connection when URI scheme is rejected")
	}

	if !strings.Contains(err.Error(), "specifying a URI scheme in the address is not supported") {
		t.Fatalf("unexpected error when rejecting URI scheme: %v", err)
	}
}
