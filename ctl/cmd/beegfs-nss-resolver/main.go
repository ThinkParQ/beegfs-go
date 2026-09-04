// Command beegfs-nss-resolver resolves UIDs and GIDs to user and group names on behalf of the
// beegfs CLI.
//
// The CLI and the other Go binaries are built with CGO_ENABLED=0, which makes os/user fall back to
// parsing /etc/passwd and /etc/group directly. That fallback never consults NSS, so any identity
// that lives in LDAP, SSSD or AD fails to resolve. This helper does nothing the CLI could not do
// itself. Its entire purpose is to be the one binary built with CGO_ENABLED=1, so that the very
// same os/user calls resolve through NSS. Building it without cgo produces a binary that answers
// every request exactly as the CLI already would, so main refuses to run in that case.
//
// It speaks newline delimited JSON over stdin/stdout and is intended to be run as a long lived
// co-process: the CLI starts it once and reuses it rather than paying a fork/exec per lookup.
// Diagnostics go to stderr only, so stdout stays a clean protocol stream.
//
//	< {"hello":"beegfs-nss-resolver","version":1}
//	> {"seq":1,"uids":[1000,1001,4242],"gids":[100]}
//	< {"seq":1,"users":{"1000":"alice","1001":"bob"},"groups":{"100":"users"},"uid_errors":{"4242":"connection refused"},"gid_errors":{}}
//
// An ID missing from the users/groups map was definitively not found and the caller may cache that
// negative result. An ID reported in uid_errors/gid_errors failed to resolve because the lookup
// itself failed (the NSS backend is down, timed out, ...) and must be retried later rather than
// cached.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"strconv"

	"github.com/thinkparq/beegfs-go/common/build"
)

const protocolVersion = 1

type request struct {
	Seq  uint64   `json:"seq"`
	UIDs []uint32 `json:"uids,omitempty"`
	GIDs []uint32 `json:"gids,omitempty"`
}

type response struct {
	Seq       uint64            `json:"seq"`
	Users     map[uint32]string `json:"users"`
	Groups    map[uint32]string `json:"groups"`
	UIDErrors map[uint32]string `json:"uid_errors"`
	GIDErrors map[uint32]string `json:"gid_errors"`
}

// handle resolves every ID in the request. Lookups are serial: a warm NSS lookup is a round trip
// to a local daemon, and callers resolve one ID per row of output they are already streaming.
//
// An ID that is absent from every NSS source is simply left out of the response. An ID whose
// lookup failed is reported in one of the error maps instead, so the caller can tell "no such
// user" apart from "ask again later".
func handle(req request) response {
	resp := response{
		Seq:       req.Seq,
		Users:     map[uint32]string{},
		Groups:    map[uint32]string{},
		UIDErrors: map[uint32]string{},
		GIDErrors: map[uint32]string{},
	}

	for _, uid := range req.UIDs {
		u, err := user.LookupId(strconv.FormatUint(uint64(uid), 10))
		var unknown user.UnknownUserIdError
		switch {
		case errors.As(err, &unknown):
		case err != nil:
			resp.UIDErrors[uid] = err.Error()
		default:
			resp.Users[uid] = u.Username
		}
	}

	for _, gid := range req.GIDs {
		g, err := user.LookupGroupId(strconv.FormatUint(uint64(gid), 10))
		var unknown user.UnknownGroupIdError
		switch {
		case errors.As(err, &unknown):
		case err != nil:
			resp.GIDErrors[gid] = err.Error()
		default:
			resp.Groups[gid] = g.Name
		}
	}

	return resp
}

func main() {
	if !build.CGO {
		// Without cgo os/user reads /etc/passwd and /etc/group directly, which is exactly what the
		// caller already does for itself. Answering anyway would silently look like it worked.
		fmt.Fprintln(os.Stderr, "beegfs-nss-resolver: built without cgo, lookups would not use NSS")
		os.Exit(1)
	}

	out := bufio.NewWriter(os.Stdout)
	enc := json.NewEncoder(out)
	if err := enc.Encode(map[string]any{"hello": "beegfs-nss-resolver", "version": protocolVersion}); err != nil {
		fmt.Fprintln(os.Stderr, "beegfs-nss-resolver: unable to write greeting:", err)
		os.Exit(1)
	}
	if err := out.Flush(); err != nil {
		fmt.Fprintln(os.Stderr, "beegfs-nss-resolver: unable to write greeting:", err)
		os.Exit(1)
	}

	dec := json.NewDecoder(bufio.NewReader(os.Stdin))
	for {
		var req request
		if err := dec.Decode(&req); err != nil {
			// Either the CLI closed the pipe or the stream contains something we cannot resync
			// from. Both mean this process has no further work to do.
			return
		}
		resp := handle(req)
		if err := enc.Encode(&resp); err != nil {
			fmt.Fprintln(os.Stderr, "beegfs-nss-resolver: unable to write response:", err)
			os.Exit(1)
		}
		if err := out.Flush(); err != nil {
			fmt.Fprintln(os.Stderr, "beegfs-nss-resolver: unable to write response:", err)
			os.Exit(1)
		}
	}
}
