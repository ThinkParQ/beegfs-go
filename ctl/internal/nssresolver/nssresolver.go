// Package nssresolver resolves user and group IDs and names, optionally by way of
// beegfs-nss-resolver so that a CGO_ENABLED=0 build can still resolve through NSS.
package nssresolver

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/thinkparq/beegfs-go/common/build"
)

// beegfs-nss-resolver is built with CGO enabled, so it resolves IDs through NSS. The path is fixed
// and deliberately never looked up in $PATH: the beegfs binary is installed setgid, so letting a
// caller choose the program executed here would run their code with the beegfs group's privileges.
const nssResolverPath = "/opt/beegfs/lib/beegfs-nss-resolver"

// lookupTimeout bounds every read from the helper, so a hung NSS backend degrades to numeric IDs
// instead of blocking the CLI forever. A variable so tests can shorten it.
var lookupTimeout = 10 * time.Second

// nssResolver resolves IDs by way of beegfs-nss-resolver, which is built with CGO enabled and so
// resolves through NSS. It is started on the first lookup and reused for the rest of the process.
// On success nothing shuts it down: the helper reads EOF and exits on its own once the CLI
// terminates and its stdin pipe closes. A failed round trip kills it, see retire.
type nssResolver struct {
	mu      sync.Mutex
	started bool
	// Sticky, so a helper that failed is neither re-execed nor asked again once per row of output.
	err    error
	proc   *exec.Cmd
	stdin  io.WriteCloser
	stdout *os.File
	enc    *json.Encoder
	dec    *json.Decoder
	seq    uint64
}

var resolver nssResolver

func (r *nssResolver) start() error {
	proc := exec.Command(nssResolverPath)
	// Name resolution needs no BeeGFS privilege. The CLI is installed setgid beegfs so it can read
	// the group-beegfs auth secret, which would otherwise leave this child running with
	// egid=beegfs. Mirrors index.CallerSysProcAttr, which does the same for the GUFI subprocesses.
	proc.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid:         uint32(os.Getuid()),
			Gid:         uint32(os.Getgid()),
			NoSetGroups: true,
		},
	}
	stdin, err := proc.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := proc.StdoutPipe()
	if err != nil {
		return err
	}
	// StdoutPipe returns the read end of an os.Pipe, which supports the deadline read puts on every
	// lookup. It isn't documented to, so check rather than assume.
	stdoutFile, ok := stdout.(*os.File)
	if !ok {
		return fmt.Errorf("unable to set a deadline on the stdout pipe of %s", nssResolverPath)
	}
	// The helper itself only writes to stderr right before exiting on an error. NSS modules loaded
	// into it may also write there, which lands where it would if the CLI resolved in-process.
	proc.Stderr = os.Stderr
	if err := proc.Start(); err != nil {
		return fmt.Errorf("unable to start %s: %w", nssResolverPath, err)
	}

	r.proc, r.stdin, r.stdout = proc, stdin, stdoutFile
	r.enc = json.NewEncoder(stdin)
	r.dec = json.NewDecoder(stdout)

	var greeting Greeting
	if err := r.read(&greeting); err != nil {
		return fmt.Errorf("unable to read greeting from %s: %w", nssResolverPath, err)
	}
	if greeting.Hello != Name {
		return fmt.Errorf("unexpected greeting from %s", nssResolverPath)
	}
	if greeting.Version != Version {
		return fmt.Errorf("%s speaks protocol version %d, expected %d", nssResolverPath,
			greeting.Version, Version)
	}
	return nil
}

// read decodes the helper's next message, giving up after lookupTimeout.
func (r *nssResolver) read(v any) error {
	if err := r.stdout.SetReadDeadline(time.Now().Add(lookupTimeout)); err != nil {
		return err
	}
	err := r.dec.Decode(v)
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return fmt.Errorf("no response within %s", lookupTimeout)
	}
	return err
}

// retire makes err sticky and kills the helper. A failed round trip can leave the answer to an
// abandoned request in the stream, so the helper can never be trusted again. Killing it rather
// than waiting for a stuck lookup to return also releases the stderr it shares with the CLI.
func (r *nssResolver) retire(err error) {
	r.err = err
	if r.proc == nil {
		return
	}
	r.stdin.Close()
	r.stdout.Close()
	r.proc.Process.Kill()
	go r.proc.Wait()
}

// resolve sends one request to the helper and returns its response, starting the helper on first
// use. Callers build the request and read whichever maps they asked to be filled.
func (r *nssResolver) resolve(req Request) (Response, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.started {
		r.started = true
		if err := r.start(); err != nil {
			r.retire(err)
		}
	}
	if r.err != nil {
		return Response{}, r.err
	}

	resp, err := r.roundTrip(req)
	if err != nil {
		r.retire(err)
		return Response{}, err
	}
	return resp, nil
}

func (r *nssResolver) roundTrip(req Request) (Response, error) {
	r.seq++
	req.Seq = r.seq
	if err := r.enc.Encode(&req); err != nil {
		return Response{}, fmt.Errorf("unable to send request to %s: %w", nssResolverPath, err)
	}

	var resp Response
	if err := r.read(&resp); err != nil {
		return Response{}, fmt.Errorf("unable to read response from %s: %w", nssResolverPath, err)
	}
	if resp.Seq != r.seq {
		return Response{}, fmt.Errorf("%s responded out of sequence (got %d, want %d)",
			nssResolverPath, resp.Seq, r.seq)
	}
	return resp, nil
}

// IdToName converts a user or group ID to its corresponding user or group name, fetched from the
// operating system's user and group database, or from beegfs-nss-resolver when nss is set. If not
// found it returns the ID as a string. If the lookup failed it returns the ID as a string together
// with the error, so a caller printing many rows can warn and keep going.
func IdToName(id uint32, idType string, nss bool) (string, error) {
	if idType != "user" && idType != "group" {
		return fmt.Sprintf("%d", id), fmt.Errorf("invalid idType: %s", idType)
	}

	// A CGO enabled build already resolves through NSS in os/user below, so the helper would only
	// add a process without changing the result.
	if nss && !build.CGO {
		req := Request{}
		if idType == "user" {
			req.UIDs = []uint32{id}
		} else {
			req.GIDs = []uint32{id}
		}
		resp, err := resolver.resolve(req)
		if err != nil {
			return fmt.Sprintf("%d", id), err
		}
		names, failed := resp.Users, resp.UIDErrors
		if idType == "group" {
			names, failed = resp.Groups, resp.GIDErrors
		}
		if name, ok := names[id]; ok {
			return name, nil
		}
		// Absent means the ID definitively does not exist, which falls through to printing it
		// numerically below. An entry in the error map means the lookup itself failed, which must
		// not be reported as if it had succeeded.
		if msg := failed[id]; msg != "" {
			return fmt.Sprintf("%d", id), fmt.Errorf("unable to look up %s %d: %s", idType, id, msg)
		}
		return fmt.Sprintf("%d", id), nil
	}

	switch idType {
	case "user":
		userName, err := user.LookupId(strconv.Itoa(int(id)))
		if err == nil {
			return userName.Username, nil
		}
	case "group":
		groupName, err := user.LookupGroupId(strconv.Itoa(int(id)))
		if err == nil {
			return groupName.Name, nil
		}
	}

	return fmt.Sprintf("%d", id), nil
}

// NamesToIds resolves names to IDs, returned as decimal strings so they can be appended to the
// --uids and --gids values. An unresolvable name is an error: unlike printing, silently dropping
// an ID the user asked for would be wrong.
func NamesToIds(names []string, idType string, nss bool) ([]string, error) {
	if len(names) == 0 {
		return nil, nil
	}
	ids := make([]string, 0, len(names))

	if nss && !build.CGO {
		req := Request{}
		if idType == "user" {
			req.Users = names
		} else {
			req.Groups = names
		}
		resp, err := resolver.resolve(req)
		if err != nil {
			return nil, err
		}
		found := resp.UserIDs
		if idType == "group" {
			found = resp.GroupIDs
		}
		for _, name := range names {
			id, ok := found[name]
			if !ok {
				if msg := resp.NameErrors[name]; msg != "" {
					return nil, fmt.Errorf("unable to look up %s %q: %s", idType, name, msg)
				}
				return nil, fmt.Errorf("unknown %s %q", idType, name)
			}
			ids = append(ids, strconv.FormatUint(uint64(id), 10))
		}
		return ids, nil
	}

	for _, name := range names {
		if idType == "user" {
			u, err := user.Lookup(name)
			if err != nil {
				return nil, err
			}
			ids = append(ids, u.Uid)
		} else {
			g, err := user.LookupGroup(name)
			if err != nil {
				return nil, err
			}
			ids = append(ids, g.Gid)
		}
	}
	return ids, nil
}
