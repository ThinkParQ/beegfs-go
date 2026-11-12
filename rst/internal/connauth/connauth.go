// Package connauth decides how Remote and Sync use BeeGFS connection authentication ("ConnAuth").
//
// ConnAuth proves that a peer holds the file system's shared secret file, usually
// /etc/beegfs/conn.auth. Peers never send the file. They send a 64-bit hash of it, and servers
// compare hashes (see beegrpc.AuthServerOptions).
//
// Remote and Sync take their ConnAuth state from the BeeGFS client that serves their mount point.
// Two facts about the client make that safe:
//
//   - The client reports its settings in /proc/fs/beegfs/<client>/config. connDisableAuthentication
//     says whether it uses ConnAuth, and connAuthFile names the secret file.
//   - A client that uses ConnAuth refuses to mount without a readable auth file. A client that does
//     not use ConnAuth cannot mount a file system whose servers require it. So a working mount
//     shows the client's settings are usable against this file system.
//
// The state decides two things at once: whether the service's gRPC server requires the hash from
// callers, and which hash the service sends on its own calls.
//
// The optional [auth] settings cover environments where the client's settings or its auth file
// cannot be read, such as a container that hides /proc/fs/beegfs or does not have the file at the
// path the client reports. They have no defaults, so an absent key is distinguishable from false.
// The client stays the authority whenever it can be read:
//
//	| Client settings | [auth] settings      | Result                                          |
//	|-----------------|----------------------|-------------------------------------------------|
//	| readable        | none                 | follow the client, read connAuthFile            |
//	| readable        | auth-file            | follow the client, read auth-file instead       |
//	| readable        | contradict client    | error                                           |
//	| not readable    | auth-file            | ConnAuth on, read auth-file                     |
//	| not readable    | auth-disable = true  | ConnAuth off                                    |
//	| not readable    | none                 | error                                           |
//
// "Contradict" means auth-disable = true while the client uses ConnAuth, or auth-file while the
// client does not. Setting both auth-file and auth-disable = true is an error. auth-disable = false
// means the same as leaving it unset.
//
// An error must stop the service, because it cannot tell whether to check callers. Running without
// checks on a file system that uses ConnAuth is never the fallback: a Sync node runs as root on a
// mount that skips file permission checks, so an open Sync node would let anyone read or overwrite
// any file in BeeGFS.
package connauth

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/thinkparq/beegfs-go/common/logger"
	"github.com/thinkparq/beegfs-go/ctl/pkg/ctl/procfs"
	"go.uber.org/zap"
)

const (
	// Keys the BeeGFS client writes to its procfs config file.
	disableAuthKey = "connDisableAuthentication"
	authFileKey    = "connAuthFile"
)

// getClients reads the BeeGFS clients from /proc/fs/beegfs. FromMount calls it through this
// variable, so tests can return fixed clients without a real mount.
var getClients = procfs.GetBeeGFSClients

// Settings are the optional [auth] settings of Remote and Sync. A nil field was not set. They are
// read from the configuration file or environment only. A command line flag always has a default,
// so an unset key could not be told apart.
type Settings struct {
	AuthFile    *string `mapstructure:"auth-file"`
	AuthDisable *bool   `mapstructure:"auth-disable"`
}

// file returns the configured auth file, or "" when unset.
func (s Settings) file() string {
	if s.AuthFile == nil {
		return ""
	}
	return *s.AuthFile
}

// disabled reports whether auth-disable is set to true.
func (s Settings) disabled() bool {
	return s.AuthDisable != nil && *s.AuthDisable
}

// State is the ConnAuth state a service uses.
type State struct {
	// Secret is the content of the auth file, or nil when ConnAuth is off.
	Secret []byte
	// AuthFile is the path the secret was read from, or empty when Secret is nil.
	AuthFile string
	// Source names what decided the state, for logs: the client or an [auth] setting.
	Source string
}

// Enabled reports whether ConnAuth is on.
func (s State) Enabled() bool {
	return s.Secret != nil
}

const (
	sourceClient      = "BeeGFS client"
	sourceAuthFile    = "auth.auth-file"
	sourceAuthDisable = "auth.auth-disable"
)

// FromMount runs once at service start and returns the ConnAuth state the service must use. It
// logs the result. mountPath is the service's mount-point setting. Any directory inside the mount
// works, including a bind mount. settings are the service's [auth] settings. An error means the
// service must not start.
func FromMount(ctx context.Context, mountPath string, settings Settings, log *logger.Logger) (State, error) {
	if mountPath == "" {
		return State{}, errors.New("mount-point must be set so connection authentication can be read from the BeeGFS client")
	}
	clients, err := getClients(ctx, procfs.GetBeeGFSClientsConfig{FilterByMounts: []string{mountPath}}, log)
	if err != nil {
		return State{}, fmt.Errorf("reading the BeeGFS client configuration from /proc/fs/beegfs: %w", err)
	}
	if len(clients) > 1 {
		return State{}, fmt.Errorf("found %d BeeGFS clients in /proc/fs/beegfs for mount point %s, expected one", len(clients), mountPath)
	}
	var clientCfg map[string]string
	if len(clients) == 1 {
		clientCfg = clients[0].Config
	}

	state, err := resolve(clientCfg, settings, mountPath)
	if err != nil {
		return State{}, err
	}
	if state.Enabled() {
		log.Info("using BeeGFS connection authentication: requiring the secret from all callers", zap.String("source", state.Source), zap.String("authFile", state.AuthFile))
	} else {
		log.Warn("not using BeeGFS connection authentication: not requiring a secret from callers", zap.String("source", state.Source))
	}
	return state, nil
}

// resolve applies the table in the package comment. clientCfg is the procfs config of the client
// that serves mountPath, or nil when no client was found. A client that does not report
// connDisableAuthentication counts as not readable, because it predates reporting it.
func resolve(clientCfg map[string]string, settings Settings, mountPath string) (State, error) {
	if settings.file() != "" && settings.disabled() {
		return State{}, errors.New("auth.auth-file and auth.auth-disable cannot both be set")
	}

	clientDisabled, readable := clientCfg[disableAuthKey]
	if !readable {
		switch {
		case settings.file() != "":
			return readSecret(settings.file(), sourceAuthFile)
		case settings.disabled():
			return State{Source: sourceAuthDisable}, nil
		}
		return State{}, fmt.Errorf("cannot read the connection authentication settings of the BeeGFS client of %s from /proc/fs/beegfs (hint: make /proc/fs/beegfs readable where this service runs, or set auth.auth-file or auth.auth-disable)", mountPath)
	}

	switch clientDisabled {
	case "true":
		if settings.file() != "" {
			return State{}, fmt.Errorf("auth.auth-file is set, but the BeeGFS client of %s does not use connection authentication", mountPath)
		}
		return State{Source: sourceClient}, nil
	case "false":
	default:
		return State{}, fmt.Errorf("the BeeGFS client of %s reports an unexpected value %q for %s", mountPath, clientDisabled, disableAuthKey)
	}

	if settings.disabled() {
		return State{}, fmt.Errorf("auth.auth-disable is set, but the BeeGFS client of %s uses connection authentication", mountPath)
	}
	if settings.file() != "" {
		return readSecret(settings.file(), sourceAuthFile)
	}
	path := clientCfg[authFileKey]
	if path == "" {
		return State{}, fmt.Errorf("the BeeGFS client of %s uses connection authentication but reports no %s", mountPath, authFileKey)
	}
	state, err := readSecret(path, sourceClient)
	if err != nil {
		return State{}, fmt.Errorf("%w (hint: the auth file must be readable at the path the BeeGFS client reports, where this service runs, or set auth.auth-file)", err)
	}
	return state, nil
}

// readSecret reads the auth file at path. An empty file is an error, because the hash of an empty
// file would be a valid but trivial secret.
func readSecret(path string, source string) (State, error) {
	secret, err := os.ReadFile(path)
	if err != nil {
		return State{}, fmt.Errorf("reading the auth file from %s: %w", source, err)
	}
	if len(secret) == 0 {
		return State{}, fmt.Errorf("the auth file %s from %s is empty", path, source)
	}
	return State{Secret: secret, AuthFile: path, Source: source}, nil
}
