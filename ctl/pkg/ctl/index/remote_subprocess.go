package index

import (
	"fmt"
	"strings"
)

// sshTransportOpts: BatchMode=yes (fail fast), ServerAliveInterval+CountMax (detect dead remote ~10s),
// "--" terminates option parsing, no -t (byte-clean output for line streaming).
var sshTransportOpts = []string{
	"-o", "BatchMode=yes",
	"-o", "ServerAliveInterval=5",
	"-o", "ServerAliveCountMax=2",
	"--",
}

func shellQuoteArg(arg string) string {
	return "'" + strings.ReplaceAll(arg, "'", `'\''`) + "'"
}

// IsRemoteAddr reports whether index-addr targets a remote host over ssh.
func IsRemoteAddr(indexAddr string) bool {
	return strings.HasPrefix(indexAddr, "ssh:")
}

func resolveSSHHost(indexAddr string) (string, error) {
	switch {
	case indexAddr == "" || indexAddr == "local":
		return "", nil
	case IsRemoteAddr(indexAddr):
		host := strings.TrimPrefix(indexAddr, "ssh:")
		if err := validateSSHHost(host); err != nil {
			return "", fmt.Errorf("invalid index-addr %q: %w", indexAddr, err)
		}
		return host, nil
	default:
		return "", fmt.Errorf("invalid index-addr %q: expected local or ssh:<host>", indexAddr)
	}
}

// WrapForRemote rewrites (bin, args) to run on the remote host via ssh when
// indexAddr is remote; otherwise it returns them unchanged.
// Two shell-quoting layers: inner (args into script), outer (script as ssh arg).
// exec replaces bash with target binary so SIGHUP on ssh close reaches it directly.
func WrapForRemote(bin string, args []string, indexAddr string) (string, []string, error) {
	host, err := resolveSSHHost(indexAddr)
	if err != nil {
		return "", nil, err
	}
	if host == "" {
		return bin, args, nil
	}

	var script strings.Builder
	script.WriteString("exec ")
	script.WriteString(shellQuoteArg(bin))
	for _, a := range args {
		script.WriteByte(' ')
		script.WriteString(shellQuoteArg(a))
	}

	sshArgs := make([]string, 0, len(sshTransportOpts)+4)
	sshArgs = append(sshArgs, sshTransportOpts...)
	sshArgs = append(sshArgs, host, "bash", "-c", shellQuoteArg(script.String()))
	return "ssh", sshArgs, nil
}
