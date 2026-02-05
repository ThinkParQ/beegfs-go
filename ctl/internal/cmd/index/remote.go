package index

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
)

const (
	indexAddrFlag    = "index-addr"
	indexAddrDefault = "local"
)

type indexAddrKind int

const (
	indexAddrLocal indexAddrKind = iota
	indexAddrSSH
)

type indexAddr struct {
	kind indexAddrKind
	host string
	port int
}

func (a indexAddr) isLocal() bool {
	return a.kind == indexAddrLocal
}

func addIndexAddrFlag(cmd *cobra.Command) {
	cmd.PersistentFlags().String(indexAddrFlag, indexAddrDefault,
		`Index backend address. Use "local" or "ssh:<host>[:port]".`)
}

func resolveIndexAddr(cmd *cobra.Command) (indexAddr, error) {
	raw, err := cmd.Flags().GetString(indexAddrFlag)
	if err != nil {
		return indexAddr{}, err
	}
	return parseIndexAddr(raw)
}

func parseIndexAddr(raw string) (indexAddr, error) {
	addr := strings.TrimSpace(raw)
	if addr == "" || addr == indexAddrDefault {
		return indexAddr{kind: indexAddrLocal}, nil
	}
	if !strings.HasPrefix(addr, "ssh:") {
		return indexAddr{}, fmt.Errorf("unsupported index address %q (expected %s or ssh:<host>[:port])", raw, indexAddrDefault)
	}

	rest := strings.TrimPrefix(addr, "ssh:")
	if rest == "" {
		return indexAddr{}, fmt.Errorf("invalid index address %q: missing ssh host", raw)
	}

	host, port, err := parseSSHHostPort(rest)
	if err != nil {
		return indexAddr{}, fmt.Errorf("invalid index address %q: %w", raw, err)
	}
	return indexAddr{kind: indexAddrSSH, host: host, port: port}, nil
}

func parseSSHHostPort(raw string) (string, int, error) {
	const defaultPort = 22

	if raw == "" {
		return "", 0, fmt.Errorf("missing host")
	}

	if strings.HasPrefix(raw, "[") {
		if strings.HasSuffix(raw, "]") {
			host := strings.TrimSuffix(strings.TrimPrefix(raw, "["), "]")
			if host == "" {
				return "", 0, fmt.Errorf("empty host")
			}
			return host, defaultPort, nil
		}
		host, port, err := net.SplitHostPort(raw)
		if err != nil {
			return "", 0, err
		}
		p, err := parsePort(port)
		if err != nil {
			return "", 0, err
		}
		return host, p, nil
	}

	if strings.Count(raw, ":") == 1 {
		host, port, err := net.SplitHostPort(raw)
		if err != nil {
			return "", 0, err
		}
		p, err := parsePort(port)
		if err != nil {
			return "", 0, err
		}
		return host, p, nil
	}

	if strings.Count(raw, ":") > 1 {
		return raw, defaultPort, nil
	}

	return raw, defaultPort, nil
}

func parsePort(raw string) (int, error) {
	if raw == "" {
		return 0, fmt.Errorf("missing port")
	}
	p, err := strconv.Atoi(raw)
	if err != nil || p < 1 || p > 65535 {
		return 0, fmt.Errorf("invalid port %q", raw)
	}
	return p, nil
}

func buildIndexCommand(target indexAddr, localBinary, remoteBinary string, args []string) (*exec.Cmd, error) {
	if target.isLocal() {
		return exec.Command(localBinary, args...), nil
	}
	if target.kind != indexAddrSSH {
		return nil, fmt.Errorf("unsupported index address type")
	}
	if remoteBinary == "" {
		return nil, fmt.Errorf("missing remote binary")
	}
	if target.host == "" {
		return nil, fmt.Errorf("missing remote host")
	}

	remoteArgs := make([]string, 0, len(args)+1)
	remoteArgs = append(remoteArgs, remoteBinary)
	remoteArgs = append(remoteArgs, args...)
	quoted := make([]string, 0, len(remoteArgs))
	for _, arg := range remoteArgs {
		quoted = append(quoted, shellQuote(arg))
	}

	cmd := exec.Command(
		"ssh",
		target.host,
		"-p", strconv.Itoa(target.port),
		"--",
		strings.Join(quoted, " "),
	)
	return cmd, nil
}

func runIndexCommand(cmdExec *exec.Cmd, usePrintomatic bool) error {
	if !usePrintomatic {
		cmdExec.Stdout = os.Stdout
		cmdExec.Stderr = os.Stderr
		if err := cmdExec.Start(); err != nil {
			return fmt.Errorf("unable to start index command: %w", err)
		}
		if err := cmdExec.Wait(); err != nil {
			return fmt.Errorf("error executing index command: %w", err)
		}
		return nil
	}

	stdout, err := cmdExec.StdoutPipe()
	if err != nil {
		return fmt.Errorf("unable to capture index command output: %w", err)
	}
	cmdExec.Stderr = os.Stderr
	if err := cmdExec.Start(); err != nil {
		return fmt.Errorf("unable to start index command: %w", err)
	}

	tbl := cmdfmt.NewPrintomatic([]string{"row"}, []string{"row"})
	defer tbl.PrintRemaining()

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		tbl.AddItem(scanner.Text())
	}
	scanErr := scanner.Err()
	waitErr := cmdExec.Wait()
	if scanErr != nil || waitErr != nil {
		return fmt.Errorf("error executing index command: %w", errors.Join(scanErr, waitErr))
	}
	return nil
}

func shellQuote(arg string) string {
	if arg == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(arg, "'", `'\'\''`) + "'"
}
