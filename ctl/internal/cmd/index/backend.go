package index

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
)

const (
	indexAddrLocal   = "local"
	indexAddrPrefix  = "ssh:"
	indexAddrDefault = indexAddrLocal
)

type indexBackend struct {
	mode string
	host string
	port string
}

func (b indexBackend) isLocal() bool {
	return b.mode == indexAddrLocal
}

func parseIndexAddr(raw string) (indexBackend, error) {
	addr := strings.TrimSpace(raw)
	if addr == "" || addr == indexAddrLocal {
		return indexBackend{mode: indexAddrLocal}, nil
	}
	if addr == "ssh" {
		return parseIndexAddrWithDefaults(raw, "", "")
	}
	if !strings.HasPrefix(addr, indexAddrPrefix) {
		return indexBackend{}, invalidIndexAddr(raw)
	}
	value := strings.TrimPrefix(addr, indexAddrPrefix)
	host := ""
	port := ""
	if strings.Contains(value, ":") {
		parts := strings.Split(value, ":")
		if len(parts) != 2 {
			return indexBackend{}, invalidIndexAddr(raw)
		}
		host = strings.TrimSpace(parts[0])
		port = strings.TrimSpace(parts[1])
	} else {
		host = strings.TrimSpace(value)
	}

	return parseIndexAddrWithDefaults(raw, host, port)
}

func invalidIndexAddr(raw string) error {
	return fmt.Errorf("invalid index-addr %q: expected %q, %q, or %q", raw, indexAddrLocal, "ssh", "ssh:<host>[:port]")
}

func parseIndexAddrWithDefaults(raw, host, port string) (indexBackend, error) {
	defaultHost, defaultPort := defaultIndexAddrFromConfig()
	if host == "" {
		host = defaultHost
	}
	if port == "" {
		port = defaultPort
	}
	if host == "" {
		return indexBackend{}, invalidIndexAddr(raw)
	}
	if strings.ContainsAny(host, " \t\n") {
		return indexBackend{}, invalidIndexAddr(raw)
	}
	if port != "" {
		parsedPort, err := strconv.Atoi(port)
		if err != nil || parsedPort <= 0 || parsedPort > 65535 {
			return indexBackend{}, fmt.Errorf("invalid index-addr %q: port must be a number between 1 and 65535", raw)
		}
	}
	return indexBackend{mode: "ssh", host: host, port: port}, nil
}

func defaultIndexAddrFromConfig() (string, string) {
	host, _ := getGUFIConfigValue("Server")
	port, _ := getGUFIConfigValue("Port")
	return host, port
}

func buildIndexCommand(backend indexBackend, binary string, args []string) (*exec.Cmd, error) {
	if backend.isLocal() {
		return exec.Command(binary, args...), nil
	}
	if backend.host == "" {
		return nil, invalidIndexAddr(backend.mode)
	}
	sshArgs := make([]string, 0, 4)
	if backend.port != "" {
		sshArgs = append(sshArgs, "-p", backend.port)
	}
	sshArgs = append(sshArgs, "--", backend.host, buildRemoteCommand(binary, args))
	return exec.Command("ssh", sshArgs...), nil
}

func buildRemoteCommand(binary string, args []string) string {
	parts := make([]string, 0, len(args)+1)
	parts = append(parts, shellQuote(binary))
	for _, arg := range args {
		parts = append(parts, shellQuote(arg))
	}
	return strings.Join(parts, " ")
}

func shellQuote(arg string) string {
	if arg == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(arg, "'", `'\''`) + "'"
}

func runIndexCommand(backend indexBackend, binary string, args []string, handle func(io.Reader) error) error {
	cmd, err := buildIndexCommand(backend, binary, args)
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("unable to start index command: %w", err)
	}
	handleErr := handle(stdout)
	waitErr := cmd.Wait()
	if handleErr != nil {
		if waitErr != nil {
			return fmt.Errorf("%w (and command error: %v)", handleErr, waitErr)
		}
		return handleErr
	}
	if waitErr != nil {
		return fmt.Errorf("error executing index command: %w", waitErr)
	}
	return nil
}
