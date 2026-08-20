package iotest

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// NodeSpec identifies a remote node. ExtraArgs is reserved for future per-node
// flag overrides and is always empty in the current implementation.
type NodeSpec struct {
	Name      string
	ExtraArgs []string
}

// parseNodes parses a -N value into a slice of NodeSpecs. The value is treated
// as a file path if a file exists at that path; otherwise it is parsed as a
// comma-separated list of node names. File format: node names separated by
// newlines and/or commas; everything from a # to the end of the line is a
// comment, whether the line starts with # or the # follows one or more node
// names (e.g. "node1   # rack 9").
func parseNodes(value string) ([]NodeSpec, error) {
	if _, err := os.Stat(value); err == nil {
		return parseNodesFile(value)
	}
	return parseNodesInline(value)
}

func parseNodesFile(path string) ([]NodeSpec, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open nodes file %s: %w", path, err)
	}
	defer f.Close()

	var nodes []NodeSpec
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if idx := strings.IndexByte(line, '#'); idx >= 0 {
			line = line[:idx]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		for _, name := range strings.Split(line, ",") {
			if name = strings.TrimSpace(name); name != "" {
				nodes = append(nodes, NodeSpec{Name: name})
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read nodes file %s: %w", path, err)
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no node names found in %s", path)
	}
	return nodes, nil
}

func parseNodesInline(value string) ([]NodeSpec, error) {
	var nodes []NodeSpec
	for _, name := range strings.Split(value, ",") {
		if name = strings.TrimSpace(name); name != "" {
			nodes = append(nodes, NodeSpec{Name: name})
		}
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no node names found in %q", value)
	}
	return nodes, nil
}

// buildRemoteArgs returns os.Args[1:] with flags that must not be forwarded to
// remote nodes stripped:
//
//   - -N/--nodes: stripped so remote nodes do not recurse into multi-node mode.
//   - --fresh: stripped because the primary invocation already cleaned the shared
//     path before launching remote nodes; re-running it remotely would race with
//     the primary node's own setup and delete its data and log files.
func buildRemoteArgs(osArgs []string) []string {
	result := make([]string, 0, len(osArgs))
	skip := false
	for _, arg := range osArgs[1:] {
		if skip {
			skip = false
			continue
		}
		if arg == "-N" || arg == "--nodes" {
			skip = true
			continue
		}
		if strings.HasPrefix(arg, "-N=") || strings.HasPrefix(arg, "--nodes=") {
			continue
		}
		if arg == "--fresh" || strings.HasPrefix(arg, "--fresh=") {
			continue
		}
		result = append(result, arg)
	}
	return result
}

// buildRemoteArgsForWorkload is like buildRemoteArgs but also strips all
// workload names from the positional args except target. This produces a
// single-workload invocation when multiple workloads were given on the
// command line (e.g. "start soak bench" → "start soak" for target=soak).
func buildRemoteArgsForWorkload(osArgs []string, allWorkloads []string, target string) []string {
	others := make(map[string]bool, len(allWorkloads))
	for _, n := range allWorkloads {
		if n != target {
			others[n] = true
		}
	}
	base := buildRemoteArgs(osArgs)
	result := make([]string, 0, len(base))
	// Workload names are always leading positional args, before any flag --
	// see "Usage: beegfs iotest start [workload...] [flags]". Only strip
	// matches from that leading run. Once a flag-looking token ("-...")
	// appears, every remaining token is a flag or a flag's value and must
	// be passed through verbatim, even if a value happens to equal another
	// workload's name (e.g. a hypothetical --tag=soak on some other flag).
	inLeadingPositionals := true
	for _, arg := range base {
		if inLeadingPositionals {
			if strings.HasPrefix(arg, "-") {
				inLeadingPositionals = false
			} else if others[arg] {
				continue
			}
		}
		result = append(result, arg)
	}
	return result
}

// sshStartBackground starts `beegfs <remoteArgs>` on node via SSH as a
// background process, redirecting output to logPath on the remote host.
// Returns as soon as the background process is launched.
func sshStartBackground(ctx context.Context, node NodeSpec, remoteArgs []string, logPath string) error {
	// Quote logPath too (not just the args): it derives from the user's --path,
	// so an unquoted redirect target would break or inject on a path containing
	// spaces or shell metacharacters.
	remote := fmt.Sprintf("nohup beegfs %s > %s 2>&1 &", shellJoin(remoteArgs), shellJoin([]string{logPath}))
	cmd := exec.CommandContext(ctx, "ssh", "-o", "BatchMode=yes", node.Name, remote)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ssh %s: %w\n%s", node.Name, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// shellJoin produces a space-separated string of single-quoted arguments,
// safe for interpolation into a remote shell command.
func shellJoin(args []string) string {
	quoted := make([]string, len(args))
	for i, arg := range args {
		quoted[i] = "'" + strings.ReplaceAll(arg, "'", `'\''`) + "'"
	}
	return strings.Join(quoted, " ")
}
