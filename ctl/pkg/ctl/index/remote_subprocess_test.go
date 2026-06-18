package index

import (
	"strings"
	"testing"
)

func TestShellQuoteArg(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", "''"},
		{"plain", "abc", "'abc'"},
		{"spaces", "a b c", "'a b c'"},
		{"single quote", "a'b", `'a'\''b'`},
		{"multiple quotes", "'x'", `''\''x'\'''`},
		{"path", "/opt/beegfs/bin/index/gufi_dir2index", "'/opt/beegfs/bin/index/gufi_dir2index'"},
		{"sql literal", "name LIKE '.%'", `'name LIKE '\''.%'\'''`},
		{"shell metachars", "$(rm -rf /); &|<>", "'$(rm -rf /); &|<>'"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := shellQuoteArg(tc.in); got != tc.want {
				t.Errorf("shellQuoteArg(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestResolveSSHHost(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		addr     string
		wantHost string
		wantErr  bool
	}{
		{"empty is local", "", "", false},
		{"explicit local", "local", "", false},
		{"ssh host", "ssh:node01", "node01", false},
		{"ssh user@host", "ssh:user@node01", "user@node01", false},
		{"ssh dotted host", "ssh:node01.cluster.example", "node01.cluster.example", false},
		{"ssh empty host", "ssh:", "", true},
		{"ssh option injection", "ssh:-oProxyCommand=x", "", true},
		{"ssh host:port rejected", "ssh:node01:2222", "", true},
		{"unknown scheme", "foo:bar", "", true},
		{"bare host no scheme", "node01", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			host, err := resolveSSHHost(tc.addr)
			if (err != nil) != tc.wantErr {
				t.Fatalf("resolveSSHHost(%q) err = %v, wantErr %v", tc.addr, err, tc.wantErr)
			}
			if host != tc.wantHost {
				t.Errorf("resolveSSHHost(%q) host = %q, want %q", tc.addr, host, tc.wantHost)
			}
		})
	}
}

func TestWrapForRemote_LocalPassthrough(t *testing.T) {
	t.Parallel()
	for _, addr := range []string{"", "local"} {
		bin, args, err := WrapForRemote("/bin/gufi_dir2index", []string{"-n", "4", "/src", "/dst"}, addr)
		if err != nil {
			t.Fatalf("addr=%q: unexpected error: %v", addr, err)
		}
		if bin != "/bin/gufi_dir2index" {
			t.Errorf("addr=%q: bin = %q, want unchanged", addr, bin)
		}
		want := []string{"-n", "4", "/src", "/dst"}
		if len(args) != len(want) {
			t.Fatalf("addr=%q: args = %v, want %v", addr, args, want)
		}
		for i := range want {
			if args[i] != want[i] {
				t.Errorf("addr=%q: args[%d] = %q, want %q", addr, i, args[i], want[i])
			}
		}
	}
}

func TestWrapForRemote_SSH(t *testing.T) {
	t.Parallel()
	bin, args, err := WrapForRemote(
		"/opt/beegfs/bin/index/gufi_dir2index",
		[]string{"-n", "4", "--plugin", IndexPluginPath, "/mnt/beegfs", "/mnt/index"},
		"ssh:node01",
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bin != "ssh" {
		t.Fatalf("bin = %q, want ssh", bin)
	}

	joined := strings.Join(args, " ")
	for _, want := range []string{"BatchMode=yes", "ServerAliveInterval=5", "ServerAliveCountMax=2"} {
		if !strings.Contains(joined, want) {
			t.Errorf("missing transport option %q in %v", want, args)
		}
	}

	hostIdx := -1
	for i, a := range args {
		if a == "node01" {
			hostIdx = i
			break
		}
	}
	if hostIdx == -1 {
		t.Fatalf("host node01 not found in %v", args)
	}
	if args[hostIdx-1] != "--" {
		t.Errorf("expected -- immediately before host, got %q", args[hostIdx-1])
	}
	tail := args[hostIdx+1:]
	if len(tail) != 3 || tail[0] != "bash" || tail[1] != "-c" {
		t.Fatalf("tail after host = %v, want [bash -c <script>]", tail)
	}

	script := tail[2]
	for _, want := range []string{
		"exec ",
		shellQuoteArg("/opt/beegfs/bin/index/gufi_dir2index"),
		shellQuoteArg("--plugin"),
		shellQuoteArg(IndexPluginPath),
		shellQuoteArg("/mnt/beegfs"),
		shellQuoteArg("/mnt/index"),
	} {
		if !strings.Contains(script, want) {
			t.Errorf("script missing %q\nscript: %s", want, script)
		}
	}
}

func TestWrapForRemote_InvalidHost(t *testing.T) {
	t.Parallel()
	if _, _, err := WrapForRemote("/bin/x", nil, "ssh:-oProxyCommand=evil"); err == nil {
		t.Fatal("expected error for option-injection host, got nil")
	}
	if _, _, err := WrapForRemote("/bin/x", nil, "bogus:addr"); err == nil {
		t.Fatal("expected error for unknown scheme, got nil")
	}
}
