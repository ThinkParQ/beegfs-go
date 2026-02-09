package index

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseIndexAddr(t *testing.T) {
	originalIndexConfig := indexConfig
	t.Cleanup(func() {
		indexConfig = originalIndexConfig
	})

	baseConfig := writeIndexConfig(t, "")
	defaultsConfig := writeIndexConfig(t, "Server=cfg-host\nPort=2200\n")

	tests := []struct {
		name    string
		input   string
		config  string
		want    indexBackend
		wantErr bool
	}{
		{
			name:   "empty defaults to local",
			input:  "",
			config: baseConfig,
			want: indexBackend{
				mode: indexAddrLocal,
			},
		},
		{
			name:   "local literal",
			input:  "local",
			config: baseConfig,
			want: indexBackend{
				mode: indexAddrLocal,
			},
		},
		{
			name:   "ssh host",
			input:  "ssh:host",
			config: baseConfig,
			want: indexBackend{
				mode: "ssh",
				host: "host",
			},
		},
		{
			name:   "ssh host with port",
			input:  "ssh:host:2222",
			config: baseConfig,
			want: indexBackend{
				mode: "ssh",
				host: "host",
				port: "2222",
			},
		},
		{
			name:   "ssh user and host",
			input:  "ssh:user@host",
			config: baseConfig,
			want: indexBackend{
				mode: "ssh",
				host: "user@host",
			},
		},
		{
			name:   "ssh user host and port",
			input:  "ssh:user@host:2022",
			config: baseConfig,
			want: indexBackend{
				mode: "ssh",
				host: "user@host",
				port: "2022",
			},
		},
		{
			name:   "trim whitespace",
			input:  "  ssh:host:2200  ",
			config: baseConfig,
			want: indexBackend{
				mode: "ssh",
				host: "host",
				port: "2200",
			},
		},
		{
			name:    "missing host",
			input:   "ssh:",
			config:  baseConfig,
			wantErr: true,
		},
		{
			name:   "missing host uses config defaults",
			input:  "ssh",
			config: defaultsConfig,
			want: indexBackend{
				mode: "ssh",
				host: "cfg-host",
				port: "2200",
			},
		},
		{
			name:   "missing port",
			input:  "ssh:host:",
			config: baseConfig,
			want: indexBackend{
				mode: "ssh",
				host: "host",
			},
		},
		{
			name:    "empty host with port",
			input:   "ssh::2200",
			config:  baseConfig,
			wantErr: true,
		},
		{
			name:    "invalid port",
			input:   "ssh:host:notaport",
			config:  baseConfig,
			wantErr: true,
		},
		{
			name:    "port out of range",
			input:   "ssh:host:99999",
			config:  baseConfig,
			wantErr: true,
		},
		{
			name:    "too many segments",
			input:   "ssh:host:22:33",
			config:  baseConfig,
			wantErr: true,
		},
		{
			name:    "invalid prefix",
			input:   "remote:host",
			config:  baseConfig,
			wantErr: true,
		},
		{
			name:    "host contains whitespace",
			input:   "ssh:ho st",
			config:  baseConfig,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.config != "" {
				indexConfig = tt.config
			}
			got, err := parseIndexAddr(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func writeIndexConfig(t *testing.T, contents string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "gufi-config")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
