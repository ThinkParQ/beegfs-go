package deploy

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	"github.com/thinkparq/beegfs-go/agent/pkg/manifest"
)

type Networker interface {
	ApplyInterfaces(ctx context.Context, add []manifest.Nic) error
	DestroyInterfaces(ctx context.Context, remove []manifest.Nic) error
}

type IP struct {
}

func (i *IP) ApplyInterfaces(ctx context.Context, add []manifest.Nic) error {
	for _, nic := range add {
		if nic.Addr == "" {
			continue // no-op
		}
		output, err := exec.CommandContext(ctx, "ip", "addr", "show", "dev", nic.Name).Output()
		if err != nil {
			return fmt.Errorf("unable to verify IP %s is configured for interface %s: %w", nic.Addr, nic.Name, err)
		}
		if !strings.Contains(string(output), nic.Addr) {
			return fmt.Errorf("unable to apply IP %s to interface %s: configuring IPs is not supported yet", nic.Addr, nic.Name)
		}
	}
	return nil
}

func (i *IP) DestroyInterfaces(ctx context.Context, remove []manifest.Nic) error {
	return errors.New("not implemented")
}
