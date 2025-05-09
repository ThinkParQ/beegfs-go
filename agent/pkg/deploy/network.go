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
	AddInterfaces(ctx context.Context, add []manifest.Nic) error
	ModifyInterfaces(ctx context.Context, old []manifest.Nic, new []manifest.Nic) error
	DestroyInterfaces(ctx context.Context, remove []manifest.Nic) error
}

type IP struct {
}

func (i *IP) AddInterfaces(ctx context.Context, add []manifest.Nic) error {
	for _, nic := range add {
		output, err := exec.CommandContext(ctx, "ip", "addr", "show", "dev", nic.Name).Output()
		if err != nil {
			return fmt.Errorf("unable to query interface %s: %w", nic.Name, err)
		}
		if !strings.Contains(string(output), nic.Addr) {
			return fmt.Errorf("interface %s does not have expected address %s", nic.Name, nic.Addr)
		}
	}
	return nil
}

func (i *IP) ModifyInterfaces(ctx context.Context, old []manifest.Nic, new []manifest.Nic) error {
	return errors.New("not implemented")
}

func (i *IP) DestroyInterfaces(ctx context.Context, remove []manifest.Nic) error {
	return errors.New("not implemented")
}
