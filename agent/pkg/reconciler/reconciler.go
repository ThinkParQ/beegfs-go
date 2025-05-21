package reconciler

import (
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/thinkparq/beegfs-go/agent/pkg/deploy"
	"github.com/thinkparq/beegfs-go/agent/pkg/manifest"
	"github.com/thinkparq/beegfs-go/common/beegfs"
	pb "github.com/thinkparq/protobuf/go/agent"
	"go.uber.org/zap"
)

type Config struct {
	ManifestPath       string   `mapstructure:"manifest-path"`
	ActiveManifestPath string   `mapstructure:"active-manifest-path"`
	DeploymentStrategy Strategy `mapstructure:"deployment-strategy"`
}

type Strategy string

const (
	DefaultStrategy Strategy = "default"
)

type Configurer interface {
	GetReconcilerConfig() Config
}

// Reconciler is responsible for comparing the current/active Filesystem with the new desired
// Filesystem and decides what needs to be created, updated, or destroyed.
type Reconciler interface {
	GetAgentID() string
	GetFsUUID() string
	Status() (ReconcileResult, error)
	Cancel(string) (ReconcileResult, error)
	UpdateConfiguration(any) error
	Stop() error
}

type ReconcileResult struct {
	Status *pb.Status
}

type defaultReconciler struct {
	agentID  string
	log      *zap.Logger
	mu       sync.Mutex
	active   manifest.Manifest
	state    state
	config   Config
	strategy deploy.Deployer
}

func New(ctx context.Context, agentID string, log *zap.Logger, config Config) (Reconciler, error) {
	log = log.With(zap.String("component", path.Base(reflect.TypeOf(defaultReconciler{}).PkgPath())))
	var deploymentStrategy deploy.Deployer
	var err error
	switch config.DeploymentStrategy {
	case DefaultStrategy:
		if deploymentStrategy, err = deploy.NewDefaultStrategy(ctx); err != nil {
			return nil, fmt.Errorf("unable to configure deployment strategy: %w", err)
		}
	default:
		return nil, fmt.Errorf("unknown deployment strategy: %v", config.DeploymentStrategy)
	}
	// Setting the initial config and file system manifest will be triggered later by ConfigMgr.
	return &defaultReconciler{
		agentID:  agentID,
		log:      log,
		state:    newAgentState(log),
		mu:       sync.Mutex{},
		strategy: deploymentStrategy,
	}, nil
}

func (r *defaultReconciler) Stop() error {
	r.log.Info("attempting to stop reconciler")
	r.state.cancel("agent is shutting down")
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.strategy.Cleanup()
}

func (r *defaultReconciler) GetAgentID() string {
	return r.agentID
}

func (r *defaultReconciler) GetFsUUID() string {
	return "TODO"
}

func (r *defaultReconciler) Status() (ReconcileResult, error) {
	return ReconcileResult{
		Status: r.state.get(),
	}, nil
}

func (r *defaultReconciler) Cancel(reason string) (ReconcileResult, error) {
	r.state.cancel(reason)
	return r.Status()
}

// UpdateConfiguration handles:
//
//   - Local config updates from ConfigMgr where the new manifest is loaded from disk.
//   - Remote config updates from the gRPC server where a new manifest is saved to disk.
//
// In both cases it will verify the new manifest and attempt to reconcile it if possible.
func (r *defaultReconciler) UpdateConfiguration(config any) error {
	if configurer, ok := config.(Configurer); ok {
		r.mu.Lock()
		r.config = configurer.GetReconcilerConfig()
		r.log.Info("loading file system manifest", zap.String("path", r.config.ManifestPath))
		newManifest, err := manifest.FromDisk(r.config.ManifestPath)
		r.mu.Unlock()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrLoadingManifest, err)
		}
		return r.verify(newManifest.Filesystems)
	} else if newManifest, ok := config.(manifest.Manifest); ok {
		r.mu.Lock()
		r.log.Info("saving file system manifest", zap.String("path", r.config.ActiveManifestPath))
		err := manifest.ToDisk(newManifest, r.config.ManifestPath)
		r.mu.Unlock()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrSavingManifest, err)
		}
		return r.verify(newManifest.Filesystems)
	}
	return fmt.Errorf("%w: received unexpected manifest (most likely this indicates a bug and a report should be filed)", ErrBadManifest)
}

// Verify performs any checks that can be done without actually reconciling the manifest. This
// allows a response to be returned quickly while the reconciliation happens in the background.
func (r *defaultReconciler) verify(newFilesystems manifest.Filesystems) error {
	r.log.Info("verifying manifest")
	if len(newFilesystems) == 0 {
		return errors.New("manifest does not contain any file systems")
	}

	// shortToFullUUIDs is used to ensure the short UUIDs derived from the full v4 FS UUID do not
	// have any collisions. While true collisions are HIGHLY unlikely from properly generated v4
	// UUIDs, this might happen if there are user generated UUIDs, typos, or copy/paste errors.
	shortToFullUUIDs := map[string]string{}
	for fsUUID, fs := range newFilesystems {
		fsUUID = strings.ToLower(fsUUID)
		u, err := uuid.Parse(fsUUID)
		if err != nil {
			return fmt.Errorf("error parsing file system UUID: %w (is it a valid v4 UUID?)", err)
		} else if u.Version() != 4 {
			return fmt.Errorf("unsupported file system UUID version: %d (must be v4)", u.Version())
		}
		shortUUID := manifest.ShortUUID(u)
		if conflictingUUID, ok := shortToFullUUIDs[shortUUID]; ok {
			return fmt.Errorf(
				"short UUID collision: %q derived from %q and %q (first %d characters are identical)",
				shortUUID, fsUUID, conflictingUUID, manifest.ShortUUIDLen,
			)
		}
		shortToFullUUIDs[shortUUID] = fsUUID
		// TODO:
		//  * Avoid necessary reconciliations by seeing if the manifest changed.
		//  * Validate we can migrate from currentFS to newFS.
		//  * Validate the FS config:
		//    * All services have IPs + targets.
		//    * Services have the correct number of targets (i.e., 1 for mgmtd meta, remote, sync).
		// Note these should be implemented as methods on manifest.Filesystem.
		if err := fs.InheritGlobalConfig(shortUUID, fsUUID); err != nil {
			return fmt.Errorf("error propagating global configuration: %w", err)
		}
	}
	go r.reconcile(newFilesystems)
	return nil
}

// Reconcile attempts to move the local state from the currentFS to the newFS.
func (r *defaultReconciler) reconcile(newFilesystems manifest.Filesystems) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log.Debug("reconciling", zap.Any("filesystem", newFilesystems))
	ctx := r.state.start()

	for fsUUID, fs := range newFilesystems {
		agent, ok := fs.Agents[r.agentID]
		if !ok {
			// Not all file systems in this manifest may have configuration for this agent. It is
			// also valid that this manifest has no services managed by this agent.
			r.log.Debug("file system has no services assigned to this agent", zap.String("fsUUID", fsUUID))
			continue
		}

		// Don't apply any common configuration if the agent doesn't have any services for this file system.
		if err := r.strategy.ApplySourceRepo(ctx, fs.Common.InstallSource); err != nil {
			r.state.fail(fmt.Sprintf("unable to apply source configuration for %s: %s", fsUUID, err.Error()))
			return
		}

		if err := r.strategy.ApplyInterfaces(ctx, agent.Interfaces); err != nil {
			r.state.fail(fmt.Sprintf("unable to apply global interface configuration for %s: %s", fsUUID, err.Error()))
			return
		}

		// Install packages for all services types managed by this agent:
		installPackages := map[beegfs.NodeType]struct{}{}
		for _, service := range agent.Services {
			if _, ok := installPackages[service.Type]; ok {
				continue
			}
			installPackages[service.Type] = struct{}{}
			if ref, ok := fs.Common.InstallSource.Refs[service.Type]; ok {
				if err := r.strategy.ApplyInstall(ctx, ref); err != nil {
					r.state.fail(fmt.Sprintf("unable to apply service installation for %s: %s", getFsServiceID(fsUUID, service.Type, service.ID), err.Error()))
				}
			}
		}

		// Roll out services:
		for _, service := range agent.Services {
			if err := r.strategy.ApplyInterfaces(ctx, service.Interfaces); err != nil {
				r.state.fail(fmt.Sprintf("unable to apply interface configuration for %s: %s", getFsServiceID(fsUUID, service.Type, service.ID), err.Error()))
				return
			}
			if err := r.strategy.ApplyTargets(ctx, service.Targets); err != nil {
				r.state.fail(fmt.Sprintf("unable to apply target configuration for %s: %s", getFsServiceID(fsUUID, service.Type, service.ID), err.Error()))
				return
			}
			if err := r.strategy.ApplyService(ctx, service); err != nil {
				r.state.fail(fmt.Sprintf("unable to apply service configuration for %s: %s", getFsServiceID(fsUUID, service.Type, service.ID), err.Error()))
				return
			}
		}
	}
	r.active = manifest.Manifest{
		Metadata: manifest.Metadata{
			Updated: time.Now(),
		},
		Filesystems: newFilesystems,
	}
	manifest.ToDisk(r.active, r.config.ActiveManifestPath)
	r.state.complete(pb.Status_SUCCESS)
}
