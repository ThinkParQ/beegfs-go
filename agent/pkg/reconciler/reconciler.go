package reconciler

import (
	"context"
	"errors"
	"fmt"
	"path"
	"reflect"
	"sync"

	"github.com/thinkparq/beegfs-go/agent/pkg/deploy"
	"github.com/thinkparq/beegfs-go/agent/pkg/manifest"
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
	agentID        string
	log            *zap.Logger
	mu             sync.Mutex
	activeManifest manifest.Filesystems
	state          state
	config         Config
	strategy       deploy.Deployer
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
		newFS, err := manifest.FromDisk(r.config.ManifestPath)
		r.mu.Unlock()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrLoadingManifest, err)
		}
		return r.verify(newFS)
	} else if newFS, ok := config.(manifest.Filesystems); ok {
		r.mu.Lock()
		r.log.Info("saving file system manifest", zap.String("path", r.config.ActiveManifestPath))
		err := manifest.ToDisk(newFS, r.config.ManifestPath)
		r.mu.Unlock()
		if err != nil {
			return fmt.Errorf("%w: %w", ErrBadManifest, err)
		}
		return r.verify(newFS)
	}
	return fmt.Errorf("received unexpected reconciler configuration (most likely this indicates a bug and a report should be filed)")
}

// Verify performs any checks that can be done without actually reconciling the manifest. This
// allows a response to be returned quickly while the reconciliation happens in the background.
func (r *defaultReconciler) verify(newManifest manifest.Filesystems) error {
	r.log.Info("verifying manifest")
	if len(newManifest) == 0 {
		return errors.New("manifest does not contain any file systems")
	}
	for fsUUID, fs := range newManifest {
		// TODO:
		//  * Avoid necessary reconciliations by seeing if the manifest changed.
		//  * Validate we can migrate from currentFS to newFS.
		//  * Validate the FS config:
		//    * All services have IPs + targets.
		//    * Services have the correct number of targets (i.e., 1 for mgmtd meta, remote, sync).
		// Note these should be implemented as methods on manifest.Filesystem.
		fs.InheritGlobalConfig(fsUUID)
	}
	go r.reconcile(newManifest)
	return nil
}

// Reconcile attempts to move the local state from the currentFS to the newFS.
func (r *defaultReconciler) reconcile(newManifest manifest.Filesystems) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.log.Debug("reconciling", zap.Any("filesystem", newManifest))
	ctx := r.state.start()

	for fsUUID, fs := range newManifest {
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

		for _, service := range agent.Services {
			if err := r.strategy.ApplyInterfaces(ctx, service.Interfaces); err != nil {
				r.state.fail(fmt.Sprintf("unable to apply interface configuration for %s: %s", getFsServiceID(fsUUID, service.Type, service.ID), err.Error()))
				return
			}
			if err := r.strategy.ApplyTargets(ctx, service.Targets); err != nil {
				r.state.fail(fmt.Sprintf("unable to apply target configuration for %s: %s", getFsServiceID(fsUUID, service.Type, service.ID), err.Error()))
				return
			}

			// Currently the source for the services should always be set by the user or inherited
			// automatically from the global configuration. This might change so avoid a panic.
			if service.InstallSource != nil {
				if err := r.strategy.ApplyInstall(ctx, *service.InstallSource); err != nil {
					r.state.fail(fmt.Sprintf("unable to apply source installation for %s: %s", getFsServiceID(fsUUID, service.Type, service.ID), err.Error()))
				}
			} else {
				r.log.Warn("service install source was unexpectedly nil (ignoring)", zap.String("fsUUID", fsUUID), zap.String("type", service.Type.String()), zap.Any("id", service.ID))
			}

			if err := r.strategy.ApplyService(ctx, service); err != nil {
				r.state.fail(fmt.Sprintf("unable to apply service configuration for %s: %s", getFsServiceID(fsUUID, service.Type, service.ID), err.Error()))
				return
			}
		}
	}
	r.activeManifest = newManifest
	manifest.ToDisk(r.activeManifest, r.config.ActiveManifestPath)
	r.state.complete(pb.Status_SUCCESS)
}
