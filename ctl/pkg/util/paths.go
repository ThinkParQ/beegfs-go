package util

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/common/types"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"golang.org/x/sync/errgroup"
)

type PathInputType int

const (
	PathInputInvalid PathInputType = iota
	PathInputStdin
	PathInputRecursion
	PathInputList
)

func (t PathInputType) String() string {
	switch t {
	case PathInputStdin:
		return "stdin"
	case PathInputRecursion:
		return "recursion"
	case PathInputList:
		return "list"
	default:
		return "unknown"
	}
}

// PathInputMethod is used to configure how paths are provided to ProcessPaths(). It must be
// initialized using DeterminePathInputMethod() before first use.
type PathInputMethod struct {
	// The stdin mechanism allows the caller to provide multiple paths over a channel. This allows
	// paths to be sent to processFunc() while simultaneously returning results for each path. This
	// is useful when processFunc() is used as part of a larger pipeline. The caller should close
	// the channel once all paths have been provided.
	pathsViaStdin  bool
	stdinDelimiter byte
	// Provide a single path to trigger walking a directory tree and updating all sub-entries.
	pathsViaRecursion string
	// Specify one or more paths.
	pathsViaList []string
	inputType    PathInputType
}

func (m PathInputMethod) Get() PathInputType {
	return m.inputType
}

// DeterminePathInputMethod() processes user configuration to determine how paths are provided to
// processEntries(). If a single path "-" is provided, then paths are read from stdin. If a single
// path and the recurse flag is set, then input paths are determined by recursively walking the
// path. Otherwise one or more paths can be specified directly. The stdinDelimiter must be provided
// if reading from stdin is allowed.
func DeterminePathInputMethod(paths []string, recurse bool, stdinDelimiter string) (PathInputMethod, error) {
	pm := PathInputMethod{}
	if pathsLen := len(paths); pathsLen == 0 {
		return pm, fmt.Errorf("nothing to process (no paths were specified)")
	} else if pathsLen == 1 {
		if paths[0] == "-" {
			var err error
			pm.pathsViaStdin = true
			pm.inputType = PathInputStdin
			pm.stdinDelimiter, err = GetStdinDelimiterFromString(stdinDelimiter)
			if err != nil {
				return pm, err
			}
		} else if recurse {
			pm.pathsViaRecursion = paths[0]
			pm.inputType = PathInputRecursion
		} else {
			pm.pathsViaList = paths
			pm.inputType = PathInputList
		}
	} else {
		if recurse {
			return pm, fmt.Errorf("only one path can be specified with the recurse option")
		}
		pm.pathsViaList = paths
		pm.inputType = PathInputList
	}
	return pm, nil
}

// The PathInputMethod is usually determined by the frontend then the backend calls ProcessPaths.
// ProcessPathOpts contains any settings that should always be determined by the backend.
type ProcessPathOpts struct {
	RecurseLexicographically bool
	FilterExpr               string
}

type ProcessPathOpt func(*ProcessPathOpts)

func RecurseLexicographically(l bool) ProcessPathOpt {
	return func(args *ProcessPathOpts) {
		args.RecurseLexicographically = l
	}
}

func FilterExpr(f string) ProcessPathOpt {
	return func(args *ProcessPathOpts) {
		args.FilterExpr = f
	}
}

// ProcessPaths() processes one or more entries based on the PathInputMethod and executes the
// specified request for each entry by invoking the provided processEntry() function.
//
// It handles any setup needed to read from the specified PathInputMethod (such as reading from
// stdin) and by default processes entries in parallel based on the global num-workers flag. Because
// entries are processed in parallel, the order entries are processed and results are returned is
// not stable. If stable results are desired, for example when recursively walking a directory and
// printing entry info, use the singleWorker option.
//
// It returns a ResultT channel where the result for each entry will be sent. The ResultT channel
// will be closed once all entries are processed, or if any error occurs after all valid results are
// sent to the channel. When any walker or worker returns an error, the shared context is cancelled;
// in-flight calls to processEntry are allowed to finish, but no new work is started.
func ProcessPaths[ResultT any](
	ctx context.Context,
	method PathInputMethod,
	singleWorker bool,
	processEntry func(path string) (ResultT, error),
	opts ...ProcessPathOpt,
) (<-chan ResultT, func() error, error) {

	numWorkers := 1
	if !singleWorker {
		// Reserve one core for another Goroutine walking paths. If there is only a single core
		// don't set the numWorkers less than one.
		numWorkers = max(viper.GetInt(config.NumWorkersKey)-1, 1)
	}

	pathsGroup, pathsGroupCtx := errgroup.WithContext(ctx)
	paths := make(chan string, numWorkers*4)
	pathsGroup.Go(func() error {
		defer close(paths)
		return StreamPaths(pathsGroupCtx, method, paths, opts...)
	})

	processGroup, processGroupCtx := errgroup.WithContext(ctx)
	results := make(chan ResultT, numWorkers*4)
	processGroup.Go(func() (err error) {
		defer close(results)
		defer func() {
			err = WaitForLastStage(pathsGroup.Wait, err, paths)
		}()
		err = startProcessing(processGroupCtx, paths, results, processEntry, numWorkers)
		return err
	})

	return results, processGroup.Wait, nil
}

// StreamPaths reads file paths from the given PathInputMethod—either stdin, directory,
// or an explicit list—applies an optional filter expression, and streams each matching path to out.
// It closes the out channel when done, respects ctx cancellation, and returns an error if any
// step fails.
func StreamPaths(ctx context.Context, method PathInputMethod, out chan<- string, opts ...ProcessPathOpt) error {
	args := &ProcessPathOpts{}
	for _, opt := range opts {
		opt(args)
	}

	var err error
	var filter filesystem.FileInfoFilter
	if args.FilterExpr != "" {
		filter, err = filesystem.CompileFilter(args.FilterExpr)
		if err != nil {
			return fmt.Errorf("invalid filter %q: %w", args.FilterExpr, err)
		}
	}

	if method.pathsViaStdin {
		return walkStdin(ctx, method.stdinDelimiter, out, filter)
	} else if method.pathsViaRecursion != "" {
		return walkDir(ctx, method.pathsViaRecursion, out, filter, args.RecurseLexicographically)
	}
	return walkList(ctx, method.pathsViaList, out, filter)
}

func startProcessing[ResultT any](
	ctx context.Context,
	paths <-chan string,
	results chan<- ResultT,
	processEntry func(path string) (ResultT, error),
	numWorkers int,
) error {

	g, gCtx := errgroup.WithContext(ctx)
	for range numWorkers {
		g.Go(func() error {
			for {
				select {
				case <-gCtx.Done():
					return gCtx.Err()
				case path, ok := <-paths:
					if !ok {
						return nil
					}
					result, err := processEntry(path)
					if err != nil {
						return err
					}

					select {
					case <-gCtx.Done():
						return gCtx.Err()
					case results <- result:
					}
				}
			}
		})
	}

	return g.Wait()
}

func walkStdin(ctx context.Context, delimiter byte, paths chan<- string, filter filesystem.FileInfoFilter) error {
	scanner := GetWalkStdinScanner(delimiter)
	var err error
	var beegfsClient filesystem.Provider
	for scanner.Scan() {
		path := scanner.Text()
		if beegfsClient, err = pushFilterInMountPath(ctx, path, filter, beegfsClient, paths); err != nil {
			return err
		}
	}

	return scanner.Err()
}

func walkList(ctx context.Context, pathList []string, paths chan<- string, filter filesystem.FileInfoFilter) error {
	var err error
	var beegfsClient filesystem.Provider
	for _, path := range pathList {
		if beegfsClient, err = pushFilterInMountPath(ctx, path, filter, beegfsClient, paths); err != nil {
			return err
		}
	}

	return nil
}

func walkDir(ctx context.Context, startPath string, paths chan<- string, filter filesystem.FileInfoFilter, lexicographically bool) error {
	beegfsClient, err := config.BeeGFSClient(startPath)
	if err != nil {
		return fmt.Errorf("unable to recursively walk directory: %w", err)
	}
	startingInMountPath, err := beegfsClient.GetRelativePathWithinMount(startPath)
	if err != nil {
		return fmt.Errorf("unable to recursively walk directory: %w", err)
	}

	walkDirFunc := func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("unable to recursively walk directory: %w", err)
		}
		if _, err := pushFilterInMountPath(ctx, path, filter, beegfsClient, paths); err != nil {
			return fmt.Errorf("unable to recursively walk directory: %w", err)
		}
		return nil
	}

	err = beegfsClient.WalkDir(startingInMountPath, walkDirFunc, filesystem.Lexicographically(lexicographically))
	if err != nil {
		return fmt.Errorf("unable to recursively walk directory: %w", err)
	}
	return nil
}

func pushFilterInMountPath(ctx context.Context, path string, filter filesystem.FileInfoFilter, client filesystem.Provider, paths chan<- string) (filesystem.Provider, error) {
	if client == nil {
		var err error
		if client, err = config.BeeGFSClient(path); err != nil {
			if !errors.Is(err, filesystem.ErrUnmounted) {
				return nil, err
			}
		}
	}

	inMountPath, err := client.GetRelativePathWithinMount(path)
	if err != nil {
		return client, err
	}

	if keep, err := filesystem.ApplyFilter(inMountPath, filter, client); err != nil {
		return client, err
	} else if !keep {
		return client, nil
	}

	select {
	case <-ctx.Done():
		return client, ctx.Err()
	case paths <- inMountPath:
	}

	return client, nil
}

// WaitForLastStage drains any provided channels and then blocks on wait(). Any error that's
// returned from the wait function is prepended to err and returned. This lets you cancel a
// downstream pipeline stage without preventing upstream stages that may still be active.
func WaitForLastStage[T any](wait func() error, err error, chans ...<-chan T) error {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func() {
			defer wg.Done()
			for range ch {
			}
		}()
	}
	wg.Wait()

	previousErr := wait()
	multiErr := types.MultiError{}
	if previousErr != nil {
		multiErr.Errors = append(multiErr.Errors, previousErr)
	}
	if err != nil {
		multiErr.Errors = append(multiErr.Errors, err)
	}
	if len(multiErr.Errors) > 0 {
		return &multiErr
	}
	return nil
}
