package util

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
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
// sent to the channel. The errs channel is NOT closed since it is used to return errors both from
// the Goroutine responsible for providing the paths based on the PathInputMethod (for example
// errors reading from stdin), and the Goroutines executing the processEntry function. Thus callers
// should not wait for this channel to be closed indefinitely, and instead rely on the ResultT
// channel to determine when all entries have been processed.
func ProcessPaths[ResultT any](
	ctx context.Context,
	method PathInputMethod,
	singleWorker bool,
	processEntry func(path string) (ResultT, error),
) (<-chan ResultT, <-chan error, error) {

	var resultsChan <-chan ResultT
	// Largely arbitrary channel size selection. There are multiple writers to this channel, but the
	// number varies based on GOMAXPROCS.
	errChan := make(chan error, 128)

	if method.pathsViaStdin {
		pathsChan := make(chan string, 1024)
		go ReadFromStdin(ctx, method.stdinDelimiter, pathsChan, errChan)
		resultsChan = startProcessing(ctx, pathsChan, errChan, singleWorker, processEntry, true)
	} else if method.pathsViaRecursion != "" {
		pathsChan, err := walkDir(ctx, method.pathsViaRecursion, errChan)
		if err != nil {
			return nil, nil, err
		}
		resultsChan = startProcessing(ctx, pathsChan, errChan, singleWorker, processEntry, false)
	} else {
		pathsChan := make(chan string, 1024)
		resultsChan = startProcessing(ctx, pathsChan, errChan, singleWorker, processEntry, true)
		go func() {
			// Writing to the channel needs to happen in a separate Goroutine so entriesChan can be
			// returned immediately. Otherwise if the number of paths is larger than the entriesChan
			// the processFunc would eventually be blocked since nothing would be reading from
			// entriesChan.
			defer close(pathsChan)
			for _, e := range method.pathsViaList {
				select {
				case <-ctx.Done():
					return
				default:
					pathsChan <- e
				}
			}
		}()
	}
	return resultsChan, errChan, nil

}

// startProcessing executes processEntry() for each path sent to the paths channel.
func startProcessing[ResultT any](
	ctx context.Context,
	paths <-chan string,
	errs chan<- error,
	singleWorker bool,
	processEntry func(path string) (ResultT, error),
	// Some processing methods do not work when BeeGFS is not mounted. Specifically recursion is not
	// possible since there is no file system tree mounted to recurse through.
	allowUnmounted bool,
) <-chan ResultT {

	results := make(chan ResultT, 1024)
	var beegfsClient filesystem.Provider
	var err error

	// Spawn a goroutine that manages one or more workers that handle processing updates to each path.
	go func() {
		// Because multiple workers may write to this channel it is closed by the parent goroutine
		// once all workers return.
		defer close(results)
		numWorkers := 1
		if !singleWorker {
			numWorkers = viper.GetInt(config.NumWorkersKey)
			if numWorkers > 1 {
				// Reserve one core for when there is another Goroutine walking a directory or reading
				// in paths from stdin. If there is only a single core don't set the numWorkers less
				// than one.
				numWorkers = viper.GetInt(config.NumWorkersKey) - 1
			}
		}
		wg := sync.WaitGroup{}
		// If any of the workers encounter an error, this context is used to signal to the other
		// workers they should exit early.
		workerCtx, workerCancel := context.WithCancel(context.Background())

		// The run loop for each worker:
		runWorker := func() {
			defer wg.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case path, ok := <-paths:
					if !ok {
						return
					}
					// Automatically initialize the BeeGFS client with the first path. If this is
					// ever moved, be aware some processEntry functions depend on having a properly
					// initialized BeeGFSClient using the absolute path, and may need to be updated.
					if beegfsClient == nil {
						beegfsClient, err = config.BeeGFSClient(path)
						if err != nil && !(errors.Is(err, filesystem.ErrUnmounted) && allowUnmounted) {
							errs <- err
							workerCancel()
							return
						}
					}
					searchPath, err := beegfsClient.GetRelativePathWithinMount(path)
					if err != nil {
						errs <- err
						workerCancel()
						return
					}
					result, err := processEntry(searchPath)
					if err != nil {
						errs <- err
						workerCancel()
						return
					}
					results <- result
				case <-ctx.Done():
					return
				}
			}
		}
		for range numWorkers {
			wg.Add(1)
			go runWorker()
		}
		wg.Wait()
	}()
	return results
}

// Asynchronously walks a directory from startingPath, immediately returning a channel where the
// paths will be sent, or an error if anything goes wrong setting up.
func walkDir(ctx context.Context, startingPath string, errChan chan<- error) (<-chan string, error) {

	beegfsClient, err := config.BeeGFSClient(startingPath)
	if err != nil {
		return nil, err
	}

	startInMount, err := beegfsClient.GetRelativePathWithinMount(startingPath)
	if err != nil {
		return nil, err
	}

	pathChan := make(chan string, 1024)
	go func() {
		walkDirFunc := func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				pathChan <- path
			}
			return nil
		}

		err := beegfsClient.WalkDir(startInMount, walkDirFunc)
		if err != nil {
			errChan <- err
			close(pathChan)
			return
		}
		close(pathChan)
	}()

	return pathChan, nil
}
