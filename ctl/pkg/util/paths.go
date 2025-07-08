package util

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/expr-lang/expr"
	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/common/filesystem"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
)

type PathInputType int

type FileInfo struct {
	Path  string    // Full file path
	Name  string    // Base name of the file
	Size  int64     // File size in bytes
	Mode  uint32    // raw mode bits from syscall.Stat_t (type + permissions)
	Perm  uint32    // just the permission bits (mode & 0777)
	Mtime time.Time // Modification time
	Atime time.Time // Access time
	Ctime time.Time // Change time
	Uid   uint32    // User ID
	Gid   uint32    // Group ID
}

var (
	octRe    = regexp.MustCompile(`\b0?[0-7]{3}\b`)
	timeRe   = regexp.MustCompile(`\b(?i)(mtime|atime|ctime)\s*(<=|>=|<|>)\s*([0-9]+(?:\.[0-9]+)?[smhdMyw]+)\b`)
	sizeRe   = regexp.MustCompile(`\b(?i)(size)\s*(<=|>=|<|>|!=|=)\s*([0-9]+(?:\.[0-9]+)?(?:B|KB|MB|GB|TB|KiB|MiB|GiB|TiB))\b`)
	globRe   = regexp.MustCompile(`\b(?i)(name|path)\s*=~\s*"([^"\*\?]*[\*\?][^"\*\?]*)"`)
	regexRe  = regexp.MustCompile(`\b(?i)(name|path)\s*=~\s*"([^"\*\?][^"\*\?]*)"`)
	identRe  = regexp.MustCompile(`\b(?i)(mtime|atime|ctime|size|name|uid|gid|path|mode|perm)\b`)
	fieldMap = map[string]string{
		"mtime": "Mtime", "atime": "Atime", "ctime": "Ctime",
		"size": "Size", "name": "Name", "uid": "Uid", "gid": "Gid",
		"path": "Path", "mode": "Mode", "perm": "Perm",
	}
	unitFactors = map[string]float64{
		"B":  1,
		"KB": 1e3, "MB": 1e6, "GB": 1e9, "TB": 1e12,
		"KiB": 1 << 10, "MiB": 1 << 20, "GiB": 1 << 30, "TiB": 1 << 40,
	}
)

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
	opts ...ProcessPathOpt,
) (<-chan ResultT, <-chan error, error) {

	args := &ProcessPathOpts{}
	for _, opt := range opts {
		opt(args)
	}

	var resultsChan <-chan ResultT
	// Largely arbitrary channel size selection. There are multiple writers to this channel, but the
	// number varies based on GOMAXPROCS.
	errChan := make(chan error, 128)

	if method.pathsViaStdin {
		pathsChan := make(chan string, 1024)
		go ReadFromStdin(ctx, method.stdinDelimiter, pathsChan, errChan)
		resultsChan = startProcessing(ctx, pathsChan, errChan, singleWorker, args.FilterExpr, processEntry, true)
	} else if method.pathsViaRecursion != "" {
		pathsChan, err := walkDir(ctx, method.pathsViaRecursion, errChan, args.RecurseLexicographically)
		if err != nil {
			return nil, nil, err
		}
		resultsChan = startProcessing(ctx, pathsChan, errChan, singleWorker, args.FilterExpr, processEntry, false)
	} else {
		pathsChan := make(chan string, 1024)
		resultsChan = startProcessing(ctx, pathsChan, errChan, singleWorker, args.FilterExpr, processEntry, true)
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
	filterExpr string,
	processEntry func(path string) (ResultT, error),
	// Some processing methods do not work when BeeGFS is not mounted. Specifically recursion is not
	// possible since there is no file system tree mounted to recurse through.
	allowUnmounted bool,
) <-chan ResultT {

	results := make(chan ResultT, 1024)
	var beegfsClient filesystem.Provider
	var err error

	var filterFunc func(FileInfo) (bool, error)
	if filterExpr != "" {
		var err error
		filterFunc, err = compileFilter(filterExpr)
		if err != nil {
			// report error and return early
			errs <- fmt.Errorf("invalid filter %q: %w", filterExpr, err)
			close(results)
			return results
		}
	}

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

					keep := true
					if filterFunc != nil {
						info, err := beegfsClient.Lstat(searchPath)
						if err != nil {
							errs <- err
							workerCancel()
							return
						}
						statT, ok := info.Sys().(*syscall.Stat_t)
						if !ok {
							errs <- errors.New("unsupported platform…")
							workerCancel()
							return
						}
						fi := statToFileInfo(path, statT)

						keep, err = filterFunc(fi)
						if err != nil {
							errs <- fmt.Errorf("filter eval %q on %s: %w", filterExpr, path, err)
							workerCancel()
							return
						}
					}

					if keep {
						result, err := processEntry(searchPath)
						if err != nil {
							errs <- err
							workerCancel()
							return
						}
						results <- result
					}
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

func ProcessPathsViaPipeline[ResultT any](
	ctx context.Context,
	method PathInputMethod,
	singleWorker bool,
	processEntryPipeline func(context.Context, <-chan string, chan<- error) <-chan ResultT,
	opts ...ProcessPathOpt,
) (<-chan ResultT, <-chan error, error) {

	args := &ProcessPathOpts{}
	for _, opt := range opts {
		opt(args)
	}

	var resultChan <-chan ResultT
	// Largely arbitrary channel size selection. There are multiple writers to this channel, but the
	// number varies based on GOMAXPROCS.
	errsChan := make(chan error, 128)

	if method.pathsViaStdin {
		pathsChan := make(chan string, 1024)
		go ReadFromStdin(ctx, method.stdinDelimiter, pathsChan, errsChan)
		resultChan = startProcessingViaPipeline(ctx, pathsChan, errsChan, singleWorker, args.FilterExpr, processEntryPipeline, true)
	} else if method.pathsViaRecursion != "" {
		pathsChan, err := walkDir(ctx, method.pathsViaRecursion, errsChan, args.RecurseLexicographically)
		if err != nil {
			close(errsChan)
			return nil, nil, err
		}
		resultChan = startProcessingViaPipeline(ctx, pathsChan, errsChan, singleWorker, args.FilterExpr, processEntryPipeline, false)
	} else {
		pathsChan := make(chan string, 1024)
		resultChan = startProcessingViaPipeline(ctx, pathsChan, errsChan, singleWorker, args.FilterExpr, processEntryPipeline, true)
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
	return resultChan, errsChan, nil

}

func startProcessingViaPipeline[ResultT any](
	ctx context.Context,
	pathChan <-chan string,
	errChan chan<- error,
	singleWorker bool,
	filterExpr string,
	processEntryPipeline func(context.Context, <-chan string, chan<- error) <-chan ResultT,
	// Some processing methods do not work when BeeGFS is not mounted. Specifically recursion is not
	// possible since there is no file system tree mounted to recurse through.
	allowUnmounted bool,
) <-chan ResultT {

	var beegfsClient filesystem.Provider
	var err error

	var filterFunc func(FileInfo) (bool, error)
	if filterExpr != "" {
		var err error
		filterFunc, err = compileFilter(filterExpr)
		if err != nil {
			errChan <- fmt.Errorf("invalid filter %q: %w", filterExpr, err)
			close(errChan)
			return nil
		}
	}

	workerCtx, workerCancel := context.WithCancel(context.Background())
	searchPathChan := make(chan string, 1024)
	resultChan := processEntryPipeline(workerCtx, searchPathChan, errChan)

	// Spawn a goroutine that manages one or more workers that handle processing updates to each path.
	go func() {
		defer close(errChan)
		defer close(searchPathChan)

		// Because multiple workers may write to this channel it is closed by the parent goroutine
		// once all workers return.
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

		// The run loop for each worker:
		runWorker := func() {
			defer wg.Done()
			for {
				select {
				case <-workerCtx.Done():
					return
				case path, ok := <-pathChan:
					if !ok {
						return
					}
					// Automatically initialize the BeeGFS client with the first path. If this is
					// ever moved, be aware some processEntry functions depend on having a properly
					// initialized BeeGFSClient using the absolute path, and may need to be updated.
					if beegfsClient == nil {
						beegfsClient, err = config.BeeGFSClient(path)
						if err != nil && !(errors.Is(err, filesystem.ErrUnmounted) && allowUnmounted) {
							errChan <- err
							workerCancel()
							return
						}
					}
					searchPath, err := beegfsClient.GetRelativePathWithinMount(path)
					if err != nil {
						errChan <- err
						workerCancel()
						return
					}

					keep := true
					if filterFunc != nil {
						info, err := beegfsClient.Lstat(searchPath)
						if err != nil {
							errChan <- err
							workerCancel()
							return
						}
						statT, ok := info.Sys().(*syscall.Stat_t)
						if !ok {
							errChan <- errors.New("unsupported platform…")
							workerCancel()
							return
						}
						fi := statToFileInfo(path, statT)

						keep, err = filterFunc(fi)
						if err != nil {
							errChan <- fmt.Errorf("filter eval %q on %s: %w", filterExpr, path, err)
							workerCancel()
							return
						}
					}

					if keep {
						searchPathChan <- searchPath
					}
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
	return resultChan
}

// Asynchronously walks a directory from startingPath, immediately returning a channel where the
// paths will be sent, or an error if anything goes wrong setting up.
func walkDir(ctx context.Context, startingPath string, errChan chan<- error, lexicographically bool) (<-chan string, error) {

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

		err := beegfsClient.WalkDir(startInMount, walkDirFunc, filesystem.Lexicographically(lexicographically))
		if err != nil {
			errChan <- err
			close(pathChan)
			return
		}
		close(pathChan)
	}()

	return pathChan, nil
}

// compileFilter turns a DSL expression into a filter function.
func compileFilter(query string) (func(FileInfo) (bool, error), error) {
	// Preprocess DSL (includes octal normalization now)
	q := preprocessDSL(query)

	prog, err := expr.Compile(q,
		expr.Env(FileInfo{}),
		expr.Function("ago", func(params ...any) (any, error) { return ago(params[0].(string)) }),
		expr.Function("bytes", func(params ...any) (any, error) { return parseBytes(params[0].(string)) }),
		expr.Function("glob", func(params ...any) (any, error) { return globMatch(params[0].(string), params[1].(string)) }),
		expr.Function("regex", func(params ...any) (any, error) { return regexMatch(params[0].(string), params[1].(string)) }),
		expr.Function("now", func(params ...any) (any, error) { return time.Now(), nil }),
	)
	if err != nil {
		return nil, err
	}

	return func(fi FileInfo) (bool, error) {
		out, err := expr.Run(prog, fi)
		if err != nil {
			return false, err
		}
		return out.(bool), nil
	}, nil
}

// preprocessDSL applies all DSL→Go rewrites, including octal normalization.
func preprocessDSL(q string) string {
	// octal to decimal
	q = octRe.ReplaceAllStringFunc(q, func(oct string) string {
		v, _ := strconv.ParseInt(oct, 8, 64)
		return strconv.FormatInt(v, 10)
	})
	// time shifts
	q = timeRe.ReplaceAllStringFunc(q, func(m string) string {
		parts := timeRe.FindStringSubmatch(m)
		f, op, val := strings.ToLower(parts[1]), parts[2], parts[3]
		if goF, ok := fieldMap[f]; ok {
			switch op {
			case ">":
				op = "<"
			case "<":
				op = ">"
			case ">=":
				op = "<="
			case "<=":
				op = ">="
			}
			return fmt.Sprintf("%s %s ago(%q)", goF, op, val)
		}
		return m
	})
	// size units
	q = sizeRe.ReplaceAllString(q, `$1 $2 bytes("$3")`)
	// globs and regex
	q = globRe.ReplaceAllString(q, `glob($1,"$2")`)
	q = regexRe.ReplaceAllString(q, `regex($1,"$2")`)
	// identifiers
	q = identRe.ReplaceAllStringFunc(q, func(s string) string {
		if goF, ok := fieldMap[strings.ToLower(s)]; ok {
			return goF
		}
		return s
	})
	return q
}

func statToFileInfo(path string, st *syscall.Stat_t) FileInfo {
	return FileInfo{
		Path:  path,
		Name:  filepath.Base(path),
		Size:  st.Size,
		Mode:  st.Mode,
		Perm:  st.Mode & uint32(os.ModePerm),
		Atime: time.Unix(st.Atim.Sec, st.Atim.Nsec),
		Mtime: time.Unix(st.Mtim.Sec, st.Mtim.Nsec),
		Ctime: time.Unix(st.Ctim.Sec, st.Ctim.Nsec),
		Uid:   st.Uid,
		Gid:   st.Gid,
	}
}

// ago returns time.Now() minus parsed duration.
func ago(durationStr string) (time.Time, error) {
	d, err := parseExtendedDuration(durationStr)
	if err != nil {
		return time.Time{}, err
	}
	return time.Now().Add(-d), nil
}

// parseExtendedDuration supports standard and custom units (d, M, y).
func parseExtendedDuration(s string) (time.Duration, error) {
	// fast path for Go durations
	sfx := s[len(s)-1]
	if strings.IndexByte("nsmh", sfx) != -1 {
		return time.ParseDuration(s)
	}
	var factor time.Duration
	num, unit := s[:len(s)-1], s[len(s)-1:]
	switch unit {
	case "d":
		factor = 24 * time.Hour
	case "M":
		factor = 30 * 24 * time.Hour
	case "y":
		factor = 365 * 24 * time.Hour
	default:
		return time.ParseDuration(s)
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q: %w", s, err)
	}
	return time.Duration(f * float64(factor)), nil
}

// parseBytes converts size strings into byte counts.
func parseBytes(sizeStr string) (int64, error) {
	i := len(sizeStr)
	for i > 0 && (sizeStr[i-1] < '0' || sizeStr[i-1] > '9') {
		i--
	}
	num, unit := sizeStr[:i], strings.TrimSpace(sizeStr[i:])
	if unit == "" {
		unit = "B"
	}
	mul, ok := unitFactors[unit]
	if !ok {
		return 0, fmt.Errorf("unknown size unit %q", unit)
	}
	f, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q: %w", sizeStr, err)
	}
	return int64(f * mul), nil
}

// globMatch uses filepath.Match
func globMatch(s, pattern string) (bool, error) {
	return filepath.Match(pattern, s)
}

// regexMatch uses precompiled regex
func regexMatch(s, pattern string) (bool, error) {
	return regexp.MatchString(pattern, s)
}
