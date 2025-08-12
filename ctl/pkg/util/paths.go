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
	"github.com/thinkparq/beegfs-go/common/types"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"golang.org/x/sync/errgroup"
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
	var filter FileInfoFilter
	if args.FilterExpr != "" {
		filter, err = compileFilter(args.FilterExpr)
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

func walkStdin(ctx context.Context, delimiter byte, paths chan<- string, filter FileInfoFilter) error {
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

func walkList(ctx context.Context, pathList []string, paths chan<- string, filter FileInfoFilter) error {
	var err error
	var beegfsClient filesystem.Provider
	for _, path := range pathList {
		if beegfsClient, err = pushFilterInMountPath(ctx, path, filter, beegfsClient, paths); err != nil {
			return err
		}
	}

	return nil
}

func walkDir(ctx context.Context, startPath string, paths chan<- string, filter FileInfoFilter, lexicographically bool) error {
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

func pushFilterInMountPath(ctx context.Context, path string, filter FileInfoFilter, client filesystem.Provider, paths chan<- string) (filesystem.Provider, error) {
	if client == nil {
		var err error
		if client, err = config.BeeGFSClient(path); err != nil {
			if !errors.Is(err, filesystem.ErrUnmounted) {
				return nil, err
			}
		}
	}
	if filter != nil {
		info, err := client.Lstat(path)
		if err != nil {
			return client, fmt.Errorf("unable to filter files: %w", err)
		}
		statT, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return client, fmt.Errorf("unable to retrieve stat information: unsupported platform")
		}
		keep, err := filter(statToFileInfo(path, statT))
		if err != nil {
			return client, fmt.Errorf("unable to apply filter: %w", err)
		}
		if !keep {
			return client, nil
		}
	}

	inMountPath, err := client.GetRelativePathWithinMount(path)
	if err != nil {
		return client, err
	}

	select {
	case <-ctx.Done():
		return client, ctx.Err()
	case paths <- inMountPath:
	}

	return client, nil
}

const FilterFilesHelp = "Filter files by expression: fields(mtime/atime/ctime durations[s,m,h,d,M,y], size bytes[B,KB,MiB,GiB], uid, gid, mode, perm, name/path glob|regex); operators(=,!=,<,>,<=,>=,=~); logic(and|or|not); e.g. \"mtime > 365d and uid == 1000\""

type FileInfoFilter func(FileInfo) (bool, error)

// compileFilter turns a DSL expression into a filter function.
func compileFilter(query string) (FileInfoFilter, error) {
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
			return false, fmt.Errorf("filter eval %q on %s: %w", query, fi.Path, err)
		}
		result, ok := out.(bool)
		if !ok {
			return false, fmt.Errorf("filter expression resulted in a non-boolean value of type %T. Make sure your filter is a valid comparison (e.g., 'size>100MB')", out)
		}

		return result, nil
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
