package util

import (
	"bufio"
	"context"
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
// sent to the channel.
func ProcessPaths[ResultT any](
	ctx context.Context,
	method PathInputMethod,
	singleWorker bool,
	processEntry func(path string) (ResultT, error),
	opts ...ProcessPathOpt,
) (<-chan ResultT, <-chan error, error) {
	results, walkErrs, processErrs, err := processPaths(ctx, method, singleWorker, processEntry, nil, opts...)
	return results, CombineErrChans(walkErrs, processErrs), err
}

// ProcessPathsViaPipeline() processes one or more entries based on the PathInputMethod and executes the
// specified request for each entry by invoking the provided processEntryPipeline() function.
//
// It handles any setup needed to read from the specified PathInputMethod (such as reading from
// stdin) and by default processes entries in parallel based on the global num-workers flag. Because
// entries are processed in parallel, the order entries are processed and results are returned is
// not stable. If stable results are desired, for example when recursively walking a directory and
// printing entry info, use the singleWorker option.
//
// It returns a ResultT channel where the result for each entry will be sent. The ResultT channel
// will be closed once all entries are processed, or if any error occurs after all valid results are
// sent to the channel.
func ProcessPathsViaPipeline[ResultT any](
	ctx context.Context,
	method PathInputMethod,
	processEntryPipeline func(context.Context, <-chan string) <-chan ResultT,
	opts ...ProcessPathOpt,
) (<-chan ResultT, <-chan error, error) {
	results, walkErrs, _, err := processPaths(ctx, method, false, nil, processEntryPipeline, opts...)
	return results, walkErrs, err
}

type processPathsWalk func() (<-chan string, <-chan error)

func processPaths[ResultT any](
	ctx context.Context,
	method PathInputMethod,
	singleWorker bool,
	processEntryFunction func(path string) (ResultT, error),
	processEntryPipeline func(context.Context, <-chan string) <-chan ResultT,
	opts ...ProcessPathOpt,
) (<-chan ResultT, <-chan error, <-chan error, error) {
	if processEntryFunction != nil && processEntryPipeline != nil {
		return nil, nil, nil, fmt.Errorf("unable to start processing by both function and pipeline! This is a bug")
	}

	args := &ProcessPathOpts{}
	for _, opt := range opts {
		opt(args)
	}

	var walk processPathsWalk
	if method.pathsViaStdin {
		walk = func() (<-chan string, <-chan error) {
			return walkStdin(ctx, method.stdinDelimiter, args.FilterExpr)
		}
	} else if method.pathsViaRecursion != "" {
		walk = func() (<-chan string, <-chan error) {
			return walkDir(ctx, method.pathsViaRecursion, args.FilterExpr, args.RecurseLexicographically)
		}
	} else {
		walk = func() (<-chan string, <-chan error) {
			return walkList(ctx, method.pathsViaList, args.FilterExpr)
		}
	}

	var results <-chan ResultT
	var walkErrs <-chan error
	var processErrs <-chan error
	if processEntryPipeline != nil {
		results, walkErrs = startProcessingViaPipeline(ctx, walk, processEntryPipeline)
	} else if processEntryFunction != nil {
		numWorkers := 1
		if !singleWorker {
			numWorkers = viper.GetInt(config.NumWorkersKey)
			if numWorkers > 1 {
				// Reserve one core for when there is another Goroutine walking a directory or reading
				// in paths from stdin. If there is only a single core don't set the numWorkers less
				// than one.
				numWorkers -= 1
			}
		}
		results, walkErrs, processErrs = startProcessingViaFunction(ctx, walk, processEntryFunction, numWorkers)
	} else {
		return nil, nil, nil, fmt.Errorf("unable to start processing without either a function or pipeline! This is a bug")
	}
	return results, walkErrs, processErrs, nil
}

func startProcessingViaFunction[ResultT any](
	ctx context.Context,
	walk processPathsWalk,
	processEntry func(path string) (ResultT, error),
	numWorkers int,
) (<-chan ResultT, <-chan error, <-chan error) {
	paths, walkErrs := walk()
	results := make(chan ResultT, 1024)
	processErrs := make(chan error, 128)
	go func() {
		defer close(results)
		defer close(processErrs)

		wg := sync.WaitGroup{}
		for range numWorkers {
			wg.Add(1)
			go func() {
				defer wg.Done()

				for {
					select {
					case <-ctx.Done():
						return
					case path, ok := <-paths:
						if !ok {
							return
						}
						result, err := processEntry(path)
						if err != nil {
							processErrs <- err
							return
						}
						results <- result
					}
				}
			}()
		}
		wg.Wait()
	}()

	return results, walkErrs, processErrs
}

func startProcessingViaPipeline[ResultT any](
	ctx context.Context,
	walk processPathsWalk,
	processEntryPipeline func(context.Context, <-chan string) <-chan ResultT,
) (<-chan ResultT, <-chan error) {
	paths, walkErrs := walk()
	results := processEntryPipeline(ctx, paths)
	return results, walkErrs
}

func walkStdin(ctx context.Context, delimiter byte, filterExpr string) (<-chan string, <-chan error) {
	paths := make(chan string, 1024)
	errs := make(chan error, 128)
	go func() {
		defer close(paths)
		defer close(errs)

		var err error
		var filter func(FileInfo) (bool, error)
		if filterExpr != "" {
			filter, err = compileFilter(filterExpr)
			if err != nil {
				errs <- fmt.Errorf("invalid filter %q: %w", filterExpr, err)
				return
			}
		}

		scanner := bufio.NewScanner(os.Stdin)
		splitFunc := func(data []byte, atEOF bool) (int, []byte, error) {
			for i := 0; i < len(data); i++ {
				if data[i] == delimiter {
					return i + 1, data[:i], nil
				}
			}
			// When EOF is reached, return what is left
			if atEOF && len(data) != 0 {
				return len(data), data, nil
			}
			// Otherwise request more data
			return 0, nil, nil
		}
		scanner.Split(splitFunc)

		var beegfsClient filesystem.Provider
		for scanner.Scan() {
			path := scanner.Text()
			if err := scanner.Err(); err != nil {
				errs <- err
				return
			}

			select {
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			default:
				keep := true
				if filter != nil {
					info, err := os.Lstat(path)
					if err != nil {
						errs <- err
					}
					statT, ok := info.Sys().(*syscall.Stat_t)
					if !ok {
						errs <- fmt.Errorf("unsupported platform…")

					}
					keep, err = filter(statToFileInfo(path, statT))
					if err != nil {
						errs <- err

					}
				}

				if keep {
					if beegfsClient == nil {
						beegfsClient, err = config.BeeGFSClient(path)
						if err != nil {
							errs <- err
						}
					}

					inMountPath, err := beegfsClient.GetRelativePathWithinMount(path)
					if err != nil {
						errs <- err
					}
					paths <- inMountPath
				}
			}

		}
	}()

	return paths, errs
}

func walkList(ctx context.Context, pathList []string, filterExpr string) (<-chan string, <-chan error) {
	paths := make(chan string, 1024)
	errs := make(chan error, 128)
	go func() {
		defer close(paths)
		defer close(errs)

		var err error
		var filter func(FileInfo) (bool, error)
		if filterExpr != "" {
			filter, err = compileFilter(filterExpr)
			if err != nil {
				errs <- fmt.Errorf("invalid filter %q: %w", filterExpr, err)
				return
			}
		}

		var beegfsClient filesystem.Provider
		for _, path := range pathList {
			select {
			case <-ctx.Done():
				errs <- ctx.Err()
				return
			default:
				keep := true
				if filter != nil {
					info, err := os.Lstat(path)
					if err != nil {
						errs <- err
					}
					statT, ok := info.Sys().(*syscall.Stat_t)
					if !ok {
						errs <- fmt.Errorf("unsupported platform…")

					}
					keep, err = filter(statToFileInfo(path, statT))
					if err != nil {
						errs <- err

					}
				}

				if keep {
					if beegfsClient == nil {
						beegfsClient, err = config.BeeGFSClient(path)
						if err != nil {
							errs <- err
						}
					}

					inMountPath, err := beegfsClient.GetRelativePathWithinMount(path)
					if err != nil {
						errs <- err
					}
					paths <- inMountPath
				}
			}
		}
	}()

	return paths, errs
}

func walkDir(ctx context.Context, startPath string, filterExpr string, lexicographically bool) (<-chan string, <-chan error) {
	paths := make(chan string, 1024)
	errs := make(chan error, 128)
	go func() {
		defer close(paths)
		defer close(errs)

		var err error
		var filter func(FileInfo) (bool, error)
		if filterExpr != "" {
			filter, err = compileFilter(filterExpr)
			if err != nil {
				errs <- fmt.Errorf("invalid filter %q: %w", filterExpr, err)
				return
			}
		}

		beegfsClient, err := config.BeeGFSClient(startPath)
		if err != nil {
			errs <- err
		}
		startingInMountPath, err := beegfsClient.GetRelativePathWithinMount(startPath)
		if err != nil {
			errs <- err
		}

		walkDirFunc := func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			default:

				keep := true
				if filter != nil {
					info, err := os.Lstat(path)
					if err != nil {
						return err
					}
					statT, ok := info.Sys().(*syscall.Stat_t)
					if !ok {
						return fmt.Errorf("unsupported platform…")

					}
					keep, err = filter(statToFileInfo(path, statT))
					if err != nil {
						return err

					}
				}
				if keep {
					inMountPath, err := beegfsClient.GetRelativePathWithinMount(path)
					if err != nil {
						errs <- err
					}

					paths <- inMountPath
				}
			}
			return nil
		}

		err = beegfsClient.WalkDir(startingInMountPath, walkDirFunc, filesystem.Lexicographically(lexicographically))
		if err != nil {
			errs <- err
		}
	}()
	return paths, errs
}

const FilterFilesHelp = "Filter files by expression: fields(mtime/atime/ctime durations[s,m,h,d,M,y], size bytes[B,KB,MiB,GiB], uid, gid, mode, perm, name/path glob|regex); operators(=,!=,<,>,<=,>=,=~); logic(and|or|not); e.g. \"mtime > 365d and uid == 1000\""

type FileInfoFilter func(FileInfo) (bool, error)

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
			return false, fmt.Errorf("filter eval %q on %s: %w", query, fi.Path, err)
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
