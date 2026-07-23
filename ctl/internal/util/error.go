package util

// Contains an actual error and extra information on how to exit the cmd app
type CtlError struct {
	inner    error
	exitCode CtlExitCode
}

type CtlExitCode int

const (
	Success        CtlExitCode = iota // 0
	GeneralError                      // 1
	PartialSuccess                    // 2

	// NodesUnreachable is returned by "node list --reachability-error" when at least one node is
	// unreachable. Its value (5) is fixed for backward compatibility with existing scripts; the
	// values 3 and 4 are intentionally left unused.
	NodesUnreachable CtlExitCode = 5
)

func (c CtlExitCode) String() string {
	switch c {
	case Success:
		return "Success"
	case GeneralError:
		return "General Error"
	case PartialSuccess:
		return "Partial Success"
	case NodesUnreachable:
		return "Nodes Unreachable"
	default:
		return "Unknown"
	}
}

// Wraps the given error together with the exit code - meant to be returned from a command to the
// caller on error. The app then exits with the given exit code.
func NewCtlError(err error, exitCode CtlExitCode) CtlError {
	return CtlError{inner: err, exitCode: exitCode}
}

func (err *CtlError) GetExitCode() int {
	return int(err.exitCode)
}

func (err CtlError) Error() string {
	return err.inner.Error()
}

// Unwrap exposes the wrapped error so errors.Is and errors.As can traverse into it.
func (err CtlError) Unwrap() error {
	return err.inner
}
