package util

// Contains an actual error and extra information on how to exit the cmd app
type CtlError struct {
	inner    error
	exitCode CtlExitCode
}

type CtlExitCode int

const (
	Success CtlExitCode = iota
	GeneralError
	PartialSuccess
)

func (c CtlExitCode) String() string {
	switch c {
	case Success:
		return "Success"
	case GeneralError:
		return "General Error"
	case PartialSuccess:
		return "Partial Success"
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
