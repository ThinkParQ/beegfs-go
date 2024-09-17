package util

// Contains an actual error and extra information on how to exit the cmd app
type CtlError struct {
	inner    error
	exitCode int
}

// Wraps the given error together with the exit code - meant to be returned from a command to the
// caller on error. The app then exits with the given exit code.
func NewCtlError(err error, exitCode int) CtlError {
	return CtlError{inner: err, exitCode: exitCode}
}

func (err *CtlError) GetExitCode() int {
	return err.exitCode
}

func (err CtlError) Error() string {
	return err.inner.Error()
}
