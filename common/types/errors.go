package types

import "strings"

// MultiError collects errors from multiple independent operations that may each fail separately.
// While it supports unwrapping via errors.Unwrap (as of Go 1.20+), using errors.Is or errors.As
// on a MultiError will indiscriminately match against all contained errors. This may be appropriate
// in some cases, but callers should be aware that any sentinel error match applies to any of the
// error chains, not a specific one.
type MultiError struct {
	Errors []error
}

func (e *MultiError) Error() string {
	var errs []string
	for _, err := range e.Errors {
		errs = append(errs, err.Error())
	}

	return strings.Join(errs, "; ")
}

func (e *MultiError) Unwrap() []error {
	return e.Errors
}
