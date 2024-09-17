package types

import "strings"

// MultiError can be used anywhere you need to collect the results of multiple
// distinct operations that could fail independently. It should generally not be
// used if you want to collect errors from a chain of operations that should be
// able to be unwrapped using errors.Unwrap.
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
