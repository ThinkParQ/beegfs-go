package types

import "strings"

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
