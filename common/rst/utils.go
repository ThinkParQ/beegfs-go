package rst

import "fmt"

func appendError(accumulatedErr error, nextErr error) error {
	if accumulatedErr == nil {
		return nextErr
	}
	if nextErr == nil {
		return accumulatedErr
	}
	return fmt.Errorf("%w; %w", accumulatedErr, nextErr)
}
