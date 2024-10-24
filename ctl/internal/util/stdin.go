package util

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
)

// GetStdinDelimiterFromString is used to convert a user provided string to a byte that can be used
// as the delimiter when reading from stdin. It returns an error if the provided string cannot be
// parsed into a single character or valid escape sequence such as a newline.
func GetStdinDelimiterFromString(s string) (byte, error) {
	// The default stdin-delimiter used to print the user help won't parse correctly.
	if s == "\n" {
		s = `\n`
	}
	if unquoted, err := strconv.Unquote(`"` + s + `"`); err == nil && len(unquoted) == 1 {
		return unquoted[0], nil
	} else {
		return 0, fmt.Errorf("the stdin-delimiter must be a single character or valid escape sequence such as \"\\n\" for a newline or \"\\x00\" for null (provided: %s)", s)
	}
}

// ReadFromStdin reads strings from stdin separated by the provided delimiter into toChan until it
// reaches EOF then the toChan is closed. If an error is encountered it is immediately returned on
// the provided errChan then the toChan is closed without reading anything else from stdin.
func ReadFromStdin(ctx context.Context, delimiter byte, toChan chan<- string, errChan chan<- error) {
	scanner := bufio.NewScanner(os.Stdin)
	splitFunc := func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		for i := 0; i < len(data); i++ {
			if data[i] == delimiter {
				return i + 1, data[:i], nil
			}
		}
		// When EOF is reached, return what is left:
		if atEOF && len(data) != 0 {
			return len(data), data, nil
		}
		// Otherwise request more data
		return 0, nil, nil
	}
	scanner.Split(splitFunc)

	func() {
		defer close(toChan)
		for scanner.Scan() {
			input := scanner.Text()
			select {
			case toChan <- input:
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}
			if err := scanner.Err(); err != nil {
				errChan <- err
				return
			}
		}
	}()
}
