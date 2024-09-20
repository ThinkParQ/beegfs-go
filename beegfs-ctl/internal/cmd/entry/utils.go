package entry

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
)

func getDelimiterFromString(s string) (byte, error) {
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

func readPathsFromStdin(ctx context.Context, delimiter byte, pathsChan chan<- string, errChan chan<- error) {
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
		defer close(pathsChan)
		for scanner.Scan() {
			input := scanner.Text()
			select {
			case pathsChan <- input:
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
