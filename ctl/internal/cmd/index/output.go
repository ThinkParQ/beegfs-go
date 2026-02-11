package index

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/viper"
	"github.com/thinkparq/beegfs-go/ctl/internal/cmdfmt"
	"github.com/thinkparq/beegfs-go/ctl/pkg/config"
	"go.uber.org/zap"
)

func newIndexLinePrintomatic(column string) cmdfmt.Printomatic {
	return cmdfmt.NewPrintomatic([]string{column}, []string{column})
}

func runIndexCommandWithPrint(backend indexBackend, binary string, args []string, logMsg string, logFields ...zap.Field) error {
	log, _ := config.GetLogger()
	log.Debug(logMsg, logFields...)
	tbl := newIndexLinePrintomatic("line")
	return runIndexCommandPrintLines(backend, binary, args, &tbl)
}

const maxIndexLineBytes = 16 * 1024 * 1024

func streamIndexLines(r io.Reader, tbl *cmdfmt.Printomatic) error {
	sanitize := shouldSanitizeIndexJSON()
	scanner := bufio.NewScanner(r)
	scanner.Split(scanCRLF)
	scanner.Buffer(make([]byte, 0, 64*1024), maxIndexLineBytes)
	for scanner.Scan() {
		line := scanner.Text()
		tbl.AddItem(sanitizeIndexLine(line, sanitize))
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			return fmt.Errorf("index output line exceeded %d bytes", maxIndexLineBytes)
		}
		return err
	}
	tbl.PrintRemaining()
	return nil
}

func sanitizeIndexLine(line string, sanitize bool) string {
	if !sanitize {
		return line
	}
	return strings.ReplaceAll(line, "\t", " ")
}

func shouldSanitizeIndexJSON() bool {
	switch config.OutputType(viper.GetString(config.OutputKey)) {
	case config.OutputJSON, config.OutputJSONPretty, config.OutputNDJSON:
		return true
	default:
		return false
	}
}

func runIndexCommandPrintLines(backend indexBackend, binary string, args []string, tbl *cmdfmt.Printomatic) error {
	return runIndexCommand(backend, binary, args, func(r io.Reader) error {
		return streamIndexLines(r, tbl)
	})
}

func scanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			return i + 1, data[:i], nil
		}
		if data[i] == '\r' {
			if i+1 < len(data) && data[i+1] == '\n' {
				return i + 2, data[:i], nil
			}
			return i + 1, data[:i], nil
		}
	}
	if atEOF {
		return len(data), data, nil
	}
	return 0, nil, nil
}
