package filesystem

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"os"
)

// writeSeekCloser combines io.Writer, io.Seeker, and io.Closer. This allows us to use the
// limitedFileWriter with mock file systems like Afero that don't provide a real os.File.
type writeSeekCloser interface {
	io.Writer
	io.Seeker
	io.Closer
}

// limitedFileWriter implements the io.WriteCloser interface but ensures the caller only ever writes
// between offsetStart/Stop.
type limitedFileWriter struct {
	file         writeSeekCloser
	offsetStart  int64
	offsetStop   int64
	isClosed     bool
	bytesWritten int64
}

func newLimitedFileWriter(file writeSeekCloser, offsetStart, offsetStop int64) *limitedFileWriter {
	return &limitedFileWriter{
		file:         file,
		offsetStart:  offsetStart,
		offsetStop:   offsetStop,
		isClosed:     false,
		bytesWritten: 0,
	}
}

func (w *limitedFileWriter) Write(p []byte) (n int, err error) {
	if w.isClosed {
		return 0, os.ErrClosed
	}

	if w.bytesWritten == 0 {
		_, err := w.file.Seek(w.offsetStart, io.SeekStart)
		if err != nil {
			return 0, err
		}
	}

	remainingSpace := (w.offsetStop - w.offsetStart + 1) - w.bytesWritten
	if int64(len(p)) > remainingSpace {
		return 0, fmt.Errorf("%w (required: %d, remaining: %d)", ErrNoSpaceForWrite, len(p), remainingSpace)
	}
	written, err := w.file.Write(p)
	w.bytesWritten += int64(written)
	return written, err
}

func (w *limitedFileWriter) Close() error {
	if w.isClosed {
		return os.ErrClosed
	}
	w.isClosed = true
	return w.file.Close()
}

func readFilePart(file io.ReadSeeker, offsetStart int64, offsetStop int64) ([]byte, error) {
	_, err := file.Seek(offsetStart, io.SeekStart)
	if err != nil {
		return nil, err
	}

	// Read and return the bytes from this part of the file into memory to avoid needing to re-read
	// from disk again.
	buf := make([]byte, (offsetStop-offsetStart)+1)
	_, err = file.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func getFilePartChecksumSHA256(filePart []byte) string {
	hasher := sha256.New()
	hasher.Write(filePart)
	hashSum := hasher.Sum(nil)
	return base64.StdEncoding.EncodeToString(hashSum)
}
