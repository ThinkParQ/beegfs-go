package filesystem

import (
	"bytes"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
)

func NewMockFS() Provider {
	return MockFS{Fs: afero.NewMemMapFs()}
}

type MockFS struct {
	Fs afero.Fs
}

func (fs MockFS) GetMountPath() string {
	return ""
}

func (fs MockFS) GetRelativePathWithinMount(path string) (string, error) {
	return filepath.Clean("/" + strings.TrimPrefix(path, "")), nil
}

func (fs MockFS) Stat(path string) (os.FileInfo, error) {
	return fs.Fs.Stat(path)
}

func (fs MockFS) Lstat(path string) (os.FileInfo, error) {
	// afereo does not have an equivalent for lstat.
	return nil, fmt.Errorf("not implemented")
}

func (fs MockFS) CreatePreallocatedFile(path string, size int64, overwrite bool) error {
	file, err := fs.Fs.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	file.Truncate(size)
	return nil
}

func (fs MockFS) CreateWriteClose(path string, buf []byte) error {
	file, err := fs.Fs.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (fs MockFS) Remove(path string) error {
	return fs.Fs.Remove(path)
}

func (fs MockFS) Open(path string) (io.ReadCloser, error) {
	return fs.Fs.Open(path)
}

func (fs MockFS) ReadFilePart(path string, offsetStart int64, offsetStop int64) (io.Reader, string, error) {
	file, err := fs.Fs.Open(path)
	if err != nil {
		return nil, "", err
	}
	defer file.Close()

	filePart, err := readFilePart(file, offsetStart, offsetStop)
	if err != nil {
		return nil, "", err
	}

	return bytes.NewReader(filePart), getFilePartChecksumSHA256(filePart), nil
}

func (fs MockFS) WriteFilePart(path string, offsetStart int64, offsetStop int64) (io.WriteCloser, error) {
	file, err := fs.Fs.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return nil, err
	}
	return newLimitedFileWriter(file, offsetStart, offsetStop), nil

}

func (fs MockFS) WalkDir(path string, fn fs.WalkDirFunc, opts ...WalkOption) error {
	return fmt.Errorf("not implemented")
}

func (fs MockFS) CopyXAttrsToFile(srcPath, dstPath string) error {
	return fmt.Errorf("not implemented")
}

func (fs MockFS) CopyContentsToFile(srcPath, dstPath string) error {
	return fmt.Errorf("not implemented")
}

func (fs MockFS) CopyOwnerAndMode(fromStat fs.FileInfo, dstPath string) error {
	return fmt.Errorf("not implemented")
}

func (fs MockFS) CopyTimestamps(fromStat fs.FileInfo, dstPath string) error {
	return fmt.Errorf("not implemented")
}

func (fs MockFS) OverwriteFile(srcPath, dstPath string) error {
	return fmt.Errorf("not implemented")
}
