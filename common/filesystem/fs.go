package filesystem

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/afero"
)

// Global mount point used for all file system interactions. This allows us to
// work with relative paths inside BeeGFS for all file system operations, and
// also mock file system operations for testing.
var MountPoint Filesystem

const (
	MockFSIdentifier = "mock"
)

// The use of an interface is mostly to allow file system operations to be
// mocked for tests. This could also potentially allow us to change behavior
// depending if we're working with BeeGFS or some other file system.
type Filesystem interface {
	Stat(name string) (os.FileInfo, error)
	CreateWriteClose(name string, buf []byte) error
	Remove(name string) error
	Open(name string) (io.ReadCloser, error)
}

func New(mountPoint string) (Filesystem, error) {

	if mountPoint == "" || mountPoint == "/" {
		return nil, fmt.Errorf("invalid mount point provided (cannot be empty or '/')")
	}

	if mountPoint == MockFSIdentifier {
		return MockFS{Fs: afero.NewMemMapFs()}, nil
	}

	fsInfo, err := os.Stat(mountPoint)
	if err != nil {
		return nil, err
	}

	if !fsInfo.IsDir() {
		return nil, fmt.Errorf("path is not a directory")
	}
	// TODO: Possibly expand our list of checks. For example we could make sure
	// this is the root of a BeeGFS file system.
	return BeeGFS{MountPoint: mountPoint}, nil
}

type BeeGFS struct {
	MountPoint string
}

func (fs BeeGFS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(fs.MountPoint + name)
}

func (fs BeeGFS) CreateWriteClose(name string, buf []byte) error {
	// If useful we can actually implement this.
	return fmt.Errorf("CreateWriteClose should only be used with MockFS")
}

func (fs BeeGFS) Remove(name string) error {
	// If useful we can actually implement this.
	return fmt.Errorf("Remove should only be used with MockFS")
}

func (fs BeeGFS) Open(name string) (io.ReadCloser, error) {
	return os.Open(name)
}

type MockFS struct {
	Fs afero.Fs
}

func (fs MockFS) Stat(name string) (os.FileInfo, error) {
	return fs.Fs.Stat(name)
}

func (fs MockFS) CreateWriteClose(name string, buf []byte) error {
	file, err := fs.Fs.Create(name)
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

func (fs MockFS) Remove(name string) error {
	return fs.Fs.Remove(name)
}

func (fs MockFS) Open(name string) (io.ReadCloser, error) {
	return fs.Fs.Open(name)
}
