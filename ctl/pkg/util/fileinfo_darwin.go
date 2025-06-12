//go:build darwin
// +build darwin

package util

import (
	"os"
	"path/filepath"
	"syscall"
	"time"
)

func statToFileInfo(path string, st *syscall.Stat_t) FileInfo {
	return FileInfo{
		Path:  path,
		Name:  filepath.Base(path),
		Size:  st.Size,
		Mode:  uint32(st.Mode),
		Perm:  uint32(st.Mode) & uint32(os.ModePerm),
		Atime: time.Unix(st.Atimespec.Sec, st.Atimespec.Nsec),
		Mtime: time.Unix(st.Mtimespec.Sec, st.Mtimespec.Nsec),
		Ctime: time.Unix(st.Ctimespec.Sec, st.Ctimespec.Nsec),
		Uid:   st.Uid,
		Gid:   st.Gid,
	}
}
