//go:build linux
// +build linux

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
		Mode:  st.Mode,
		Perm:  st.Mode & uint32(os.ModePerm),
		Atime: time.Unix(st.Atim.Sec, st.Atim.Nsec),
		Mtime: time.Unix(st.Mtim.Sec, st.Mtim.Nsec),
		Ctime: time.Unix(st.Ctim.Sec, st.Ctim.Nsec),
		Uid:   st.Uid,
		Gid:   st.Gid,
	}
}
