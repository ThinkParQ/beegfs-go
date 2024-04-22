// Filesystem provides a common interface for working with different file system types. It allows
// use of relative paths inside the actual file system for all operations and allows applications to
// seamlessly mock file systems for testing.
//
// The New() function is used with a path to an already mounted filesystem to initialize a
// Filesystem instance. The resulting Filesystem can be used directly, or optionally set as the
// MountPoint global variable exposed by this package to create a singleton instance of the
// Filesystem that can be used anywhere inside the application importing the filesystem package.
//
// Example initializing MountPoint and determining if a real or mock file system should be used:
//
//	var err error
//	filesystem.MountPoint, err = filesystem.New("/mnt/beegfs")
//	if err != nil {
//	    logger.Fatal("unable to access BeeGFS mount point", zap.Error(err))
//	}
//	if initialCfg.MountPoint == filesystem.MockFSIdentifier {
//	    logger.Warn("start requested with a mock file system, operations will happen in memory only")
//	}
package filesystem
