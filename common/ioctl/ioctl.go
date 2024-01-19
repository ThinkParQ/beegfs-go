package ioctl

// The Go equivalents of the C macros typically defined by the Linux kernel's
// user space API (UAPI) in ioctl.h.

const (
	_ioc_nrbits   = 8
	_ioc_typebits = 8

	// These may need to be updated for specific architectures:
	_ioc_sizebits = 14
	_ioc_dirbits  = 2

	// We don't currently use these:
	// _ioc_nrmask   = 1<<_ioc_nrbits - 1
	// _ioc_typemask = 1<<_ioc_typebits - 1
	// _ioc_sizemask = 1<<_ioc_sizebits - 1
	// _ioc_dirmask  = 1<<_ioc_dirbits - 1

	_ioc_nrshift   = 0
	_ioc_typeshift = _ioc_nrshift + _ioc_nrbits
	_ioc_sizeshift = _ioc_typeshift + _ioc_typebits
	_ioc_dirshift  = _ioc_sizeshift + _ioc_sizebits

	// Direction bits may also need to be updated for specific architectures:
	// _ioc_write indicates userland is writing and kernel is reading.
	// _ioc_read means userland is reading and kernel is writing.
	_ioc_none  = 0
	_ioc_write = 1
	_ioc_read  = 2
)

// Functions that are the equivalent of the C macros used to create numbers.

// _ioc replicates the functionality of the _IOC macro from C. Typically you do
// not want to use _ioc directly, but instead use it through _ior or _iow. It
// combines various pieces of information into a single command number for ioctl
// operations using bitwise operations.
//
// Parameters:
//   - dir: the direction of data transfer (read, write, or both). This is usually one of
//     the constants _ioc_none, _ioc_write, or _ioc_read.
//   - t: the magic number or type identifier for the ioctl command. This is specific to
//     the driver or subsystem handling the ioctl. For BeeGFS this is beegfsIOCTypeID.
//   - nr: the command number or identifier within the type. Each ioctl command within
//     a given type should have a unique number.
//   - size: the size of the data involved in the ioctl call (if any). For ioctls that
//     work with data structures, this would be the size of the structure.
//
// Returns:
//   - A uintptr representing the ioctl command number, constructed by combining
//     the provided arguments using bitwise shifts and OR operations.
//
// Usage:
//   - This function is used to generate ioctl command numbers dynamically. The
//     returned command number can then be passed to syscall.Syscall (or similar
//     functions) to perform the ioctl operation.
//
// Example:
//
//	cmd := _ioc(_IOC_READ, myType, myCmd, unsafe.Sizeof(myDataStruct))
func _ioc(dir, t, nr, size uintptr) uintptr {
	return (dir << _ioc_dirshift) | (t << _ioc_typeshift) | (nr << _ioc_nrshift) | (size << _ioc_sizeshift)
}

// _ior replicates the functionality of the _IOR macro from C. It is used for
// creating ioctl command numbers for operations that read data from a device
// driver (i.e., data is transferred from kernel to user space).
//
// Parameters:
//   - t: the magic number or type identifier for the ioctl command. This is specific to
//     the driver or subsystem handling the ioctl. For BeeGFS use beegfsIOCTypeID.
//   - nr: the command number or identifier within the type. Each ioctl command within
//     a given type should have a unique number.
//   - size: the size of the data involved in the ioctl call. For ioctls that
//     read data structures, this would be the size of the structure being read.
//
// Returns: - A uintptr representing the ioctl command number for a read
// operation.
//
// Usage:
//   - This function is used when an ioctl command involves reading data from the kernel.
//     The returned command number can then be used with syscall.Syscall (or similar functions)
//     to perform the ioctl operation.
//
// Example:
//
//	cmd := _ior(myType, myCmd, unsafe.Sizeof(myDataStruct))
func _ior(t, nr, size uintptr) uintptr {
	return _ioc(_ioc_read, t, nr, size)
}

// _iow replicates the functionality of the _IOW macro from C. It is used for
// creating ioctl command numbers for operations that write data to a device
// driver (i.e., data is transferred from user space to kernel).
//
// Parameters:
//   - t: the magic number or type identifier for the ioctl command. This is specific to
//     the driver or subsystem handling the ioctl. For BeeGFS use beegfsIOCTypeID.
//   - nr: the command number or identifier within the type. Each ioctl command within
//     a given type should have a unique number.
//   - size: the size of the data involved in the ioctl call. For ioctls that
//     write data structures, this would be the size of the structure being written.
//
// Returns: - A uintptr representing the ioctl command number for a write
// operation.
//
// Usage:
//   - This function is used when an ioctl command involves writing data to the kernel.
//     The returned command number can then be used with syscall.Syscall (or similar functions)
//     to perform the ioctl operation.
//
// Example:
//
//	cmd := _iow(myType, myCmd, unsafe.Sizeof(myDataStruct))
func _iow(t, nr, size uintptr) uintptr {
	return _ioc(_ioc_write, t, nr, size)
}
