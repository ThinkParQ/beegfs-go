package beegfs

import "fmt"

// OpsErr is a custom type for error codes based on an int.
type OpsErr int32

// Enumeration of all error codes as per the provided C++ enumeration.
const (
	OpsErr_DUMMY_DONTUSEME        OpsErr = -1
	OpsErr_SUCCESS                OpsErr = 0
	OpsErr_INTERNAL               OpsErr = 1
	OpsErr_INTERRUPTED            OpsErr = 2
	OpsErr_COMMUNICATION          OpsErr = 3
	OpsErr_COMMTIMEDOUT           OpsErr = 4
	OpsErr_UNKNOWNNODE            OpsErr = 5
	OpsErr_NOTOWNER               OpsErr = 6
	OpsErr_EXISTS                 OpsErr = 7
	OpsErr_PATHNOTEXISTS          OpsErr = 8
	OpsErr_INUSE                  OpsErr = 9
	OpsErr_DYNAMICATTRIBSOUTDATED OpsErr = 10
	OpsErr_PARENTTOSUBDIR         OpsErr = 11
	OpsErr_NOTADIR                OpsErr = 12
	OpsErr_NOTEMPTY               OpsErr = 13
	OpsErr_NOSPACE                OpsErr = 14
	OpsErr_UNKNOWNTARGET          OpsErr = 15
	OpsErr_WOULDBLOCK             OpsErr = 16
	OpsErr_INODENOTINLINED        OpsErr = 17
	OpsErr_SAVEERROR              OpsErr = 18
	OpsErr_TOOBIG                 OpsErr = 19
	OpsErr_INVAL                  OpsErr = 20
	OpsErr_ADDRESSFAULT           OpsErr = 21
	OpsErr_AGAIN                  OpsErr = 22
	OpsErr_STORAGE_SRV_CRASHED    OpsErr = 23
	OpsErr_PERM                   OpsErr = 24
	OpsErr_DQUOT                  OpsErr = 25
	OpsErr_OUTOFMEM               OpsErr = 26
	OpsErr_RANGE                  OpsErr = 27
	OpsErr_NODATA                 OpsErr = 28
	OpsErr_NOTSUPP                OpsErr = 29
	OpsErr_UNKNOWNPOOL            OpsErr = 30
)

func (e OpsErr) Error() string {
	return e.String()
}

// String method returns a string representation of the error codes.
func (e OpsErr) String() string {
	switch e {
	case OpsErr_DUMMY_DONTUSEME:
		return "Dummy - do not use"
	case OpsErr_SUCCESS:
		return "Success"
	case OpsErr_INTERNAL:
		return "Internal error"
	case OpsErr_INTERRUPTED:
		return "Operation interrupted"
	case OpsErr_COMMUNICATION:
		return "Communication error"
	case OpsErr_COMMTIMEDOUT:
		return "Communication timeout"
	case OpsErr_UNKNOWNNODE:
		return "Unknown node"
	case OpsErr_NOTOWNER:
		return "Node is not owner of entry"
	case OpsErr_EXISTS:
		return "Entry exists already"
	case OpsErr_PATHNOTEXISTS:
		return "Path does not exist"
	case OpsErr_INUSE:
		return "Entry is in use"
	case OpsErr_DYNAMICATTRIBSOUTDATED:
		return "Dynamic attributes of entry are outdated"
	case OpsErr_PARENTTOSUBDIR:
		return "Invalid parent to subdirectory operation"
	case OpsErr_NOTADIR:
		return "Entry is not a directory"
	case OpsErr_NOTEMPTY:
		return "Directory is not empty"
	case OpsErr_NOSPACE:
		return "No space left"
	case OpsErr_UNKNOWNTARGET:
		return "Unknown storage target"
	case OpsErr_WOULDBLOCK:
		return "Operation would block"
	case OpsErr_INODENOTINLINED:
		return "Inode not inlined"
	case OpsErr_SAVEERROR:
		return "Underlying file system error"
	case OpsErr_TOOBIG:
		return "Argument too large"
	case OpsErr_INVAL:
		return "Invalid argument"
	case OpsErr_ADDRESSFAULT:
		return "Bad memory address"
	case OpsErr_AGAIN:
		return "Try again"
	case OpsErr_STORAGE_SRV_CRASHED:
		return "Potential cache loss for open file handle. (Server crash detected.)"
	case OpsErr_PERM:
		return "Permission denied"
	case OpsErr_DQUOT:
		return "Quota exceeded"
	case OpsErr_OUTOFMEM:
		return "Out of memory"
	case OpsErr_RANGE:
		return "Numerical result out of range"
	case OpsErr_NODATA:
		return "No data available"
	case OpsErr_NOTSUPP:
		return "Operation not supported"
	case OpsErr_UNKNOWNPOOL:
		return "Unknown storage pool"
	default:
		return fmt.Sprintf("Unknown error (%d)", int(e))
	}
}
