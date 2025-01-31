package rst

import "errors"

var (
	ErrEntryNotFound                = errors.New("entry was not found")
	ErrFileHasNoRSTs                = errors.New("entry does not have any remote storage target IDs configured")
	ErrFileOpenForWriting           = errors.New("entry is opened for writing on one or more clients")
	ErrFileOpenForReading           = errors.New("entry is opened for reading on one or more clients")
	ErrFileOpenForReadingAndWriting = errors.New("entry is opened for reading and writing on one or more clients")
)
