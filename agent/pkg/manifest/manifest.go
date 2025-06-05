package manifest

import "time"

// Manifest includes both user-defined file systems and system generated metadata. It is intended to
// help future proof the manifest definition by encapsulating user-defined filesystems so we can add
// system generated or other field as needed in a backwards compatible manner (e.g., versioning).
type Manifest struct {
	Metadata    Metadata    `yaml:"metadata"`
	Filesystems Filesystems `yaml:"filesystems"`
}

// Metadata contains auto-generated fields that are appended to the active manifest.
type Metadata struct {
	Updated time.Time `yaml:"updated"`
}

// Filesystems is a map of FsUUIDs to file systems.
type Filesystems map[string]Filesystem
