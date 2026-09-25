package nssresolver

// The messages exchanged between the beegfs CLI and beegfs-nss-resolver. The helper imports this
// package for them, so it must not import anything beyond the standard library and common/build.

// Name and Version are sent in the greeting. Bump Version on any change to the messages below, so
// that a CLI and helper from different releases refuse to talk rather than misread each other.
const (
	Name    = "beegfs-nss-resolver"
	Version = 1
)

type Greeting struct {
	Hello   string `json:"hello"`
	Version int    `json:"version"`
}

type Request struct {
	Seq    uint64   `json:"seq"`
	UIDs   []uint32 `json:"uids,omitempty"`
	GIDs   []uint32 `json:"gids,omitempty"`
	Users  []string `json:"users,omitempty"`
	Groups []string `json:"groups,omitempty"`
}

// Response reports every ID or name in the request that resolved. One missing from every map was
// not found, and one present in an error map failed to resolve and may succeed if asked again.
type Response struct {
	Seq        uint64            `json:"seq"`
	Users      map[uint32]string `json:"users"`
	Groups     map[uint32]string `json:"groups"`
	UIDErrors  map[uint32]string `json:"uid_errors"`
	GIDErrors  map[uint32]string `json:"gid_errors"`
	UserIDs    map[string]uint32 `json:"user_ids"`
	GroupIDs   map[string]uint32 `json:"group_ids"`
	NameErrors map[string]string `json:"name_errors"`
}
