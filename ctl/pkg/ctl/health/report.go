// Package health collects the results of the BeeGFS health checks into a single Report value that
// can be rendered as either human-readable text or JSON, keeping the two representations in sync.
package health

import "encoding/json"

// Status represents the health of a single check or of the overall report. Values are ordered from
// best to worst so the worst status in a set can be found by numeric comparison.
type Status int

const (
	Unknown Status = iota
	Healthy
	Degraded
	Critical
)

// updateStatusIfWorse promotes s to newStatus if newStatus is worse (numerically greater).
func (s *Status) updateStatusIfWorse(newStatus Status) {
	if newStatus > *s {
		*s = newStatus
	}
}

// String returns a stable, machine-readable representation. Human-facing rendering (e.g. emojis) is
// the frontend's responsibility.
func (s Status) String() string {
	switch s {
	case Healthy:
		return "healthy"
	case Degraded:
		return "degraded"
	case Critical:
		return "critical"
	default:
		return "unknown"
	}
}

// MarshalJSON encodes the status as its String() form so JSON consumers get a stable value rather
// than a bare integer.
func (s Status) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// Section titles. These identify a section both when building the report and when rendering it, so
// they are defined once here rather than compared as string literals at each site.
const (
	SectionGeneralChecks = "General Checks"
	SectionBusyNodes     = "Busy Nodes"
	SectionTargets       = "Targets"
	SectionConnections   = "Connections to Server Nodes"
)

// Report is the full result of a health check and the single source of truth shared by the human
// and JSON renderers, so the two cannot drift.
type Report struct {
	FS       string    `json:"fs"` // "beegfs://<mgmtd-addr>"
	FsUUID   string    `json:"fsUUID"`
	Status   Status    `json:"status"` // overall status: the worst of all checks
	Sections []Section `json:"sections"`

	// ConnCheckErr is set (non-fatal) when not all client/server connections could be established, so
	// the connection checks may be incomplete. It is an operational note (rendered to stderr by the
	// frontend), not part of the serialized report.
	ConnCheckErr error `json:"-"`
}

// Section groups related checks under a title.
type Section struct {
	Title  string  `json:"title"`
	Checks []Check `json:"checks"`

	// Detail holds section-level structured data (e.g. TargetsDetail, ConnectionsDetail). Its clean
	// fields are serialized; any Raw field it carries for the human renderer is not.
	Detail any `json:"detail,omitempty"`
}

// Check is the result of a single named check.
type Check struct {
	Name    string `json:"name"`
	Status  Status `json:"status"`
	Summary string `json:"summary"`

	// Detail holds check-level structured data (e.g. BusyDetail). Its clean fields are serialized;
	// any Raw field it carries for the human renderer is not.
	Detail any `json:"detail,omitempty"`
}

// worst returns the worst status across every check in the report.
func (r *Report) worst() Status {
	worst := Healthy
	for _, section := range r.Sections {
		for _, check := range section.Checks {
			worst.updateStatusIfWorse(check.Status)
		}
	}
	return worst
}
