package dispatch

import (
	"fmt"
	"path"
	"strings"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/thinkparq/protobuf/go/beewatch"
)

// ignorePattern is a single validated glob pattern. Whether it matches against the path basename or
// the full path depends only on the pattern, so it is computed once at construction rather than on
// every event.
type ignorePattern struct {
	// matchBasename is true for patterns without a "/", which are matched against the path basename
	// (e.g. ".*" or "*.tmp"); otherwise the pattern is matched against the full path and may use
	// "**" for recursive directory ignores.
	matchBasename bool
	pattern       string
}

// newIgnorePattern validates the glob and precomputes how it should be matched. doublestar uses "/"
// as the path separator for both Match and ValidatePattern, matching BeeGFS path semantics.
func newIgnorePattern(pattern string) (ignorePattern, error) {
	if !doublestar.ValidatePattern(pattern) {
		return ignorePattern{}, fmt.Errorf("invalid ignore-files glob-pattern %q", pattern)
	}
	return ignorePattern{
		matchBasename: !strings.ContainsRune(pattern, '/'),
		pattern:       pattern,
	}, nil
}

// match reports whether the pattern matches the given path. MatchUnvalidated is used because the
// pattern was already validated by newIgnorePattern, so the only otherwise-possible error (a
// malformed pattern) cannot occur here.
func (ip ignorePattern) match(fullPath, base string) bool {
	if ip.matchBasename {
		return doublestar.MatchUnvalidated(ip.pattern, base)
	}
	return doublestar.MatchUnvalidated(ip.pattern, fullPath)
}

// ignoreMatcher decides whether an event should be ignored based on its file path and type. It is
// built once at startup from the configured IgnoreFileRules and is immutable (and therefore safe
// for concurrent use) after construction.
type ignoreMatcher struct {
	// byType holds glob patterns that apply only to a specific event type.
	byType map[beewatch.V2Event_Type][]ignorePattern
	// wildcard holds glob patterns that apply to all event types (event-types ["*"] or absent).
	wildcard []ignorePattern
}

// newIgnoreMatcher parses and validates the configured rules and constructs the matcher. Event type
// names are validated via parseEventTypes and glob patterns via newIgnorePattern, so a malformed
// configuration fails fast at startup. Returns a nil matcher (and nil error) when no rules are
// configured, allowing the hot path to skip the check cheaply.
func newIgnoreMatcher(rules []IgnoreFileRule) (*ignoreMatcher, error) {
	if len(rules) == 0 {
		return nil, nil
	}

	im := &ignoreMatcher{byType: make(map[beewatch.V2Event_Type][]ignorePattern)}

	for _, r := range rules {
		specific, wildcard, err := parseEventTypes(r.EventTypes)
		if err != nil {
			return nil, fmt.Errorf("invalid ignore-files event-types: %w", err)
		}

		patterns := make([]ignorePattern, 0, len(r.GlobPatterns))
		for _, p := range r.GlobPatterns {
			pattern, err := newIgnorePattern(p)
			if err != nil {
				return nil, err
			}
			patterns = append(patterns, pattern)
		}

		if wildcard {
			im.wildcard = append(im.wildcard, patterns...)
		}
		for _, et := range specific {
			im.byType[et] = append(im.byType[et], patterns...)
		}
	}

	return im, nil
}

// match reports whether the given event should be ignored. It returns the first glob pattern that
// matches (for logging) and true, or "" and false if no pattern matches.
func (im *ignoreMatcher) match(eventType beewatch.V2Event_Type, eventPath string) (string, bool) {
	base := path.Base(eventPath)
	for _, p := range im.wildcard {
		if p.match(eventPath, base) {
			return p.pattern, true
		}
	}
	for _, p := range im.byType[eventType] {
		if p.match(eventPath, base) {
			return p.pattern, true
		}
	}
	return "", false
}
