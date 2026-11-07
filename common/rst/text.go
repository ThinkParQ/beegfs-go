package rst

import (
	"fmt"
	"unicode"
	"unicode/utf8"
)

// Limits on text an admin gives Remote, which Remote stores and later shows to other users: target
// and backend names, and the reasons given with a delete or a cancel. The CLI prints such text to a
// terminal as it is, so a control character could move the cursor or rewrite what another admin
// sees, and an unbounded length could fill the database and every job message.
const (
	MaxNameLength   = 256
	MaxReasonLength = 1024
)

// CheckName returns an error when name is empty or fails CheckText with MaxNameLength. kind names
// what the name belongs to in the error, such as "remote storage backend". Targets and backends
// both use it, so their names follow one rule. A target's name is optional, so ValidateTarget calls
// it only for a target that has one.
func CheckName(kind string, name string) error {
	if name == "" {
		return fmt.Errorf("a %s needs a name", kind)
	}
	if err := CheckText("name", name, MaxNameLength); err != nil {
		return fmt.Errorf("invalid %s name: %w", kind, err)
	}
	return nil
}

// CheckText returns an error when value is longer than maxLength bytes, is not valid UTF-8, or holds
// a control character, such as a newline or an escape. field names the value in the error, such as
// "name".
func CheckText(field string, value string, maxLength int) error {
	if len(value) > maxLength {
		return fmt.Errorf("the %s is %d bytes long, and at most %d are allowed", field, len(value), maxLength)
	}
	if !utf8.ValidString(value) {
		return fmt.Errorf("the %s is not valid UTF-8", field)
	}
	for _, r := range value {
		if unicode.IsControl(r) {
			return fmt.Errorf("the %s contains the control character %U", field, r)
		}
	}
	return nil
}
