package rst

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/oklog/ulid/v2"
)

// Limits on text an admin gives Remote, which Remote stores and later shows to other users: target
// and backend names, and the reasons given with a delete or a cancel. The CLI prints such text to a
// terminal as it is, so a control character could move the cursor or rewrite what another admin
// sees, and an unbounded length could fill the database and every job message.
//
// MaxNameLength is longer than the 32 bytes the management service allows for an alias. That
// limit exists because the BeeGFS client keeps aliases in fixed-size buffers, and RST names never
// reach the client. The names GenerateTargetName makes are up to 41 bytes long.
const (
	MaxNameLength   = 256
	MaxReasonLength = 1024
)

// namePattern is the rule the management service applies to the alias of a node, target, pool or
// buddy group, so every name an admin gives BeeGFS follows one rule. A name starts with a letter,
// so it is never read as a flag, such as "--force", or as a numeric ID. It holds only letters,
// digits, "-", "_" and ".", so it needs no quoting in a shell and no escaping in table or CSV
// output. It is at least two characters long, as an alias is.
var namePattern = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_.-]+$`)

// GenerateTargetName returns the name Remote gives a new target that was created without one:
// "rst-<id>-<ULID>". The ID makes the name easy to match with the target. The ULID makes it unique
// in practice, so a target that later reuses the ID gets a different name. A ULID is 26 characters,
// so the name is at most 41 bytes long, well within MaxNameLength.
func GenerateTargetName(id uint32) string {
	return fmt.Sprintf("rst-%d-%s", id, ulid.Make().String())
}

// CheckName returns an error when name does not follow the naming rule (namePattern) or is longer
// than MaxNameLength. kind names what the name belongs to in the error, such as "remote storage
// backend". Remote calls it whenever a name is set:
//   - Every backend name, on create and on every update, because backends are new and no stored
//     name can break the rule.
//   - A target name given on create, or changed by an update.
//
// It does not run on target names Remote already stores, or imports from its TOML file. Earlier
// versions accepted any target name there, and refusing one would stop Remote from starting.
// ValidateTarget applies only CheckText to those.
func CheckName(kind string, name string) error {
	if name == "" {
		return fmt.Errorf("a %s needs a name", kind)
	}
	if len(name) > MaxNameLength {
		return fmt.Errorf("invalid %s name: it is %d bytes long, and at most %d are allowed", kind, len(name), MaxNameLength)
	}
	if !namePattern.MatchString(name) {
		hint := ""
		if strings.HasPrefix(name, "-") {
			hint = " (hint: was the value of --name left out?)"
		}
		return fmt.Errorf("invalid %s name %q: it must start with a letter, be at least two characters long, and hold only letters, digits, '-', '_' and '.'%s", kind, name, hint)
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
