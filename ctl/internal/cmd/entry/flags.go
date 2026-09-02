package entry

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/thinkparq/beegfs-go/common/beegfs"
	"github.com/thinkparq/beegfs-go/ctl/internal/util"
)

const (
	// Equivalent of STRIPEPATTERN_MIN_CHUNKSIZE
	minChunksize = 1024 * 64
	// BeeGFS represents chunksize as a uint32, so the max is UINT32_MAX.
	maxChunksize = math.MaxUint32
)

type chunksizeFlag struct {
	// Pointer to a pointer to a uint32. Required so we can set SetEntriesConfig fields directly using flags.
	p **uint32
}

func newChunksizeFlag(p **uint32) *chunksizeFlag {
	return &chunksizeFlag{p: p}
}

func (f *chunksizeFlag) String() string {
	if *f.p == nil {
		// Default printed in help text.
		return "unchanged"
	}
	return fmt.Sprintf("%d", **f.p)
}

func (f *chunksizeFlag) Type() string {
	return "<bytes>"
}

func (f *chunksizeFlag) Set(value string) error {
	chunksize, err := util.ParseIntFromStr(value)
	if err != nil {
		return err
	}
	if chunksize < minChunksize || chunksize > maxChunksize {
		return fmt.Errorf("parsed chunksize (%d bytes) is out of bounds (must be between %d bytes and %d bytes)", chunksize, minChunksize, maxChunksize)
	}
	if (chunksize & (chunksize - 1)) != 0 {
		return fmt.Errorf("chunksize is not a power of 2: %d", chunksize)
	}

	finalChunksize := uint32(chunksize)
	*f.p = &finalChunksize
	return nil
}

type poolFlag struct {
	p **beegfs.EntityId
}

func newPoolFlag(p **beegfs.EntityId) *poolFlag {
	return &poolFlag{p: p}
}

func (f *poolFlag) String() string {
	if *f.p == nil {
		return "unchanged"
	}
	return fmt.Sprintf("%d", **f.p)
}

func (f *poolFlag) Type() string {
	return "[alias|id]"
}

func (f *poolFlag) Set(value string) error {
	sp, err := beegfs.NewEntityIdParser(16, beegfs.Storage).Parse(value)
	if err != nil {
		return err
	}
	*f.p = &sp
	return nil
}

type stripePatternFlag struct {
	p **beegfs.StripePatternType
}

// validStripePatterns lists the patterns a user can set, in the order they are advertised in help.
// Each pattern's display name and its separator spellings are accepted via beegfs.EnumInputMatches;
// legacyAliases keeps the tokens accepted before the enums were standardized working so existing
// scripts don't break. Note StripePatternRaid10 is intentionally absent: it is a pattern the system
// can report but not one a user sets.
var validStripePatterns = []struct {
	pattern       beegfs.StripePatternType
	legacyAliases []string
}{
	{pattern: beegfs.StripePatternRaid0},
	{pattern: beegfs.StripePatternBuddyMirror, legacyAliases: []string{"mirrored"}},
}

func newStripePatternFlag(p **beegfs.StripePatternType) *stripePatternFlag {
	return &stripePatternFlag{p: p}
}

// validStripePatternKeys returns the patterns to advertise in help, as their display names so what
// `entry info` prints can be typed straight back in.
func validStripePatternKeys() []string {
	keys := make([]string, 0, len(validStripePatterns))
	for _, p := range validStripePatterns {
		keys = append(keys, p.pattern.String())
	}
	return keys
}

func (f *stripePatternFlag) String() string {
	if *f.p == nil {
		return "unchanged"
	}
	return (**f.p).String()
}

func (f *stripePatternFlag) Type() string {
	return "<pattern>"
}

func (f *stripePatternFlag) Set(value string) error {
	for _, p := range validStripePatterns {
		if beegfs.EnumInputMatches(value, p.pattern, p.legacyAliases...) {
			// Copy the value so the caller can do whatever they want with it.
			pattern := p.pattern
			*f.p = &pattern
			return nil
		}
	}
	return fmt.Errorf("unsupported stripe pattern (supported patterns: %s)", strings.Join(validStripePatternKeys(), ", "))
}

type numTargetsFlag struct {
	p **uint32
}

func newNumTargetsFlag(p **uint32) *numTargetsFlag {
	return &numTargetsFlag{p: p}
}

func (f *numTargetsFlag) String() string {
	if *f.p == nil {
		return "unchanged"
	}
	return fmt.Sprintf("%d", **f.p)
}

func (f *numTargetsFlag) Type() string {
	return "<number>"
}

func (f *numTargetsFlag) Set(value string) error {
	nt, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return err
	}
	numTargets := uint32(nt)
	*f.p = &numTargets
	return nil
}

type accessControlFlag struct {
	p **beegfs.AccessFlags
}

func newAccessControlFlag(p **beegfs.AccessFlags) *accessControlFlag {
	return &accessControlFlag{p: p}
}

func (f *accessControlFlag) String() string {
	if *f.p == nil {
		return "unchanged"
	}
	return (**f.p).String()
}

func (f *accessControlFlag) Type() string {
	return "<unlocked|locked-read|locked-write|locked-read-write|none>"
}

// Set accepts each access flag's display name and its separator spellings, plus the read-lock family
// of tokens this flag accepted before the enums were standardized. The legacy tokens name what is
// blocked where the display form names what is locked, so neither can be derived from the other and
// both have to be listed.
func (f *accessControlFlag) Set(value string) error {
	// Create a new AccessFlags if it doesn't exist
	if *f.p == nil {
		*f.p = new(beegfs.AccessFlags)
	}

	switch {
	case beegfs.EnumInputMatches(value, beegfs.AccessFlagUnlocked, "none"):
		**f.p = beegfs.AccessFlagUnlocked
	case beegfs.EnumInputMatches(value, beegfs.AccessFlagReadLock, "read-lock"):
		**f.p = beegfs.AccessFlagReadLock
	case beegfs.EnumInputMatches(value, beegfs.AccessFlagWriteLock, "write-lock"):
		**f.p = beegfs.AccessFlagWriteLock
	case beegfs.EnumInputMatches(value, beegfs.AccessFlagReadLock|beegfs.AccessFlagWriteLock, "read-write-lock"):
		**f.p = beegfs.AccessFlagReadLock | beegfs.AccessFlagWriteLock
	default:
		return fmt.Errorf("invalid access flags value: %s (valid values: %s, none)", value,
			strings.Join([]string{
				beegfs.AccessFlagUnlocked.String(),
				beegfs.AccessFlagReadLock.String(),
				beegfs.AccessFlagWriteLock.String(),
				(beegfs.AccessFlagReadLock | beegfs.AccessFlagWriteLock).String(),
			}, ", "))
	}

	return nil
}

type dataStateFlag struct {
	p **beegfs.DataState
}

func newDataStateFlag(p **beegfs.DataState) *dataStateFlag {
	return &dataStateFlag{p: p}
}

// namedDataStates are the data states with a display name, used to accept that name (and its
// kebab-case spelling) as input. The remaining values in the 0-7 range are reserved and unnamed, so
// they stay reachable only through the numeric form.
var namedDataStates = []beegfs.DataState{
	beegfs.DataStateAvailable,
	beegfs.DataStateManualRestore,
	beegfs.DataStateAutoRestore,
	beegfs.DataStateDelayedRestore,
	beegfs.DataStateUnavailable,
}

func (f *dataStateFlag) String() string {
	if *f.p == nil {
		return "unchanged"
	}
	return (**f.p).String()
}

func (f *dataStateFlag) Type() string {
	return "<state|0-7|none>"
}

// Set accepts a data state's display name or its kebab-case spelling, and still accepts the raw 0-7
// numeric form this support-only flag has always taken so existing scripts don't break.
func (f *dataStateFlag) Set(value string) error {
	// Create a new DataState if it doesn't exist
	if *f.p == nil {
		*f.p = new(beegfs.DataState)
	}

	// Handle special "none" value
	if beegfs.NormalizeEnumInput(value) == "none" {
		**f.p = 0
		return nil
	}

	if state, ok := beegfs.MatchEnumInput(value, namedDataStates...); ok {
		**f.p = state
		return nil
	}

	// Parse the data state
	val, err := strconv.ParseUint(value, 10, 8)
	if err != nil {
		return fmt.Errorf("invalid data state: %s (must be one of %s, a numeric value between 0-7, or 'none')",
			value, strings.Join(dataStateNames(), ", "))
	}

	if val > 7 {
		return fmt.Errorf("invalid data state value: %d (must be 0-7)", val)
	}

	// Set the new data state
	**f.p = beegfs.DataState(val)
	return nil
}

// dataStateNames returns the display names of the named data states, for help and error messages.
func dataStateNames() []string {
	names := make([]string, 0, len(namedDataStates))
	for _, s := range namedDataStates {
		names = append(names, s.String())
	}
	return names
}

type permissionsFlag struct {
	p **int32
}

func newPermissionsFlag(p **int32, defaultPerm int32) *permissionsFlag {
	// This actually sets the default.
	if *p == nil {
		*p = &defaultPerm
	}
	return &permissionsFlag{p: p}
}

func (f *permissionsFlag) String() string {
	if *f.p == nil {
		return ""
	}
	return fmt.Sprintf("%#o", **f.p)
}

func (f *permissionsFlag) Type() string {
	return "<permissions>"
}

func (f *permissionsFlag) Set(value string) error {
	// Base 8 because we expect permissions are specified in octal.
	p, err := strconv.ParseInt(value, 8, 32)
	if err != nil {
		return err
	}
	perm := int32(p)
	*f.p = &perm
	return nil
}

type userFlag struct {
	p **uint32
}

func newUserFlag(p **uint32) *userFlag {
	// This actually sets the default.
	if *p == nil {
		defaultUID := uint32(os.Geteuid())
		*p = &defaultUID
	}
	return &userFlag{p: p}
}

func (f *userFlag) String() string {
	if *f.p == nil {
		return "none"
	}
	return fmt.Sprintf("%d", **f.p)
}

func (f *userFlag) Type() string {
	return "<id>"
}

func (f *userFlag) Set(value string) error {
	v, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return err
	}
	flag := uint32(v)
	*f.p = &flag
	return nil
}

type groupFlag struct {
	p **uint32
}

func newGroupFlag(p **uint32) *groupFlag {
	// This actually sets the default.
	if *p == nil {
		defaultGID := uint32(os.Getegid())
		*p = &defaultGID
	}
	return &groupFlag{p: p}
}

func (f *groupFlag) String() string {
	if *f.p == nil {
		return "none"
	}
	return fmt.Sprintf("%d", **f.p)
}

func (f *groupFlag) Type() string {
	return "<id>"
}

func (f *groupFlag) Set(value string) error {
	v, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return err
	}
	flag := uint32(v)
	*f.p = &flag
	return nil
}
