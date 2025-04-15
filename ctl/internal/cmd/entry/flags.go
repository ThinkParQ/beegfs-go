package entry

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

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

var validStripePatterns = map[string]beegfs.StripePatternType{"raid0": beegfs.StripePatternRaid0, "mirrored": beegfs.StripePatternBuddyMirror}

func newStripePatternFlag(p **beegfs.StripePatternType) *stripePatternFlag {
	return &stripePatternFlag{p: p}
}

func validStripePatternKeys() []string {
	keys := make([]string, 0, len(validStripePatterns))
	for k := range validStripePatterns {
		keys = append(keys, k)
	}
	return keys
}

func (f *stripePatternFlag) String() string {
	if *f.p == nil {
		return "unchanged"
	}
	for k, v := range validStripePatterns {
		if **f.p == v {
			return k
		}
	}
	return "invalid"
}

func (f *stripePatternFlag) Type() string {
	return "<pattern>"
}

func (f *stripePatternFlag) Set(value string) error {
	pattern, ok := validStripePatterns[value]
	if !ok {
		return fmt.Errorf("unsupported stripe pattern (supported patterns: %s)", strings.Join(validStripePatternKeys(), ", "))
	}
	// Copy the value so the caller can do whatever they want with it.
	p := pattern
	*f.p = &p
	return nil
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

type rstCooldownFlag struct {
	p **uint16
}

func newRstCooldownFlag(p **uint16) *rstCooldownFlag {
	return &rstCooldownFlag{p: p}
}

func (f *rstCooldownFlag) String() string {
	if *f.p == nil {
		return "unchanged"
	}
	return fmt.Sprintf("%d", **f.p)
}

func (f *rstCooldownFlag) Type() string {
	return "<duration>"
}

func (f *rstCooldownFlag) Set(value string) error {
	parsedTime, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("invalid duration %s: %w", value, err)
	}

	if parsedTime.Seconds() > math.MaxUint16 {
		return fmt.Errorf("cooldown cannot be greater than %d", math.MaxUint16)
	}

	cd := uint16(parsedTime.Seconds())
	cooldown := uint16(cd)
	*f.p = &cooldown
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

	// Format the access control flag in CLI format
	accessCtlFlag := **f.p
	switch accessCtlFlag {
	case beegfs.AccessFlagUnlocked:
		return "unlocked"
	case beegfs.AccessFlagReadLock:
		return "read-lock"
	case beegfs.AccessFlagWriteLock:
		return "write-lock"
	case beegfs.AccessFlagReadLock | beegfs.AccessFlagWriteLock:
		return "read-write-lock"
	default:
		return fmt.Sprintf("Unknown(%d)", accessCtlFlag)
	}
}

func (f *accessControlFlag) Type() string {
	return "<unlocked|read-lock|write-lock|read-write-lock|none>"
}

func (f *accessControlFlag) Set(value string) error {
	// Create a new AccessFlags if it doesn't exist
	if *f.p == nil {
		*f.p = new(beegfs.AccessFlags)
	}

	// Parse the access flags
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "unlocked", "none":
		**f.p = beegfs.AccessFlagUnlocked
	case "read-lock":
		**f.p = beegfs.AccessFlagReadLock
	case "write-lock":
		**f.p = beegfs.AccessFlagWriteLock
	case "read-write-lock":
		**f.p = beegfs.AccessFlagReadLock | beegfs.AccessFlagWriteLock
	default:
		return fmt.Errorf("invalid access flags value: %s (valid values: unlocked, read-lock, write-lock, read-write-lock, none)", value)
	}

	return nil
}

type dataStateFlag struct {
	p **beegfs.DataState
}

func newDataStateFlag(p **beegfs.DataState) *dataStateFlag {
	return &dataStateFlag{p: p}
}

func (f *dataStateFlag) String() string {
	if *f.p == nil {
		return "unchanged"
	}

	// Format the data state
	return fmt.Sprintf("%d", **f.p)
}

func (f *dataStateFlag) Type() string {
	return "<0-7|none>"
}

func (f *dataStateFlag) Set(value string) error {
	// Create a new DataState if it doesn't exist
	if *f.p == nil {
		*f.p = new(beegfs.DataState)
	}

	// Handle special "none" value
	if strings.ToLower(strings.TrimSpace(value)) == "none" {
		**f.p = 0
		return nil
	}

	// Parse the data state
	val, err := strconv.ParseUint(value, 10, 8)
	if err != nil {
		return fmt.Errorf("invalid data state: %s (must be a numeric value between 0-7 or 'none')", value)
	}

	if val > 7 {
		return fmt.Errorf("invalid data state value: %d (must be 0-7)", val)
	}

	// Set the new data state
	**f.p = beegfs.DataState(val)
	return nil
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
