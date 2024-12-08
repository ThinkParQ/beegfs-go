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
	return "bytes"
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
	return "pattern"
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
	return "int"
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

type rstsFlag struct {
	p *[]uint32
}

func newRstsFlag(p *[]uint32) *rstsFlag {
	return &rstsFlag{p: p}
}

func (f *rstsFlag) String() string {
	if f.p == nil {
		return "unchanged"
	}
	if len(*f.p) == 0 {
		return "unchanged"
	}
	rsts := make([]string, 0, len(*f.p))
	for _, v := range *f.p {
		rsts = append(rsts, strconv.FormatUint(uint64(v), 10))
	}
	return strings.Join(rsts, ", ")
}

func (f *rstsFlag) Type() string {
	return "<id>[,<id>]..."
}

func (f *rstsFlag) Set(value string) error {
	if strings.ToLower(value) == "none" {
		*f.p = make([]uint32, 0)
		return nil
	}
	rstStrings := strings.Split(value, ",")
	rstUint32s := make([]uint32, 0, len(rstStrings))
	rstMap := make(map[uint64]struct{})
	for _, str := range rstStrings {
		parsedRST, err := strconv.ParseUint(str, 10, 32)
		if err != nil {
			return fmt.Errorf("error parsing RST ID %s: %w", str, err)
		}
		if parsedRST == 0 {
			return fmt.Errorf("using '0' as an RST ID is not allowed")
		}
		if _, ok := rstMap[parsedRST]; ok {
			return fmt.Errorf("RST ID %d was specified multiple times", parsedRST)
		}
		rstMap[parsedRST] = struct{}{}
		rstUint32s = append(rstUint32s, uint32(parsedRST))
	}
	*f.p = rstUint32s
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
	return "permissions"
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
	return "int"
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
	return "int"
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
