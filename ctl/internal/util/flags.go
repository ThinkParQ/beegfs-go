package util

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/spf13/pflag"
)

// I64BytesVar defines an `int64` flag that accepts input as human-readable sizes (e.g., "1GiB",
// "128KB") and converts them into bytes. The flag is associated with a `*int64` variable, which is
// set to the converted byte value.
//
// Parameters:
//   - flags: A `pflag.FlagSet` where the flag will be registered.
//   - p: A pointer to an `int64` variable that will hold the flag's value after parsing.
//   - name: The name of the flag.
//   - shorthand: A shorthand letter that can be used after a single dash.
//   - defaultValue: The default value of the flag, provided as a string (e.g., "1GiB").
//   - usage: A string representing the usage/help text for the flag.
//
// Panics:
//   - This function panics if the default value cannot be parsed into an `int64` byte size.
//
// Example usage:
//
//	I64BytesVar(cmd.Flags(), &backendCfg.FileSize, "file-size", "s", "1GiB", "Set file size in human-readable format")
func I64BytesVar(flags *pflag.FlagSet, p *int64, name string, shorthand string, defaultValue string, usage string) {
	bf := newI64BytesFlag(p, defaultValue)
	flags.VarP(bf, name, shorthand, usage)
}

// i64BytesFlag implements the `pflag.Value` interface for flags that accept human-readable byte
// sizes and converts them to bytes (as `int64` values). It is used to define a custom flag that
// interprets values like "1GiB", "128MB", and converts them into an appropriate number of bytes.
// Intended to be used with I64BytesVar.
type i64BytesFlag struct {
	p            *int64
	defaultValue string
}

// newI64BytesFlag creates a new instance of `i64BytesFlag`, which stores an `int64` byte value
// converted from the provided human-readable `defaultValue` string (e.g., "1GiB"). Intended to be
// used with I64BytesVar.
func newI64BytesFlag(p *int64, defaultValue string) *i64BytesFlag {
	bf := &i64BytesFlag{p: p}
	err := bf.Set(defaultValue)
	if err != nil {
		panic(fmt.Sprintf("error setting default value (this is a bug): %s", err.Error()))
	}
	bf.defaultValue = defaultValue
	return bf
}

func (f *i64BytesFlag) String() string {
	return f.defaultValue
}

func (f *i64BytesFlag) Type() string {
	return "<size><unit>"
}

func (f *i64BytesFlag) Set(value string) error {
	sizeInBytes, err := ParseIntFromStr(value)
	if err != nil {
		return err
	}

	if sizeInBytes > math.MaxInt64 {
		return fmt.Errorf("parsed size (%d bytes) is out of bounds (must be between %d bytes and %d bytes)", sizeInBytes, 0, math.MaxInt64)
	}

	finalSize := int64(sizeInBytes)
	*f.p = finalSize
	return nil
}

// ValidatedStringFlag defines a flag that only accepts one of the allowed strings. Intended to be
// used with typed constants that satisfy the stringer interface. Allowed strings should by all
// lowercase and the user provided string will be converted to lowercase before checking it is one
// of the allowed strings.
func ValidatedStringFlag(allowed []fmt.Stringer, defaultValue fmt.Stringer) *validatedStringFlag {
	return &validatedStringFlag{
		value:   defaultValue,
		allowed: allowed,
	}
}

type validatedStringFlag struct {
	value   fmt.Stringer
	allowed []fmt.Stringer
}

func (f *validatedStringFlag) String() string {
	return f.value.String()
}

func (f *validatedStringFlag) Set(val string) error {
	val = strings.ToLower(val)
	for _, allowed := range f.allowed {
		if val == allowed.String() {
			f.value = allowed
			return nil
		}
	}
	return fmt.Errorf("invalid value: %q (allowed: %v)", val, f.allowed)
}

func (v *validatedStringFlag) Type() string {
	return "string"
}

type rstsFlag struct {
	p *[]uint32
}

func NewRemoteTargetsFlag(p *[]uint32) *rstsFlag {
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
