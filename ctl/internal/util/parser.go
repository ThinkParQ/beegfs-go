package util

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/dsnet/golib/unitconv"
)

const (
	// UnlimitedText is the text returned by I64FormatPrefixWithUnlimited when the provided input is
	// the max I64 value. It should be used anywhere callers need to print similar output they want
	// to be consistent with the unlimited text returned by that function.
	UnlimitedText        = "∞"
	invalidUnitErrorText = "invalid size format, must be a number followed by a valid SI prefix (k, M, G, T, P, E), IEC prefix (ki, Mi, Gi, Ti, Pi, Ei, Ei), or no prefix for bytes"
)

var siAndIECPrefixMultipliers = map[string]float64{
	"":    1,
	"B":   1,
	"k":   1e3,
	"kB":  1e3,
	"K":   1e3,
	"KB":  1e3,
	"M":   1e6,
	"MB":  1e6,
	"G":   1e9,
	"GB":  1e9,
	"T":   1e12,
	"TB":  1e12,
	"P":   1e15,
	"PB":  1e15,
	"E":   1e18,
	"EB":  1e18,
	"ki":  1 << 10,
	"kiB": 1 << 10,
	"Ki":  1 << 10,
	"KiB": 1 << 10,
	"Mi":  1 << 20,
	"MiB": 1 << 20,
	"Gi":  1 << 30,
	"GiB": 1 << 30,
	"Ti":  1 << 40,
	"TiB": 1 << 40,
	"Pi":  1 << 50,
	"PiB": 1 << 50,
	"Ei":  1 << 60,
	"EiB": 1 << 60,
}

// Parses a string in the form `<number>[kMGTPE][i][B]` into uint64 bytes. This function takes the
// given value and multiplies it according to the given SI suffix, using base 10 (e.g., `10k`
// becomes 10000). When the `[i]` is given, base 2 is used (`10kiB` becomes 10240). An optional `B`
// suffix is allowed to be able to specify units like "PiB" or "PB".
//
// Inputs may be specified as a decimal, but as a result precision starts to degrade with very large
// input. There is no loss of precision for values specified as whole numbers. For details see:
// https://en.wikipedia.org/wiki/Double-precision_floating-point_format#Precision_limitations_on_integer_values
//
// In all cases if the parsed input would overflow an uint64 an error is returned.
func ParseIntFromStr(input string) (uint64, error) {
	re := regexp.MustCompile(`^([\d\.]+)([a-zA-Z]+B?)?$`)
	matches := re.FindStringSubmatch(input)
	if matches == nil {
		return 0, errors.New(invalidUnitErrorText)
	} else if len(matches) != 3 {
		return 0, fmt.Errorf("unexpected result parsing a number and unit from the provided input: %v", matches)
	}
	prefix := matches[2]

	multiplier, ok := siAndIECPrefixMultipliers[prefix]
	if !ok {
		return 0, fmt.Errorf("bytes must be specified as a whole number, did you mean to specify an SI or IEC prefix besides B? (provided input: %s)", input)
	}

	if !strings.Contains(matches[1], ".") {
		num, err := strconv.ParseUint(matches[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("unable to parse a valid number from the provided input (%s): %w", input, err)
		}
		// Check if multiplication would result in an overflow.
		if num != 0 && uint64(multiplier) > math.MaxUint64/num {
			return 0, fmt.Errorf("value parsed from the provided input would exceed the maximum allowed (%d)", uint64(math.MaxUint64))
		}
		return num * uint64(multiplier), nil
	} else if prefix == "" || prefix == "B" {
		// If the input was provided as a raw value (e.g., bytes) we refuse to use ParseFloat which
		// would lose precision given large integers. For example the max int64 9223372036854775807
		// will be treated as 9223372036854775808 which would likely cause issues elsewhere. Instead
		// return an error so the user knows not to specify bytes with a decimal.
		return 0, fmt.Errorf("bytes must be specified as a whole number, did you mean to specify an SI or IEC prefix besides B? (provided input: %s)", input)
	}

	num, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, fmt.Errorf("unable to parse a valid number from the provided input (%s): %w", input, err)
	}
	b := num * multiplier
	if b > math.MaxUint64 {
		return 0, fmt.Errorf("value parsed from the provided input (%f) is larger than the maximum allowed (%d)", b, uint64(math.MaxUint64))
	}
	return uint64(b), nil
}

// Parses a string in the form `<int>-<int>` or `<int>` into two integers representing the lowest
// and the highest value of a range.
func ParseUint64RangeFromStr(input string, minLower uint64, maxUpper uint64) (uint64, uint64, error) {
	if minLower > maxUpper {
		return 0, 0, fmt.Errorf("invalid range bounds configuration: minLower can't be larger than maxUpper")
	}

	formatError := "invalid range, must be in the form `<min>-<max>` or `<value>`"

	re := regexp.MustCompile(`^(\d+)-(\d+)$`)
	matches := re.FindStringSubmatch(strings.TrimSpace(input))
	if matches == nil {
		// No match, try to parse single number
		v, err := strconv.ParseUint(input, 10, 64)
		if err != nil {
			return 0, 0, errors.New(formatError)
		}
		if v < minLower {
			return 0, 0, fmt.Errorf("minimum value is %d, got %d", minLower, v)
		}
		if v > maxUpper {
			return 0, 0, fmt.Errorf("maximum value is %d, got %d", maxUpper, v)
		}

		return v, v, nil
	} else if len(matches) != 3 {
		return 0, 0, errors.New(formatError)
	}

	// Match, parse both numbers
	lower, err := strconv.ParseUint(matches[1], 10, 64)
	if err != nil {
		return 0, 0, errors.New(formatError)
	}
	if lower < minLower {
		return 0, 0, fmt.Errorf("minimum lower value is %d, got %d", minLower, lower)
	}
	upper, err := strconv.ParseUint(matches[2], 10, 64)
	if err != nil {
		return 0, 0, errors.New(formatError)
	}
	if upper > maxUpper {
		return 0, 0, fmt.Errorf("maximum upper value is %d, got %d", maxUpper, upper)
	}

	if lower > upper {
		return 0, 0, fmt.Errorf("invalid range, the lower value (%d) is larger than the upper value (%d)", lower, upper)
	}

	return lower, upper, nil
}

// I64FormatPrefixWithUnlimited is a wrapper around unitconv.FormatPrefix that allows the maximum
// value for a int64 to be interpreted as unlimited. This is helpful for modes like quotas where
// setting the limit to unlimited just sets the limit to the int64 max. The option withB can be set
// to control if the string should be suffixed with a "B" where the unit represents bytes.
func I64FormatPrefixWithUnlimited(val int64, m unitconv.Mode, prec int, withB bool) string {
	if val == math.MaxInt64 {
		return UnlimitedText
	}
	if withB {
		return unitconv.FormatPrefix(float64(val), m, prec) + "B"
	}
	return unitconv.FormatPrefix(float64(val), m, prec)
}
