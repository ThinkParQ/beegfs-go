package util

import (
	"fmt"
	"regexp"
	"strconv"
)

var siPrefixes = map[string]float64{
	"":   1,
	"k":  1e3,
	"kB": 1e3,
	"M":  1e6,
	"MB": 1e6,
	"G":  1e9,
	"GB": 1e9,
	"T":  1e12,
	"TB": 1e12,
	"P":  1e15,
	"PB": 1e15,
	"E":  1e18,
	"EB": 1e18,
	"Z":  1e21,
	"ZB": 1e21,
	"Y":  1e24,
	"YB": 1e24,
}

var iecPrefixes = map[string]float64{
	"":    1,
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
	"Zi":  1 << 70,
	"ZiB": 1 << 70,
	"Yi":  1 << 80,
	"YiB": 1 << 80,
}

// Parses a string in the form `<int>[kMGTPE][i]<unit>` into an integer.
//
// Takes the given integer and multiplies it according to the given SI suffix, using base 10
// (`10k` becomes 10000). When the `[i]` is given, base 2 is used (`10kiB` becomes 10240).
//
// The `<unit>` suffix is ignored and can be anything or be omitted.
func ParseIntFromStr(input string) (uint64, error) {
	re := regexp.MustCompile(`^([\d\.]+)([a-zA-Z]+B?)?$`)
	matches := re.FindStringSubmatch(input)
	if matches == nil {
		return 0, fmt.Errorf("invalid size format, must be a number followed by a valid SI prefix (k, M, G, T, P, E, Z, Y), IEC prefix (Ki, Mi, Gi, Ti, Pi, Ei, Ei, Zi, Yi), or no prefix for bytes")
	} else if len(matches) != 3 {
		return 0, fmt.Errorf("unexpected result parsing a number and unit from the provided input: %v", matches)
	}

	num, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, fmt.Errorf("unable to parse a valid number from the provided input: %w", err)
	}
	prefix := matches[2]

	if multiplier, ok := siPrefixes[prefix]; ok {
		return uint64(num * multiplier), nil
	}

	if multiplier, ok := iecPrefixes[prefix]; ok {
		return uint64(num * multiplier), nil
	}
	return 0, fmt.Errorf("invalid size prefix, must be a valid SI prefix (k, M, G, T, P, E, Z, Y), or IEC prefix (Ki, Mi, Gi, Ti, Pi, Ei, Ei, Zi, Yi), or no prefix for bytes")
}
