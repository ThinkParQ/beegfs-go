package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseIntFromStr(t *testing.T) {
	tests := []struct {
		input    string
		expected uint64
		err      bool
	}{
		{"1024", 1024, false},
		{"1Mi", 1048576, false},
		{"2Mi", 2097152, false},
		{"2MiB", 2097152, false},
		{"4Mi", 4194304, false},
		{"1kB", 1000, false},
		{"1ki", 1024, false},
		{"1kiB", 1024, false},
		{"1.5Mi", 1572864, false}, // 1.5 * 1024 * 1024
		{"1G", 1000000000, false},
		{"1GB", 1000000000, false},
		{"2.5T", 2500000000000, false},                        // 2.5 * 1000 * 1000 * 1000 * 1000
		{"3.2Gi", 3435973836, false},                          // 3.2 * 1024 * 1024 * 1024
		{"1536ki", 1572864, false},                            // equivalent to 1.5Mi in bytes
		{"", 0, true},                                         // invalid input
		{"10XY", 0, true},                                     // invalid prefix
		{"1.5.5G", 0, true},                                   // invalid number format
		{"18446744073709551615", 18446744073709551615, false}, // Max uint64 in bytes does not return an error.
		{"18446744073709551616", 0, true},                     // Max uint64+1 in bytes returns an error.
		{"9007199254740992.0", 0, true},                       // Bytes must be specified as a whole number.
		{"8.00PiB", 9007199254740992, false},                  // There is no loss of precision up to 9007199254740992 for decimal values.
		{"9PiB", 10133099161583616, false},                    // There is no loss of precision above 9007199254740992 for whole numbers.
		{"18014398509481984KiB", 0, true},                     // Converting whole numbers that would overflow a uint64 results in an error.
		{"18014398509481984.00KiB", 0, true},                  // Converting decimals that would overflow a uint64 results in an error.
		{"1kiI", 0, true},                                     // Reject if the unit is not B for bytes.
		{"1kib", 1024, true},                                  // Reject if the unit is not an uppercase B for bytes.
	}

	for _, test := range tests {
		result, err := ParseIntFromStr(test.input)
		if test.err {
			assert.Error(t, err, "expected an error for input %s", test.input)
		} else {
			assert.NoError(t, err, "did not expect an error for input %s", test.input)
			assert.Equal(t, test.expected, result, "unexpected result for input %s", test.input)
		}
	}
}

func TestParseUintRangeFromStr(t *testing.T) {
	tests := []struct {
		input       string
		expectedMin uint64
		expectedMax uint64
		err         bool
	}{
		{"1-100", 1, 100, false},
		{" 100-200  ", 100, 200, false},
		{"1234", 1234, 1234, false},
		{"100-100", 100, 100, false},
		{"1-2000", 1, 2000, false},
		{"100-200X", 0, 0, true},
		{"200-100", 0, 0, true},
		{"100=200", 0, 0, true},
		{"0", 0, 0, true},
		{"2001", 0, 0, true},
		{"0-100", 0, 0, true},
		{"1900-2001", 0, 0, true},
		{"-1-100", 0, 0, true},
		{"-1234", 0, 0, true},
		{"-0", 0, 0, true},
	}

	for _, test := range tests {
		min, max, err := ParseUint64RangeFromStr(test.input, 1, 2000)
		if test.err {
			assert.Error(t, err, "expected an error for input %q", test.input)
		} else {
			assert.NoError(t, err, "did not expect an error for input %q", test.input)
			assert.Equal(t, test.expectedMin, min, "unexpected minimum for input %q", test.input)
			assert.Equal(t, test.expectedMax, max, "unexpected maximum for input %q", test.input)
		}
	}

}
