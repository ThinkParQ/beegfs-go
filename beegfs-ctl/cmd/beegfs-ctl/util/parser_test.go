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
		{"2.5T", 2500000000000, false}, // 2.5 * 1000 * 1000 * 1000 * 1000
		{"3.2Gi", 3435973836, false},   // 3.2 * 1024 * 1024 * 1024
		{"1536ki", 1572864, false},     // equivalent to 1.5Mi in bytes
		{"", 0, true},                  // invalid input
		{"10XY", 0, true},              // invalid prefix
		{"1.5.5G", 0, true},            // invalid number format
	}

	for _, test := range tests {
		result, err := ParseIntFromStr(test.input)
		if test.err {
			assert.Error(t, err, "expected an error for input %q", test.input)
		} else {
			assert.NoError(t, err, "did not expect an error for input %q", test.input)
			assert.Equal(t, test.expected, result, "unexpected result for input %q", test.input)
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
