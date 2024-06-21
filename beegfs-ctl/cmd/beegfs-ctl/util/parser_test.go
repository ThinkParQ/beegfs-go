package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseSize(t *testing.T) {
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
		{"1Ki", 1024, false},
		{"1KiB", 1024, false},
		{"1.5Mi", 1572864, false}, // 1.5 * 1024 * 1024
		{"1G", 1000000000, false},
		{"1GB", 1000000000, false},
		{"2.5T", 2500000000000, false}, // 2.5 * 1000 * 1000 * 1000 * 1000
		{"3.2Gi", 3435973836, false},   // 3.2 * 1024 * 1024 * 1024
		{"1536Ki", 1572864, false},     // equivalent to 1.5Mi in bytes
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
