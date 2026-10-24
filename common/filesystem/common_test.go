package filesystem

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shortReader returns at most max bytes from each Read, as a kernel does for a read larger than it
// will return at once.
type shortReader struct {
	*bytes.Reader
	max int
}

func (r *shortReader) Read(p []byte) (int, error) {
	if len(p) > r.max {
		p = p[:r.max]
	}
	return r.Reader.Read(p)
}

// TestReadFilePart checks that readFilePart returns the whole part even when each Read returns
// only some of it, and that a file shorter than the part is an error rather than a padded part.
func TestReadFilePart(t *testing.T) {
	data := []byte("0123456789abcdefghij")

	t.Run("a part larger than one read is read in full", func(t *testing.T) {
		part, err := readFilePart(&shortReader{Reader: bytes.NewReader(data), max: 3}, 2, 16)

		require.NoError(t, err)
		assert.Equal(t, data[2:17], part)
	})

	t.Run("a file that ends before the part is an error", func(t *testing.T) {
		_, err := readFilePart(&shortReader{Reader: bytes.NewReader(data), max: 3}, 10, 29)

		assert.ErrorIs(t, err, io.ErrUnexpectedEOF)
	})
}
