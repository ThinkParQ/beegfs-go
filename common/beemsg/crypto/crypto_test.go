package crypto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"slices"
)

func TestEncryptDecrypt(t *testing.T) {
	buf := []byte("Hello Beegfs")
	plain := slices.Clone(buf)

	info, buf, err := Aes256Encrypt(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(plain), len(buf))

	buf, err = Aes256Decrypt(info, buf)
	assert.NoError(t, err)
	assert.Equal(t, plain, buf)
}

func TestEncryptDecryptTampering(t *testing.T) {
	buf := []byte("Hello Beegfs")
	plain := slices.Clone(buf)

	info, buf, err := Aes256Encrypt(buf)
	assert.NoError(t, err)
	assert.Equal(t, len(plain), len(buf))

	buf[0] ^= buf[0]

	buf, err = Aes256Decrypt(info, buf)
	assert.Error(t, err)
}
