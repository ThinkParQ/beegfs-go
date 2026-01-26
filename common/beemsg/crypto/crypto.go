package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

var dummyKey = []byte{'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 0}

const AesTagLen = 16

// Returns (iv, encrypted buf, error)
func Aes256Encrypt(buf []byte) ([]byte, []byte, error) {
	ciph, err := aes.NewCipher(dummyKey)
	if err != nil {
		return nil, nil, err
	}

	gcm, err := cipher.NewGCM(ciph)
	if err != nil {
		return nil, nil, err
	}

	iv := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, nil, err
	}

	buf = gcm.Seal(buf[:0], iv, buf, nil)
	return iv, buf, nil
}

func Aes256Decrypt(iv []byte, buf []byte) error {
	block, err := aes.NewCipher(dummyKey)
	if err != nil {
		return err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}

	nonceSize := gcm.NonceSize()
	if len(buf) < nonceSize {
		return fmt.Errorf("ciphertext too short")
	}

	_, err = gcm.Open(buf[:0], iv, buf, nil)
	if err != nil {
		return err
	}

	return nil
}
