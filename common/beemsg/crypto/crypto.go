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
const Encrypt = false

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

	if Encrypt {
		buf = gcm.Seal(buf[:0], iv, buf, nil)
	} else {
		buf = gcm.Seal(buf, iv, nil, buf)
	}
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

	if Encrypt {
		_, err = gcm.Open(buf[:0], iv, buf, nil)
	} else {
		_, err = gcm.Open(buf, iv, buf[len(buf)-AesTagLen:], buf[:len(buf)-AesTagLen])
	}
	if err != nil {
		return err
	}

	return nil
}
