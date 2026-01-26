package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"

	"github.com/thinkparq/beegfs-go/common/beemsg/beeserde"
)

type AesEncryptionInfo struct {
	Iv  []byte
	Tag []byte
}

func NewAesEncryptionInfo() AesEncryptionInfo {
	iv := make([]byte, 12)
	tag := make([]byte, 16)

	return AesEncryptionInfo{Iv: iv, Tag: tag}
}

func (m *AesEncryptionInfo) Serialize(s *beeserde.Serializer) {
	// not needed, dummy
	beeserde.Zeroes(s, 28)
}
func (m *AesEncryptionInfo) Deserialize(d *beeserde.Deserializer) {
	// not needed, dummy
	beeserde.Skip(d, 28)
}

var dummyKey = []byte{'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 'a', 0}

func Aes256Encrypt(buf []byte) (AesEncryptionInfo, []byte, error) {
	ciph, err := aes.NewCipher(dummyKey)
	if err != nil {
		return AesEncryptionInfo{}, nil, err
	}

	gcm, err := cipher.NewGCM(ciph)
	if err != nil {
		return AesEncryptionInfo{}, nil, err
	}

	iv := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return AesEncryptionInfo{}, nil, err
	}

	buf = gcm.Seal(buf[:0], iv, buf, nil)
	// The tag is appended to the ciphertext buffer, we want to extract it
	tag := buf[len(buf)-gcm.Overhead():]
	buf = buf[:len(buf)-gcm.Overhead()]

	return AesEncryptionInfo{Iv: iv, Tag: tag}, buf, nil
}

func Aes256Decrypt(info AesEncryptionInfo, buf []byte) ([]byte, error) {
	block, err := aes.NewCipher(dummyKey)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(buf) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	// To decrypt we need to reappend the tag to the ciphertext buffer
	buf = append(buf, info.Tag...)

	_, err = gcm.Open(buf[:0], info.Iv, buf, nil)
	if err != nil {
		return nil, err
	}

	// And remove it again
	buf = buf[:len(buf)-16]

	return buf, nil
}
