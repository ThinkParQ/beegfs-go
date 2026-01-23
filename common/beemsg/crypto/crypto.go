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
	iv  []byte
	tag []byte
}

func (m *AesEncryptionInfo) Serialize(s *beeserde.Serializer) {
	beeserde.SerializeBytes(s, m.iv)
	beeserde.SerializeBytes(s, m.tag)
}
func (m *AesEncryptionInfo) Deserialize(d *beeserde.Deserializer) {
	beeserde.DeserializeBytes(d, &m.iv)
	beeserde.DeserializeBytes(d, &m.tag)
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

	return AesEncryptionInfo{iv: iv, tag: tag}, buf, nil
}

func Aes256Decrypt(info AesEncryptionInfo, buf []byte) error {
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

	// To decrypt we need to reappend the tag to the ciphertext buffer
	buf = append(buf, info.tag...)

	_, err = gcm.Open(buf[:0], info.iv, buf, nil)
	if err != nil {
		return err
	}

	return nil
}
