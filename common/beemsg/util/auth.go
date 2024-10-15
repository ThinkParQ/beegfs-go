package util

import (
	"crypto/sha256"
	"encoding/binary"
)

// Generates sha256 hash from the input byte slice and returns the 64 bit auth secret containing
// the 8 most significant bytes of the hash in little endian order. Matches the behavior of other
// implementations.
func GenerateAuthSecret(input []byte) uint64 {
	h := sha256.New()
	h.Write(input)
	secret := binary.LittleEndian.Uint64(h.Sum(nil)[0:8])

	return secret
}
