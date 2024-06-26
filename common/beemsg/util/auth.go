package util

func GenerateAuthSecret(input []byte) int64 {
	length := len(input)

	high := int64(HsiehHash(input[0 : length/2]))
	low := int64(HsiehHash(input[length/2 : length]))

	hash := (high << 32) | low

	return hash
}
