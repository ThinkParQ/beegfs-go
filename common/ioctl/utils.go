package ioctl

// cStringLen finds the length of a C style null-terminated string in a byte
// slice. We don't use `bytes.IndexByte(path, 0)` in case the string is not
// actually null-terminated (then we just return the length of the whole
// string).
func cStringLen(cString []byte) int {
	for i, b := range cString {
		if b == 0 {
			return i
		}
	}
	return len(cString)
}
