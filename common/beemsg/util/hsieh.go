// Paul Hsieh derivative license
//
// The derivative content includes raw computer source code, ideas, opinions,
// and excerpts whose original source is covered under another license and
// transformations of such derivatives. Note that mere excerpts by themselves
// (with the exception of raw source code) are not considered derivative works
// under this license. Use and redistribution is limited to the following
// conditions:
//
// One may not create a derivative work which, in any way, violates the Paul
// Hsieh exposition license described above on the original content.
// One may not apply a license to a derivative work that precludes anyone else
// from using and redistributing derivative content.
// One may not attribute any derivative content to authors not involved in the
// creation of the content, though an attribution to the author is not
// necessary.

// Go implementation of Peter Hsiehs hash function as presented on
// <http://www.azillionmonkeys.com/qed/hash.html>
package util

import (
	"encoding/binary"
)

func popWord(data *[]byte) uint32 {
	word := binary.LittleEndian.Uint16((*data)[0:2])
	*data = (*data)[2:len(*data)]
	return uint32(word)
}

// Generate the hash value for the given data
func HsiehHash(data []byte) uint32 {
	length := len(data)

	if length == 0 {
		return 0
	}

	remainder := length & 3
	blocks := length >> 2

	hash := uint32(length)
	for i := 0; i < blocks; i += 1 {
		hash += popWord(&data)
		tmp := (popWord(&data) << 11) ^ hash
		hash = (hash << 16) ^ tmp
		hash += (hash >> 11)
	}

	switch remainder {
	case 3:
		hash += popWord(&data)
		hash ^= hash << 16
		hash ^= (uint32(data[0])) << 18
		hash += hash >> 11
	case 2:
		hash += popWord(&data)
		hash ^= hash << 11
		hash += hash >> 17
	case 1:
		hash += uint32(data[0])
		hash ^= hash << 10
		hash += hash >> 1
	case 0:
	// Nothing to do
	default:
	}

	hash ^= hash << 3
	hash += hash >> 5
	hash ^= hash << 4
	hash += hash >> 17
	hash ^= hash << 25
	hash += hash >> 6

	return hash
}
