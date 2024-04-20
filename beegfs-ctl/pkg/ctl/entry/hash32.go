// Paul Hsiehs hash function, available under http://www.azillionmonkeys.com/qed/hash.html
// Paul Hsieh OLD BSD license
// Copyright (c) 2010, Paul Hsieh All rights reserved.
//
// Redistribution and use in source and binary forms, with or without modification, are permitted
// provided that the following conditions are met:
//
//     Redistributions of source code must retain the above copyright notice, this list of
//     conditions and the following disclaimer.
//
//     Redistributions in binary form must reproduce the above copyright notice, this list of
//     conditions and the following disclaimer in the documentation and/or other materials provided
//     with the distribution.
//
//     Neither my name, Paul Hsieh, nor the names of any other contributors to the code use may not
//     be used to endorse or promote products derived from this software without specific prior
//     written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
// DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
// WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package entry

import "encoding/binary"

// A Go implementation of the BufferTK::hash32() function from the BeeGFS common toolkit. This
// implementation is not as "Super Fast" as the original C++ version, and should not be used in
// performance critical code paths. This is because it adheres to Go's safe memory manipulation
// practices, without pointer arithmetic and direct memory access (i.e., using unsafe). It is
// intended for recreating dentry paths and hash paths for verbose beegfs-ctl outputs.
func hash32(data string) uint32 {

	buf := []byte(data)
	len := len(buf)
	if len == 0 {
		return 0
	}

	hash := uint32(len)
	tmp := uint32(0)
	// The equivalent of len % 4 using a bitwise operation.
	rem := len & 3

	// Process each 4-byte chunk. This approach avoids the need to use unsafe, as would be required
	// for a more exact implementation of the equivalent C++ function.
	for i := 0; i < len-rem; i += 4 {
		hash += uint32(binary.LittleEndian.Uint16(buf[i:]))
		// Extract the next 16 bits, starting two bytes after index i:
		tmp = (uint32(binary.LittleEndian.Uint16(buf[i+2:])) << 11) ^ hash
		hash = (hash << 16) ^ tmp
		hash += hash >> 11
	}

	// Handle the remaining 0-3 bytes.
	tail := data[len-rem:]
	switch rem {
	case 3:
		hash += uint32(binary.LittleEndian.Uint16([]byte(tail)))
		hash ^= hash << 16
		hash ^= uint32(tail[2]) << 18
		hash += hash >> 11
	case 2:
		hash += uint32(binary.LittleEndian.Uint16([]byte(tail)))
		hash ^= hash << 11
		hash += hash >> 17
	case 1:
		hash += uint32(tail[0])
		hash ^= hash << 10
		hash += hash >> 1
	}

	// Force "avalanching" of final 127 bits
	hash ^= hash << 3
	hash += hash >> 5
	hash ^= hash << 4
	hash += hash >> 17
	hash ^= hash << 25
	hash += hash >> 6
	return hash
}
