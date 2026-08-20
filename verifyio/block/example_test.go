// This is a set of Go runnable examples, not traditional unit tests: each
// Example function's output is checked against its trailing "// Output:"
// comment by `go test`, and the source doubles as the package's godoc usage
// documentation.
//
// Coverage: the canonical write-side call (MakeBlock producing a populated
// header ready to store in an xattr), the canonical read-side call
// (VerifyBlock against a just-written block), and the diagnostic path
// (VerifyBlock's verdict after a body byte is corrupted).
package block_test

import (
	"fmt"

	"github.com/thinkparq/beegfs-go/verifyio/block"
)

// ExampleMakeBlock shows the canonical write-side use: fill a block buffer
// and get back a populated header ready to marshal into an xattr.
func ExampleMakeBlock() {
	const bodyLen = 64
	buf := make([]byte, block.BlockDataSize(bodyLen))

	h := block.Header{
		WorkerID: 0,
		Cycle:    7,
		Offset:   0x1000,
	}
	if err := block.MakeBlock(buf, &h, block.KindDecimal, 42, bodyLen); err != nil {
		fmt.Println("err:", err)
		return
	}
	// h is now populated: Version, Kind, Seed, BodyLen, BodyCRC.
	// Marshal h to store it in an xattr; buf holds the on-disk block.
	fmt.Printf("kind=%s seed=%d cycle=%d bodyLen=%d\n",
		h.Kind, h.Seed, h.Cycle, h.BodyLen)
	// Output: kind=decimal seed=42 cycle=7 bodyLen=64
}

// ExampleVerifyBlock shows verifying a block that was just written. The
// header h is passed in (as if read from an xattr) alongside the on-disk
// block data.
func ExampleVerifyBlock() {
	const bodyLen = 32
	buf := make([]byte, block.BlockDataSize(bodyLen))
	h := block.Header{}
	_ = block.MakeBlock(buf, &h, block.KindCountUp, 100, bodyLen)

	verdict, _ := block.VerifyBlock(buf, &h, nil)
	fmt.Println(verdict)
	// Output: OK
}

// ExampleVerifyBlock_corrupt shows the diagnostic path: corrupting a body
// byte yields VerdictBodyCRCMismatch. The caller already holds the header
// and can use it for logging alongside the verdict.
func ExampleVerifyBlock_corrupt() {
	const bodyLen = 32
	buf := make([]byte, block.BlockDataSize(bodyLen))
	h := block.Header{Cycle: 99}
	_ = block.MakeBlock(buf, &h, block.KindCountUp, 1, bodyLen)

	buf[0] ^= 0xFF // flip a body byte

	verdict, _ := block.VerifyBlock(buf, &h, nil)
	fmt.Printf("verdict=%s cycle=%d\n", verdict, h.Cycle)
	// Output: verdict=BODY_CRC_MISMATCH cycle=99
}
