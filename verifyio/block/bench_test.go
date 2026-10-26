// This is a performance benchmark, not a correctness unit test (no
// assertions; run with `go test -bench=.`).
//
// Coverage: throughput of MakeBlock (full block production), VerifyBlock
// (stripe-CRC check + body regeneration + compare), and GenerateBody in
// isolation (pattern generation alone, without CRC overhead) -- each across
// every body-pattern Kind and a few representative body sizes (4/16/64 KiB).
package block

import (
	"fmt"
	"sync"
	"testing"
)

// legendOnce ensures the column legend is printed exactly once per
// `go test -bench=...` invocation, by the first benchmark to run.
var legendOnce sync.Once

func printLegend() {
	legendOnce.Do(func() {
		fmt.Println("# columns: name-<GOMAXPROCS>  iters  ns/op  MB/s (b.SetBytes)  [B/op  allocs/op with -benchmem]")
	})
}

// blockSizes covers a few body sizes likely to matter in practice:
// 4 KiB (page), 16 KiB, 64 KiB. On-disk size = BlockDataSize(bodyLen).
var blockSizes = []int{4096, 16 * 1024, 64 * 1024}

// BenchmarkMakeBlock measures the cost of producing a complete block
// (data-region generation + stripe CRCs). Run for each pattern at each
// body size; KindDecimal is what most tools use.
func BenchmarkMakeBlock(b *testing.B) {
	printLegend()
	kinds := []Kind{KindZeros, KindCountUp, KindRepeat, KindDecimal, KindPRNG}
	for _, k := range kinds {
		for _, bodyLen := range blockSizes {
			b.Run(k.String()+"/"+sizeLabel(bodyLen), func(b *testing.B) {
				buf := make([]byte, BlockDataSize(bodyLen))
				h := Header{}
				b.SetBytes(int64(BlockDataSize(bodyLen)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := MakeBlock(buf, &h, k, uint64(i), bodyLen); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkVerifyBlock measures end-to-end verification: stripe CRC
// check + body regeneration + bytes.Equal. Uses pre-built blocks.
func BenchmarkVerifyBlock(b *testing.B) {
	printLegend()
	kinds := []Kind{KindZeros, KindCountUp, KindRepeat, KindDecimal, KindPRNG}
	for _, k := range kinds {
		for _, bodyLen := range blockSizes {
			b.Run(k.String()+"/"+sizeLabel(bodyLen), func(b *testing.B) {
				buf := make([]byte, BlockDataSize(bodyLen))
				h := Header{}
				if err := MakeBlock(buf, &h, k, 1, bodyLen); err != nil {
					b.Fatal(err)
				}
				scratch := make([]byte, bodyLen)
				b.SetBytes(int64(BlockDataSize(bodyLen)))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					v, _ := VerifyBlock(buf, &h, scratch)
					if v != VerdictOK {
						b.Fatalf("verdict=%v", v)
					}
				}
			})
		}
	}
}

// BenchmarkGenerateBody isolates just the data-region-fill cost. Useful
// for comparing pattern generators in isolation from CRC noise.
func BenchmarkGenerateBody(b *testing.B) {
	printLegend()
	kinds := []Kind{KindZeros, KindCountUp, KindRepeat, KindDecimal, KindPRNG}
	for _, k := range kinds {
		for _, bodyLen := range blockSizes {
			b.Run(k.String()+"/"+sizeLabel(bodyLen), func(b *testing.B) {
				out := make([]byte, bodyLen)
				b.SetBytes(int64(bodyLen))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := GenerateBody(k, uint64(i), out); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func sizeLabel(n int) string {
	switch {
	case n >= 1024*1024:
		return fmtKB(n / 1024)
	case n >= 1024:
		return fmtKB(n / 1024)
	}
	return "B"
}

func fmtKB(kb int) string {
	return itoa(kb) + "KiB"
}

// itoa returns a base-10 string. Avoids pulling in strconv just for
// benchmark labels.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
