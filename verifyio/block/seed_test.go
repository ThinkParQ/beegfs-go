// This is a unit test.
//
// Coverage: DeriveSeed's determinism, its coordinate-order sensitivity, the
// avalanche property it inherits from mixSeed, and the specific collision class
// it exists to close -- structurally composed coordinates with equal XOR folds
// producing identical seeds, and therefore identical bodies.
package block

import (
	"fmt"
	"math/bits"
	"testing"
)

func TestDeriveSeedDeterministic(t *testing.T) {
	const runSeed = uint64(0xdeadbeefcafe1234)

	a := DeriveSeed(runSeed, 7, 4096, 3)
	b := DeriveSeed(runSeed, 7, 4096, 3)
	if a != b {
		t.Errorf("DeriveSeed not deterministic: %#x != %#x", a, b)
	}

	// Total on zero coordinates: still mixed, not runSeed handed back.
	if got := DeriveSeed(runSeed); got == runSeed {
		t.Errorf("DeriveSeed(runSeed) = runSeed (%#x); the trailing mix did not run", got)
	}
}

func TestDeriveSeedOrderMatters(t *testing.T) {
	const runSeed = uint64(0x0123456789abcdef)

	if ab, ba := DeriveSeed(runSeed, 1, 2), DeriveSeed(runSeed, 2, 1); ab == ba {
		t.Errorf("DeriveSeed is order-insensitive: (1,2) and (2,1) both gave %#x", ab)
	}
	// Arity matters too: a trailing zero coordinate is not a no-op.
	if two, three := DeriveSeed(runSeed, 1, 2), DeriveSeed(runSeed, 1, 2, 0); two == three {
		t.Errorf("a trailing zero coordinate was absorbed: both gave %#x", two)
	}
}

// TestDeriveSeedAvalanche pins that a single-bit change anywhere in the input
// decorrelates the output. Without it the composition could pass the
// distinctness tests above while still leaking the caller's field layout --
// which is what makes aligned collisions possible in the first place.
func TestDeriveSeedAvalanche(t *testing.T) {
	const runSeed = uint64(0xa5a5a5a5a5a5a5a5)
	base := DeriveSeed(runSeed, 9, 8192)

	// A conservative bound: an unmixed or linear composition flips very few
	// bits, so anything in this band rules that out without asserting the
	// exact statistics of splitmix64.
	const minFlipped, maxFlipped = 12, 52

	check := func(t *testing.T, what string, got uint64) {
		t.Helper()
		flipped := bits.OnesCount64(base ^ got)
		if flipped < minFlipped || flipped > maxFlipped {
			t.Errorf("%s flipped %d bits (%#x vs %#x), want %d..%d",
				what, flipped, base, got, minFlipped, maxFlipped)
		}
	}

	for bit := 0; bit < 64; bit++ {
		t.Run(fmt.Sprintf("runSeed bit %d", bit), func(t *testing.T) {
			check(t, "runSeed", DeriveSeed(runSeed^(1<<bit), 9, 8192))
		})
		t.Run(fmt.Sprintf("coord0 bit %d", bit), func(t *testing.T) {
			check(t, "coord0", DeriveSeed(runSeed, 9^(1<<bit), 8192))
		})
		t.Run(fmt.Sprintf("coord1 bit %d", bit), func(t *testing.T) {
			check(t, "coord1", DeriveSeed(runSeed, 9, 8192^(1<<bit)))
		})
	}
}

// TestDeriveSeedClosesXORFoldCollisions is the regression this function exists
// for. mixSeed's doc describes the failure: a caller composing coordinates
// structurally (runSeed ^ a<<32 ^ b) reduces to C ^ a ^ b, so any two blocks
// with equal a^b generate byte-identical bodies -- the misdirected write the
// seed-bearing kinds are supposed to detect. Each pair below has an equal XOR
// fold and must still derive distinct seeds.
func TestDeriveSeedClosesXORFoldCollisions(t *testing.T) {
	const runSeed = uint64(0x51ed_c0de_0000_0001)

	for _, tc := range []struct{ aX, bX, aY, bY uint64 }{
		{3, 100, 100, 3},         // swapped coordinates
		{4, 101, 5, 100},         // equal fold, different values
		{0, 0, 1, 1},             // both folds zero
		{1 << 40, 1, 1, 1 << 40}, // beyond the 32-bit field the old packing assumed
	} {
		name := fmt.Sprintf("(%d,%d) vs (%d,%d)", tc.aX, tc.bX, tc.aY, tc.bY)
		t.Run(name, func(t *testing.T) {
			if tc.aX^tc.bX != tc.aY^tc.bY {
				t.Fatalf("test case is not an equal-fold pair: %#x != %#x",
					tc.aX^tc.bX, tc.aY^tc.bY)
			}
			x := DeriveSeed(runSeed, tc.aX, tc.bX)
			y := DeriveSeed(runSeed, tc.aY, tc.bY)
			if x == y {
				t.Errorf("equal-fold coordinates collided at %#x", x)
			}
		})
	}
}

// TestDeriveSeedSeparatesRuns is the property the xattrstore Writer depends on:
// the same block coordinates under two different run seeds must produce
// different bodies, because a stored seed that repeats between runs is what
// lets a stale read of the previous generation verify clean.
func TestDeriveSeedSeparatesRuns(t *testing.T) {
	const worker, offset, cycle = 0, 0, 0

	a := DeriveSeed(1, worker, offset, cycle)
	b := DeriveSeed(2, worker, offset, cycle)
	if a == b {
		t.Errorf("run seeds 1 and 2 derived the same block seed %#x", a)
	}

	bodyA := make([]byte, 512)
	bodyB := make([]byte, 512)
	for _, kind := range []Kind{KindPRNG, KindRepeat, KindCountUp, KindDecimal} {
		t.Run(kind.String(), func(t *testing.T) {
			if err := GenerateBody(kind, a, bodyA); err != nil {
				t.Fatalf("GenerateBody: %v", err)
			}
			if err := GenerateBody(kind, b, bodyB); err != nil {
				t.Fatalf("GenerateBody: %v", err)
			}
			if string(bodyA) == string(bodyB) {
				t.Errorf("%v bodies identical across run seeds", kind)
			}
		})
	}
}
