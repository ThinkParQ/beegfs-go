package posixbench

import (
	"math"
	"math/bits"
	"time"
)

// latencyBuckets is 4 sub-buckets per octave over the whole uint64 range.
// Four is the resolution/size trade: a power-of-two histogram's bucket is as
// wide as its own lower edge, and sub-buckets narrow that. They divide the
// octave LINEARLY -- step is constant across it -- so the widest sub-bucket is
// the FIRST of an octave rather than the last, and its width relative to its
// own lower edge is what bounds the percentile error. A geometric subdivision
// would give a different, narrower bound; that is not what latBucket does.
// The whole thing is still one small fixed array per worker, with no
// allocation in the hot loop.
const (
	latSubBits    = 2
	latSubBuckets = 1 << latSubBits
	latBuckets    = 64 * latSubBuckets
)

// Latency accumulates per-operation durations.
//
// Count, Sum, Min and Max are exact. The percentiles are not: they come from a
// log-scale histogram, so each is the upper bound of the bucket the percentile
// falls in and reads HIGH by at most one sub-bucket's width -- see
// latencyBuckets for what bounds that. That is deliberate -- keeping every
// sample would be gigabytes on a large run, and a benchmark comparing week to
// week needs a stable summary far more than an exact tail.
type Latency struct {
	count   uint64
	sum     uint64 // nanoseconds
	min     uint64
	max     uint64
	buckets [latBuckets]uint64
}

// Observe records one operation's duration. Non-positive durations are counted
// at zero rather than dropped, so Count always equals the number of operations.
func (l *Latency) Observe(d time.Duration) {
	n := uint64(0)
	if d > 0 {
		n = uint64(d)
	}
	if l.count == 0 || n < l.min {
		l.min = n
	}
	if n > l.max {
		l.max = n
	}
	l.count++
	l.sum += n
	l.buckets[latBucket(n)]++
}

// Merge folds another Latency into this one, for combining per-worker
// histograms into a phase total.
func (l *Latency) Merge(o *Latency) {
	if o.count == 0 {
		return
	}
	if l.count == 0 || o.min < l.min {
		l.min = o.min
	}
	if o.max > l.max {
		l.max = o.max
	}
	l.count += o.count
	l.sum += o.sum
	for i := range l.buckets {
		l.buckets[i] += o.buckets[i]
	}
}

// latBucket maps a nanosecond count to its histogram index. Values below
// latSubBuckets are their own bucket, so the smallest durations are exact.
func latBucket(n uint64) int {
	if n < latSubBuckets {
		return int(n)
	}
	exp := 63 - bits.LeadingZeros64(n)
	sub := (n >> (exp - latSubBits)) & (latSubBuckets - 1)
	i := (exp-latSubBits+1)*latSubBuckets + int(sub)
	if i >= latBuckets {
		return latBuckets - 1
	}
	return i
}

// latBucketUpper returns the exclusive upper bound of bucket i in nanoseconds.
func latBucketUpper(i int) uint64 {
	if i < latSubBuckets {
		return uint64(i) + 1
	}
	exp := i/latSubBuckets + latSubBits - 1
	sub := uint64(i % latSubBuckets)
	if exp >= 64 {
		return math.MaxUint64
	}
	step := uint64(1) << (exp - latSubBits)
	// Addition, not OR: for the top sub-bucket (sub+1)*step is exactly the bit
	// 1<<exp already sets, so an OR folds it away and returns the octave's
	// LOWER edge -- a bound beneath its own bucket, which makes quantile report
	// a percentile beneath the samples it summarises and beneath the exact min.
	hi := uint64(1)<<exp + (sub+1)*step
	if hi < uint64(1)<<exp {
		// The carry the OR used to swallow can leave the top of uint64. Only
		// the last bucket does so, and its true bound is one past the end.
		return math.MaxUint64
	}
	return hi
}

// quantile returns the upper bound of the bucket holding the given quantile.
func (l *Latency) quantile(q float64) time.Duration {
	if l.count == 0 {
		return 0
	}
	want := uint64(math.Ceil(q * float64(l.count)))
	if want == 0 {
		want = 1
	}
	var seen uint64
	for i := range l.buckets {
		seen += l.buckets[i]
		if seen >= want {
			if u := latBucketUpper(i); u < l.max {
				return time.Duration(u)
			}
			return time.Duration(l.max)
		}
	}
	return time.Duration(l.max)
}
