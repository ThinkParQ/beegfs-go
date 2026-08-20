// Package block defines a verifiable block format for IO testing.
//
// Every on-disk block consists of a body (deterministic data generated
// from a seed) followed by per-stripe CRC32C checksums. The header
// describing the block (seed, kind, node, timestamps, etc.) is stored
// exclusively in an extended attribute (see package xattrstore) rather
// than embedded in the block data. Keeping metadata out of the data
// region makes the on-disk layout cleaner and lets the verifier give
// precise per-stripe corruption reports.
//
// # On-disk layout
//
//	[body: BodyLen bytes][stripe CRCs: NumStripes(BodyLen)*4 bytes]
//
// The body is divided into StripeSize-byte stripes; a CRC32C (4 bytes,
// little-endian) for each stripe is appended after the body. The last
// stripe covers however many bytes remain and need not be a full stripe.
// Total on-disk size for a given body length is BlockDataSize(bodyLen).
//
// Header layout (little-endian, 90 bytes total):
//
//	offset  size  field
//	  0      8    Magic   = "BADGERIO"
//	  8      2    Version = 3
//	 10      4    Kind (data-region-generation mode)
//	 14      8    TimeNs    (time.Now().UnixNano())
//	 22     16    NodeName  (null-padded hostname of the writing node)
//	 38      4    TID       (caller-supplied; e.g. kernel TID)
//	 42      4    WorkerID  (caller-supplied)
//	 46      4    Tag       bits[7:0]=IOType  bit[8]=Fsynced (see TagIOTypeMask, TagFsynced)
//	 50      8    Cycle
//	 58      8    Offset    (byte offset in the target file)
//	 66      8    BodyLen
//	 74      8    Seed
//	 82      4    BodyCRC   (CRC32C over body bytes)
//	 86      4    HeadCRC   (CRC32C over header bytes [0..86))
//
// HeadCRC is computed last and covers everything before it, so a
// verifier can validate the header as a self-contained object before
// using any of its fields (BodyLen, Seed, ...) to regenerate and check
// the body.
package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	mrand "math/rand/v2"
	"time"
)

// HeaderSize is the on-wire size of a serialized record header.
const HeaderSize = 90

// HeaderVersion is the current header format version.
const HeaderVersion uint16 = 3

// Tag bit layout — the Tag field in Header carries two sub-fields:
//   - bits 7:0  (TagIOTypeMask): the fileops.IOType used for the write
//   - bit 8     (TagFsynced): set after a successful Sync() confirmed data
//     reached the storage servers; clear for buffered writes without a flush.
//
// A BODY_CORRUPT anomaly with TagFsynced=true proves the writer flushed
// before releasing the lock, ruling out a missing fsync as the cause.
const (
	TagIOTypeMask uint32 = 0x000000FF
	TagFsynced    uint32 = 1 << 8
)

// headerMagic is the 8-byte magic tag at the start of every header.
// Unexported so external code cannot mutate it (a single rogue
// assignment would silently break parsing process-wide).
var headerMagic = [8]byte{'B', 'A', 'D', 'G', 'E', 'R', 'I', 'O'}

// Kind identifies how to regenerate the data region of a block from its header.
type Kind uint32

const (
	KindPRNG    Kind = 1 // data region = stream from math/rand/v2 PCG seeded by Seed
	KindRepeat  Kind = 2 // data region = repeating 8-byte pattern derived from Seed
	KindCountUp Kind = 3 // data region[i] = byte(Seed + i)
	KindZeros   Kind = 4 // data region = all zero bytes
	KindOnes    Kind = 5 // data region = all 0xFF bytes
	KindDecimal Kind = 6 // data region = "NNN " tokens, NNN = (seed%512 + i) % 512, zero-padded
)

// String returns the printable name of a data-region-generation Kind.
func (k Kind) String() string {
	switch k {
	case KindPRNG:
		return "prng"
	case KindRepeat:
		return "repeat"
	case KindCountUp:
		return "countup"
	case KindZeros:
		return "zeros"
	case KindOnes:
		return "ones"
	case KindDecimal:
		return "decimal"
	default:
		return fmt.Sprintf("kind(%d)", uint32(k))
	}
}

// KindFromString parses a Kind from its printable name.
func KindFromString(s string) (Kind, error) {
	switch s {
	case "prng":
		return KindPRNG, nil
	case "repeat":
		return KindRepeat, nil
	case "countup":
		return KindCountUp, nil
	case "zeros":
		return KindZeros, nil
	case "ones":
		return KindOnes, nil
	case "decimal":
		return KindDecimal, nil
	default:
		return 0, fmt.Errorf("unknown pattern %q (want decimal | prng | repeat | countup | zeros | ones)", s)
	}
}

// Header is the in-memory form of a record header. The on-wire form is
// produced by MarshalHeader; the inverse parse is UnmarshalHeader.
//
// NodeName holds the null-padded hostname of the node that wrote the
// record. Use NodeNameFromString / NodeNameString to convert between
// the [16]byte wire form and a plain string.
type Header struct {
	Version  uint16
	Kind     Kind
	TimeNs   int64
	NodeName [16]byte
	TID      int32
	WorkerID int32
	Tag      uint32
	Cycle    uint64
	Offset   uint64
	BodyLen  uint64
	Seed     uint64
	BodyCRC  uint32
}

// NodeNameFromString converts a hostname string into the [16]byte form
// used by Header.NodeName. Strings longer than 15 characters are
// truncated; the last byte is always zero.
func NodeNameFromString(s string) [16]byte {
	var out [16]byte
	copy(out[:15], s)
	return out
}

// NodeNameString returns the NodeName as a plain string, stripping
// trailing null bytes.
func NodeNameString(name [16]byte) string {
	return string(bytes.TrimRight(name[:], "\x00"))
}

// String returns a single-line human-readable rendering of the header
// suitable for log lines and quick eyeballing. TimeNs is rendered as
// an RFC3339 timestamp; numeric fields that are commonly viewed as
// bitfields (Tag, Offset, Seed, BodyCRC) are printed in hex.
//
// Implements fmt.Stringer so callers can pass a Header (or *Header)
// straight to fmt.Println, log lines, %s / %v formatters, etc.
func (h Header) String() string {
	ts := time.Unix(0, h.TimeNs).UTC().Format(time.RFC3339Nano)
	return fmt.Sprintf(
		"ver=%d kind=%s time=%s node=%s tid=%d worker=%d tag=0x%08x "+
			"cycle=%d offset=0x%x bodyLen=%d seed=0x%016x bodyCRC=0x%08x",
		h.Version, h.Kind, ts, NodeNameString(h.NodeName), h.TID, h.WorkerID, h.Tag,
		h.Cycle, h.Offset, h.BodyLen, h.Seed, h.BodyCRC)
}

// castagnoli is the CRC32C polynomial table. Hardware-accelerated on
// modern x86_64 and ARM64. Shared across all header and body CRCs.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// MarshalHeader writes the 90-byte on-wire form of h into out (which
// must have at least HeaderSize bytes). The caller must have already
// populated h.BodyCRC. HeadCRC is computed over the first 86 bytes and
// placed in the last four. Returns an error if out is too small.
func MarshalHeader(out []byte, h *Header) error {
	if len(out) < HeaderSize {
		return fmt.Errorf("MarshalHeader: buf too small (%d < %d)", len(out), HeaderSize)
	}

	// Zero the header region first so reserved bytes are deterministic --
	// important for the header CRC to be stable.
	clear(out[:HeaderSize])

	copy(out[0:8], headerMagic[:])
	binary.LittleEndian.PutUint16(out[8:10], h.Version)
	binary.LittleEndian.PutUint32(out[10:14], uint32(h.Kind))
	binary.LittleEndian.PutUint64(out[14:22], uint64(h.TimeNs))
	copy(out[22:38], h.NodeName[:])
	binary.LittleEndian.PutUint32(out[38:42], uint32(h.TID))
	binary.LittleEndian.PutUint32(out[42:46], uint32(h.WorkerID))
	binary.LittleEndian.PutUint32(out[46:50], h.Tag)
	binary.LittleEndian.PutUint64(out[50:58], h.Cycle)
	binary.LittleEndian.PutUint64(out[58:66], h.Offset)
	binary.LittleEndian.PutUint64(out[66:74], h.BodyLen)
	binary.LittleEndian.PutUint64(out[74:82], h.Seed)
	binary.LittleEndian.PutUint32(out[82:86], h.BodyCRC)
	// HeadCRC covers [0..86).
	headCRC := crc32.Checksum(out[:86], castagnoli)
	binary.LittleEndian.PutUint32(out[86:90], headCRC)
	return nil
}

// Sentinel errors returned by UnmarshalHeader so callers can distinguish
// "this isn't one of our records" from "this is ours but is corrupt".
var (
	ErrBadMagic   = errors.New("block: bad magic")
	ErrBadHeadCRC = errors.New("block: header CRC mismatch")
	ErrBadVersion = errors.New("block: unsupported version")
	ErrTruncated  = errors.New("block: truncated header")
)

// UnmarshalHeader parses the on-wire header at in[:HeaderSize]. Returns
// ErrTruncated for a short buffer, ErrBadMagic for non-verifyio data,
// ErrBadVersion if the version field doesn't match HeaderVersion,
// or ErrBadHeadCRC if the CRC fails.
//
// On ErrBadVersion and ErrBadHeadCRC the returned Header IS populated
// from the on-wire bytes -- the values are untrusted but useful for
// diagnostic logging. On ErrTruncated and ErrBadMagic the returned
// Header is zero-valued (no usable data).
func UnmarshalHeader(in []byte) (Header, error) {
	var h Header
	if len(in) < HeaderSize {
		return h, ErrTruncated
	}
	if !bytes.Equal(in[:8], headerMagic[:]) {
		return h, ErrBadMagic
	}

	// Parse all fields before validating Version / HdrLen / CRC, so
	// callers receive a populated (untrusted) header alongside any
	// validation error -- helpful for logging "what was actually on
	// disk" when something fails.
	h.Version = binary.LittleEndian.Uint16(in[8:10])
	h.Kind = Kind(binary.LittleEndian.Uint32(in[10:14]))
	h.TimeNs = int64(binary.LittleEndian.Uint64(in[14:22]))
	copy(h.NodeName[:], in[22:38])
	h.TID = int32(binary.LittleEndian.Uint32(in[38:42]))
	h.WorkerID = int32(binary.LittleEndian.Uint32(in[42:46]))
	h.Tag = binary.LittleEndian.Uint32(in[46:50])
	h.Cycle = binary.LittleEndian.Uint64(in[50:58])
	h.Offset = binary.LittleEndian.Uint64(in[58:66])
	h.BodyLen = binary.LittleEndian.Uint64(in[66:74])
	h.Seed = binary.LittleEndian.Uint64(in[74:82])
	h.BodyCRC = binary.LittleEndian.Uint32(in[82:86])

	// CRC before Version, deliberately: VerdictHeadBadFormat's whole point is
	// "the header is internally consistent, it's just a different version
	// than this verifier expects" -- a claim that requires the CRC to have
	// actually passed. Checking Version first would let ordinary bit-rot
	// landing on the Version byte get misclassified as benign version skew
	// instead of corruption, since ErrBadVersion would return before the CRC
	// was ever computed.
	wantCRC := binary.LittleEndian.Uint32(in[86:90])
	gotCRC := crc32.Checksum(in[:86], castagnoli)
	if wantCRC != gotCRC {
		return h, ErrBadHeadCRC
	}
	if h.Version != HeaderVersion {
		return h, fmt.Errorf("%w: got %d, want %d", ErrBadVersion, h.Version, HeaderVersion)
	}
	return h, nil
}

// GenerateBody fills out with deterministic bytes derived from
// (kind, seed). Two calls with the same (kind, seed, len(out)) always
// produce the same bytes, which is the invariant verification relies
// on.
func GenerateBody(kind Kind, seed uint64, out []byte) error {
	switch kind {
	case KindPRNG:
		// math/rand/v2 PCG is specified -- stable across Go versions --
		// and fast enough that even 4 KiB bodies are cheap to fill or
		// verify. Derive the two 64-bit PCG state words deterministically
		// from seed.
		src := mrand.NewPCG(seed, ^seed)
		r := mrand.New(src)
		i := 0
		for ; i+8 <= len(out); i += 8 {
			binary.LittleEndian.PutUint64(out[i:i+8], r.Uint64())
		}
		if i < len(out) {
			var tail [8]byte
			binary.LittleEndian.PutUint64(tail[:], r.Uint64())
			copy(out[i:], tail[:len(out)-i])
		}
	case KindRepeat:
		// Body = the 8-byte little-endian encoding of seed, repeated.
		var pat [8]byte
		binary.LittleEndian.PutUint64(pat[:], seed)
		for i := range out {
			out[i] = pat[i%8]
		}
	case KindCountUp:
		for i := range out {
			out[i] = byte(seed) + byte(i)
		}
	case KindZeros:
		for i := range out {
			out[i] = 0
		}
	case KindOnes:
		for i := range out {
			out[i] = 0xFF
		}
	case KindDecimal:
		// Body is a stream of fixed-width "NNN " tokens (3 zero-padded
		// decimal digits + one space, 4 bytes each). The starting number
		// is seed % wrap; each subsequent token is the next integer mod
		// wrap. Fixed width means the byte offset uniquely determines
		// the expected token, which makes corruption obvious to the eye
		// in `less` and trivial for the verifier to localize.
		const tokenLen = 4
		const wrap = 512
		start := uint32(seed % wrap)
		var tok [tokenLen]byte
		tok[3] = ' '
		var tokenIdx uint32
		for i := 0; i < len(out); {
			n := (start + tokenIdx) % wrap
			tok[0] = byte('0' + (n/100)%10)
			tok[1] = byte('0' + (n/10)%10)
			tok[2] = byte('0' + n%10)
			remaining := len(out) - i
			if remaining >= tokenLen {
				copy(out[i:i+tokenLen], tok[:])
				i += tokenLen
			} else {
				copy(out[i:], tok[:remaining])
				i += remaining
			}
			tokenIdx++
		}
	default:
		return fmt.Errorf("GenerateBody: unknown kind %v", kind)
	}
	return nil
}

// StripeSize is the granularity of per-block integrity checking. The body
// is divided into StripeSize-byte stripes; a 4-byte CRC32C checksum for
// each stripe is appended after the body.
const StripeSize = 512

// NumStripes returns the number of StripeSize-byte stripes covering a body
// of bodyLen bytes.
func NumStripes(bodyLen int) int {
	return (bodyLen + StripeSize - 1) / StripeSize
}

// BlockDataSize returns the total on-disk byte count for a block with the
// given body length: body bytes + one 4-byte CRC32C per stripe.
func BlockDataSize(bodyLen int) int {
	return bodyLen + NumStripes(bodyLen)*4
}

// BodyLen returns the body length for a block of the given total on-disk size.
// The on-disk layout is [body][stripe CRCs], so blockSize = bodyLen + NumStripes(bodyLen)*4.
// Not all values are achievable; powers of two (512, 1024, 4096, ...) and multiples
// of StripeSize+4 (516, 1032, ...) always work.
func BodyLen(blockSize int) (int, error) {
	if blockSize <= 0 {
		return 0, fmt.Errorf("block.BodyLen: blockSize must be > 0, got %d", blockSize)
	}
	k := (blockSize + StripeSize + 3) / (StripeSize + 4)
	bodyLen := blockSize - 4*k
	if bodyLen <= 0 || BlockDataSize(bodyLen) != blockSize {
		return 0, fmt.Errorf("block.BodyLen: %d cannot be partitioned into body+stripe-CRCs; use a power of two or a multiple of %d", blockSize, StripeSize+4)
	}
	return bodyLen, nil
}

// MakeBlock fills buf with one complete on-disk block: body data followed
// by per-stripe CRC32C checksums. buf must have length >= BlockDataSize(bodyLen).
//
// The header is NOT written into buf. Instead, MakeBlock populates h with
// Version, Kind, Seed, BodyLen, and BodyCRC so the caller can marshal h
// into an xattr via MarshalHeader.
func MakeBlock(buf []byte, h *Header, kind Kind, seed uint64, bodyLen int) error {
	if bodyLen < 0 {
		return fmt.Errorf("MakeBlock: negative bodyLen %d", bodyLen)
	}
	needed := BlockDataSize(bodyLen)
	if len(buf) < needed {
		return fmt.Errorf("MakeBlock: buf too small (%d < %d)", len(buf), needed)
	}

	body := buf[:bodyLen]
	if err := GenerateBody(kind, seed, body); err != nil {
		return err
	}

	n := NumStripes(bodyLen)
	crcSlice := buf[bodyLen : bodyLen+n*4]
	for i := 0; i < n; i++ {
		start := i * StripeSize
		end := start + StripeSize
		if end > bodyLen {
			end = bodyLen
		}
		c := crc32.Checksum(body[start:end], castagnoli)
		binary.LittleEndian.PutUint32(crcSlice[i*4:(i+1)*4], c)
	}

	h.Version = HeaderVersion
	h.Kind = kind
	h.Seed = seed
	h.BodyLen = uint64(bodyLen)
	h.BodyCRC = crc32.Checksum(body, castagnoli)
	return nil
}

// Verdict classifies the outcome of verifying one record.
type Verdict int

const (
	VerdictOK              Verdict = iota // body consistent with stored header, seed regeneration matches
	VerdictBodyCorrupt                    // stripe CRCs pass but body bytes differ from regenerated
	VerdictHeadBadCRC                     // xattr header CRC failed (from UnmarshalHeader)
	VerdictHeadBadMagic                   // xattr header has no verifyio magic (from UnmarshalHeader)
	VerdictHeadBadFormat                  // xattr header magic/CRC OK but version disagrees
	VerdictBodyCRCMismatch                // a stripe CRC does not match the on-disk bytes
	VerdictTruncated                      // buffer shorter than BlockDataSize(h.BodyLen)
)

// String returns a short human name for v.
func (v Verdict) String() string {
	switch v {
	case VerdictOK:
		return "OK"
	case VerdictBodyCorrupt:
		return "BODY_CORRUPT"
	case VerdictHeadBadCRC:
		return "HEAD_BAD_CRC"
	case VerdictHeadBadMagic:
		return "HEAD_BAD_MAGIC"
	case VerdictHeadBadFormat:
		return "HEAD_BAD_FORMAT"
	case VerdictBodyCRCMismatch:
		return "BODY_CRC_MISMATCH"
	case VerdictTruncated:
		return "TRUNCATED"
	default:
		return fmt.Sprintf("verdict(%d)", int(v))
	}
}

// Explanation returns a one-sentence, human-readable description of what v
// means. It exists because the short String() code is not self-explanatory —
// in particular that BODY_CORRUPT is valid-but-wrong-version data (typically a
// stale cross-node read), which is a different failure than the internal
// inconsistency of BODY_CRC_MISMATCH (torn write / bit-rot).
func (v Verdict) Explanation() string {
	switch v {
	case VerdictOK:
		return "body matches the current header and its regenerated seed data."
	case VerdictBodyCorrupt:
		return "the block is internally consistent (its own in-data stripe CRCs pass) " +
			"but does not match the version its header describes. The header is stored in " +
			"an xattr, separate from the data body, so here the (freshly read) xattr header " +
			"and the data body disagree: valid data, wrong version. Across nodes this is the " +
			"classic stale-read signature — the xattr header has advanced to a newer write " +
			"while the reader still serves an older, coherent copy of the data body from its " +
			"page cache."
	case VerdictBodyCRCMismatch:
		return "the body does not match its own in-data stripe CRCs: the block is " +
			"internally inconsistent, indicating a torn/partial write or on-media bit-rot " +
			"(genuine corruption, not merely stale)."
	case VerdictHeadBadCRC:
		return "the header's own CRC check failed: the stored header bytes are damaged."
	case VerdictHeadBadMagic:
		return "no valid block header was found (magic missing or zeroed): the region was " +
			"never written, or its xattr record is absent."
	case VerdictHeadBadFormat:
		return "the header's version/length is unrecognized: a format mismatch between the " +
			"writer and this verifier."
	case VerdictTruncated:
		return "the on-disk block is shorter than its header claims: an incomplete write or " +
			"a truncated file."
	default:
		return "unknown verdict."
	}
}

// VerifyBlock checks the on-disk block in buf against h, which must be the
// header previously stored in an xattr (via MarshalHeader). buf must contain
// exactly BlockDataSize(h.BodyLen) bytes.
//
// Verification proceeds in two stages:
//  1. Each 512-byte stripe's CRC32C is checked against the appended CRC.
//     A mismatch returns VerdictBodyCRCMismatch immediately.
//  2. The body is regenerated from (h.Kind, h.Seed) and compared
//     byte-for-byte. A mismatch returns VerdictBodyCorrupt.
//
// scratch is an optional reusable buffer for the regenerated body; pass nil
// to allocate. It must be at least h.BodyLen bytes.
func VerifyBlock(buf []byte, h *Header, scratch []byte) (Verdict, error) {
	// Guard against a corrupt or untrusted header: a BodyLen larger than the
	// buffer can never be satisfied, and converting an out-of-range uint64 to
	// int below would overflow and panic when slicing the body. Treat it as a
	// truncated block rather than crashing the verifier.
	if h.BodyLen > uint64(len(buf)) {
		return VerdictTruncated, nil
	}
	needed := BlockDataSize(int(h.BodyLen))
	if len(buf) < needed {
		return VerdictTruncated, nil
	}

	body := buf[:h.BodyLen]
	n := NumStripes(int(h.BodyLen))
	crcSlice := buf[h.BodyLen : uint64(h.BodyLen)+uint64(n*4)]

	for i := 0; i < n; i++ {
		start := uint64(i) * StripeSize
		end := start + StripeSize
		if end > h.BodyLen {
			end = h.BodyLen
		}
		want := binary.LittleEndian.Uint32(crcSlice[i*4 : (i+1)*4])
		got := crc32.Checksum(body[start:end], castagnoli)
		if want != got {
			return VerdictBodyCRCMismatch, nil
		}
	}

	if scratch == nil || uint64(len(scratch)) < h.BodyLen {
		scratch = make([]byte, h.BodyLen)
	} else {
		scratch = scratch[:h.BodyLen]
	}
	if err := GenerateBody(h.Kind, h.Seed, scratch); err != nil {
		return VerdictBodyCorrupt, err
	}
	if !bytes.Equal(scratch, body) {
		return VerdictBodyCorrupt, nil
	}
	return VerdictOK, nil
}

// Diagnosis holds forensic detail about a block that failed verification. It is
// meant to be computed from the SAME buffer VerifyBlock inspected — never a
// re-read, which on a stale cache could pull different bytes and contradict the
// reported Verdict.
type Diagnosis struct {
	StripesTotal  int    // stripes covering the body
	StripesFailed int    // stripes whose in-data CRC disagrees with the read bytes
	ReadBodyCRC   uint32 // CRC32C over the body as read (compare against Header.BodyCRC)
	FirstDiffByte int    // first body byte differing from seed-regenerated data; -1 if none/unknown
	ExpByte       byte   // expected byte at FirstDiffByte (valid when FirstDiffByte >= 0)
	GotByte       byte   // actual byte at FirstDiffByte (valid when FirstDiffByte >= 0)
}

// Diagnose computes forensic detail for the block in buf against header h,
// mirroring VerifyBlock's checks but without short-circuiting. StripesFailed==0
// means the block is internally consistent — a coherent-but-wrong-version stale
// read rather than corruption. FirstDiffByte locates where the read body first
// diverges from what the header's seed should regenerate.
//
// buf must hold at least BlockDataSize(h.BodyLen) bytes (the same precondition
// as VerifyBlock); a short buffer yields a zero-value Diagnosis with
// FirstDiffByte == -1.
func Diagnose(buf []byte, h *Header) Diagnosis {
	d := Diagnosis{FirstDiffByte: -1}
	if h.BodyLen > uint64(len(buf)) || len(buf) < BlockDataSize(int(h.BodyLen)) {
		return d
	}
	body := buf[:h.BodyLen]
	d.ReadBodyCRC = crc32.Checksum(body, castagnoli)

	n := NumStripes(int(h.BodyLen))
	d.StripesTotal = n
	crcSlice := buf[h.BodyLen : uint64(h.BodyLen)+uint64(n*4)]
	for i := 0; i < n; i++ {
		start := uint64(i) * StripeSize
		end := start + StripeSize
		if end > h.BodyLen {
			end = h.BodyLen
		}
		want := binary.LittleEndian.Uint32(crcSlice[i*4 : (i+1)*4])
		got := crc32.Checksum(body[start:end], castagnoli)
		if want != got {
			d.StripesFailed++
		}
	}

	// Locate the first byte where the read body diverges from the data the
	// header's seed should regenerate. Best-effort: if regeneration errors
	// (unknown Kind), leave FirstDiffByte == -1.
	scratch := make([]byte, h.BodyLen)
	if err := GenerateBody(h.Kind, h.Seed, scratch); err == nil {
		for i := 0; i < len(body); i++ {
			if body[i] != scratch[i] {
				d.FirstDiffByte = i
				d.ExpByte = scratch[i]
				d.GotByte = body[i]
				break
			}
		}
	}
	return d
}
