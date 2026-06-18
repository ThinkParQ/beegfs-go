package index

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// buildTLV synthesizes a gufi_query --print-tlv stream so the parser can be
// exercised without depending on the real binary. Layout mirrors the
// upstream hex format in beegfs-index/src/print.c: per row an 8-hex row_len
// and 4-hex column count, then per column a type byte, a len_of_len byte,
// the column length in hex, and the raw value. No magic prefix, no newline.
func buildTLV(t *testing.T, rows [][]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	for _, cols := range rows {
		var body bytes.Buffer
		for _, v := range cols {
			vb := []byte(v)
			lenHex := strconv.FormatUint(uint64(len(vb)), 16)
			body.WriteByte('3')
			body.WriteByte(byte('0' + len(lenHex)))
			body.WriteString(lenHex)
			body.Write(vb)
		}
		rowLen := 12 + body.Len()
		fmt.Fprintf(&buf, "%08x", rowLen)
		fmt.Fprintf(&buf, "%04x", len(cols))
		buf.Write(body.Bytes())
	}
	return buf.Bytes()
}

// TestParseTLV_PreservesAdversarialBytes: every byte pattern that breaks the text+split path
// (delimiter, newline, NUL, non-UTF-8) must round-trip exactly through the TLV parser.
func TestParseTLV_PreservesAdversarialBytes(t *testing.T) {
	t.Parallel()
	want := [][]string{
		{"plain"},
		{"with|pipe"},
		{"with\nnewline"},
		{"with\x00null", "two|cols"},
		{""},
		{"unicode 文件_测试"},
		{"\x01\x02\x03\x7f\xff"},
	}
	stream := buildTLV(t, want)

	ch := make(chan []string, len(want))
	if err := parseTLV(context.Background(), bytes.NewReader(stream), ch); err != nil {
		t.Fatalf("parseTLV: %v", err)
	}
	close(ch)

	var got [][]string
	for row := range ch {
		got = append(got, row)
	}
	if len(got) != len(want) {
		t.Fatalf("row count: got %d want %d", len(got), len(want))
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("row %d col count: got %d want %d", i, len(got[i]), len(want[i]))
		}
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				t.Errorf("row %d col %d: got %q want %q", i, j, got[i][j], want[i][j])
			}
		}
	}
}

// TestParseTLV_EmptyStream: empty stdout is a clean empty result, not an error.
func TestParseTLV_EmptyStream(t *testing.T) {
	t.Parallel()
	ch := make(chan []string, 1)
	if err := parseTLV(context.Background(), bytes.NewReader(nil), ch); err != nil {
		t.Fatalf("empty stream should be tolerated, got: %v", err)
	}
	close(ch)
	if _, ok := <-ch; ok {
		t.Fatalf("no rows expected from empty stream")
	}
}

// TestParseTLV_BadPrefix: malformed prefix surfaces as explicit error, not silent drop.
func TestParseTLV_BadPrefix(t *testing.T) {
	t.Parallel()
	bad := bytes.Repeat([]byte{'X'}, 12)
	ch := make(chan []string, 1)
	err := parseTLV(context.Background(), bytes.NewReader(bad), ch)
	close(ch)
	if err == nil {
		t.Fatalf("expected error on malformed prefix")
	}
}

// TestParseTLV_TruncatedRow: row prefix promising columns but ending mid-row is truncation error.
func TestParseTLV_TruncatedRow(t *testing.T) {
	t.Parallel()
	full := buildTLV(t, [][]string{{"abcdef"}})
	ch := make(chan []string, 1)
	err := parseTLV(context.Background(), bytes.NewReader(full[:len(full)-2]), ch)
	close(ch)
	if err == nil {
		t.Fatalf("expected truncation error")
	}
}

// TestParseTLV_RowLenMismatch: a row header whose row_len disagrees with the
// serialized bytes must fail on that row instead of silently mis-framing the
// rows that follow it.
func TestParseTLV_RowLenMismatch(t *testing.T) {
	t.Parallel()

	corrupt := func(mutate func(stream []byte) []byte) error {
		stream := mutate(buildTLV(t, [][]string{{"abc"}, {"def"}}))
		ch := make(chan []string, 2)
		err := parseTLV(context.Background(), bytes.NewReader(stream), ch)
		close(ch)
		return err
	}

	t.Run("count-overruns-row_len", func(t *testing.T) {
		// Claim 2 columns while row_len only covers 1: the column loop must hit
		// the row boundary, not read into the next row.
		err := corrupt(func(s []byte) []byte {
			copy(s[8:12], "0002")
			return s
		})
		if err == nil {
			t.Fatal("expected error when column count overruns row_len")
		}
	})

	t.Run("row_len-exceeds-columns", func(t *testing.T) {
		// Inflate row_len past the bytes its columns consume: the excess must be
		// reported, not parsed as the next row's prefix.
		err := corrupt(func(s []byte) []byte {
			copy(s[:8], "00000014") // actual row is 0x12 bytes
			return s
		})
		if err == nil {
			t.Fatal("expected error when row_len exceeds the serialized columns")
		}
	})

	t.Run("row_len-below-prefix", func(t *testing.T) {
		err := corrupt(func(s []byte) []byte {
			copy(s[:8], "00000004")
			return s
		})
		if err == nil {
			t.Fatal("expected error when row_len is smaller than the prefix")
		}
	})
}

// TestLocalExecutor_WaitTearsDownAbandonedQuery: a caller that stops reading the
// rows channel and calls wait() must not deadlock; wait kills the child and
// unblocks the reader goroutine.
func TestLocalExecutor_WaitTearsDownAbandonedQuery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake_gufi_query")
	// Emits valid single-column TLV rows forever, ignoring its arguments, so the
	// reader goroutine ends up blocked on the channel send.
	script := "#!/bin/sh\nwhile :; do printf '000000100001311x'; done\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("writing fake gufi_query: %v", err)
	}

	e := &LocalExecutor{QueryBin: bin}
	rows, wait, err := e.Execute(context.Background(), QuerySpec{IndexRoot: dir})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Take a single row to prove the stream is live, then abandon the channel.
	select {
	case <-rows:
	case <-time.After(5 * time.Second):
		t.Fatal("no row produced within 5s")
	}

	done := make(chan error, 1)
	go func() { done <- wait() }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation error from wait() on abandoned query")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("wait() deadlocked on abandoned query")
	}
}
