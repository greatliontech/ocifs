package store

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Bookkeeping values are versioned binary records
// (docs/specs/store.md REQ-store-bookkeeping): every string field is
// length-prefixed raw bytes — names, link targets, xattr keys and
// values round-trip byte-exactly with no escaping layer — and
// integers are varints. A record's first byte is its format
// version; a foreign version is handled as the keyspace's
// absent-row case.

type binWriter struct{ buf []byte }

func (w *binWriter) u64(v uint64)   { w.buf = binary.AppendUvarint(w.buf, v) }
func (w *binWriter) i64(v int64)    { w.buf = binary.AppendVarint(w.buf, v) }
func (w *binWriter) byteVal(b byte) { w.buf = append(w.buf, b) }
func (w *binWriter) str(s string) {
	w.u64(uint64(len(s)))
	w.buf = append(w.buf, s...)
}

var errBinTruncated = errors.New("truncated bookkeeping record")

type binReader struct{ buf []byte }

func (r *binReader) u64() (uint64, error) {
	v, n := binary.Uvarint(r.buf)
	if n <= 0 {
		return 0, errBinTruncated
	}
	r.buf = r.buf[n:]
	return v, nil
}

func (r *binReader) i64() (int64, error) {
	v, n := binary.Varint(r.buf)
	if n <= 0 {
		return 0, errBinTruncated
	}
	r.buf = r.buf[n:]
	return v, nil
}

func (r *binReader) byteVal() (byte, error) {
	if len(r.buf) < 1 {
		return 0, errBinTruncated
	}
	b := r.buf[0]
	r.buf = r.buf[1:]
	return b, nil
}

func (r *binReader) str() (string, error) {
	n, err := r.u64()
	if err != nil {
		return "", err
	}
	if uint64(len(r.buf)) < n {
		return "", errBinTruncated
	}
	s := string(r.buf[:n])
	r.buf = r.buf[n:]
	return s, nil
}

func (r *binReader) done() error {
	if len(r.buf) != 0 {
		return fmt.Errorf("bookkeeping record carries %d trailing bytes", len(r.buf))
	}
	return nil
}
