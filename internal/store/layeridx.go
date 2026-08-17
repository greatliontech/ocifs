package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/greatliontech/gmdb"

	"github.com/greatliontech/ocifs/internal/layer"
)

// The layeridx keyspace (docs/specs/store.md REQ-store-bookkeeping)
// holds one record per unpacked layer, keyed by
// <algorithm> 0x00 <hex> of the layer digest the manifest lists:
// the layer's entries in tar order — the tar header's metadata
// fields plus, for regular files, the content-CAS key. Every header
// string persists as raw bytes. The record is a regenerable cache:
// a missing or foreign-versioned row heals by re-unpacking the
// retained blob (REQ-store-self-heal).

const layerIdxVersion = 1

func layerKey(d v1.Hash) []byte {
	k := make([]byte, 0, len(d.Algorithm)+1+len(d.Hex))
	k = append(k, d.Algorithm...)
	k = append(k, 0x00)
	k = append(k, d.Hex...)
	return k
}

func encodeLayer(l layer.Layer) []byte {
	w := &binWriter{}
	w.byteVal(layerIdxVersion)
	w.u64(uint64(len(l)))
	for i := range l {
		h := &l[i].Header
		w.byteVal(h.Typeflag)
		w.str(h.Name)
		w.str(h.Linkname)
		w.i64(h.Mode)
		w.i64(int64(h.Uid))
		w.i64(int64(h.Gid))
		w.str(h.Uname)
		w.str(h.Gname)
		w.i64(h.Size)
		w.i64(h.ModTime.Unix())
		w.i64(int64(h.ModTime.Nanosecond()))
		w.i64(h.AccessTime.Unix())
		w.i64(int64(h.AccessTime.Nanosecond()))
		w.i64(h.ChangeTime.Unix())
		w.i64(int64(h.ChangeTime.Nanosecond()))
		w.i64(h.Devmajor)
		w.i64(h.Devminor)
		writeStringMap(w, h.PAXRecords)
		writeStringMap(w, h.Xattrs) //nolint:staticcheck // legacy producers round-trip
		w.str(l[i].Digest.Algorithm)
		w.str(l[i].Digest.Hex)
	}
	return w.buf
}

func decodeLayer(data []byte) (layer.Layer, error) {
	r := &binReader{buf: data}
	ver, err := r.byteVal()
	if err != nil {
		return nil, err
	}
	if ver != layerIdxVersion {
		// Foreign version: unparseable state healing as an absent
		// index (REQ-store-self-heal).
		return nil, fmt.Errorf("layer index version %d (want %d): %w", ver, layerIdxVersion, os.ErrNotExist)
	}
	n, err := r.u64()
	if err != nil {
		return nil, err
	}
	// A corrupt count must error, never allocate: each encoded entry
	// costs at least 21 bytes (one typeflag byte, twelve varints,
	// six string length prefixes, two map counts), so a count the
	// remaining bytes cannot hold is truncation
	// (REQ-store-self-heal turns it into a re-unpack).
	if n > uint64(len(r.buf))/21 {
		return nil, errBinTruncated
	}
	l := make(layer.Layer, 0, n)
	for i := uint64(0); i < n; i++ {
		var e layer.Entry
		h := &e.Header
		if h.Typeflag, err = r.byteVal(); err != nil {
			return nil, err
		}
		if h.Name, err = r.str(); err != nil {
			return nil, err
		}
		if h.Linkname, err = r.str(); err != nil {
			return nil, err
		}
		if h.Mode, err = r.i64(); err != nil {
			return nil, err
		}
		var uid, gid int64
		if uid, err = r.i64(); err != nil {
			return nil, err
		}
		if gid, err = r.i64(); err != nil {
			return nil, err
		}
		h.Uid, h.Gid = int(uid), int(gid)
		if h.Uname, err = r.str(); err != nil {
			return nil, err
		}
		if h.Gname, err = r.str(); err != nil {
			return nil, err
		}
		if h.Size, err = r.i64(); err != nil {
			return nil, err
		}
		if h.ModTime, err = readTime(r); err != nil {
			return nil, err
		}
		if h.AccessTime, err = readTime(r); err != nil {
			return nil, err
		}
		if h.ChangeTime, err = readTime(r); err != nil {
			return nil, err
		}
		if h.Devmajor, err = r.i64(); err != nil {
			return nil, err
		}
		if h.Devminor, err = r.i64(); err != nil {
			return nil, err
		}
		if h.PAXRecords, err = readStringMap(r); err != nil {
			return nil, err
		}
		xattrs, err := readStringMap(r)
		if err != nil {
			return nil, err
		}
		h.Xattrs = xattrs //nolint:staticcheck // legacy producers round-trip
		algo, err := r.str()
		if err != nil {
			return nil, err
		}
		hex, err := r.str()
		if err != nil {
			return nil, err
		}
		e.Digest = v1.Hash{Algorithm: algo, Hex: hex}
		l = append(l, e)
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return l, nil
}

func writeStringMap(w *binWriter, m map[string]string) {
	w.u64(uint64(len(m)))
	// Deterministic order is not needed for round-trip correctness,
	// but map iteration is fine either way: the decoder rebuilds the
	// map, and equality is by content.
	for k, v := range m {
		w.str(k)
		w.str(v)
	}
}

func readStringMap(r *binReader) (map[string]string, error) {
	n, err := r.u64()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	m := make(map[string]string, n)
	for i := uint64(0); i < n; i++ {
		k, err := r.str()
		if err != nil {
			return nil, err
		}
		v, err := r.str()
		if err != nil {
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}

func readTime(r *binReader) (time.Time, error) {
	sec, err := r.i64()
	if err != nil {
		return time.Time{}, err
	}
	nsec, err := r.i64()
	if err != nil {
		return time.Time{}, err
	}
	if sec == zeroTimeSec && nsec == 0 {
		return time.Time{}, nil
	}
	return time.Unix(sec, nsec), nil
}

// zeroTimeSec is time.Time{}.Unix() — the sentinel a zero ModTime
// round-trips through (tar headers legitimately carry zero times).
var zeroTimeSec = time.Time{}.Unix()

// LayerIdxGet loads the index for layerDigest; a missing row is
// os.ErrNotExist for the caller's heal path, exactly as a foreign
// version.
func (b *bookkeeping) LayerIdxGet(ctx context.Context, layerDigest v1.Hash) (layer.Layer, error) {
	var l layer.Layer
	err := b.db.View(ctx, func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksLayerIdx)
		if err != nil {
			return err
		}
		v, err := ks.Get(layerKey(layerDigest))
		if errors.Is(err, gmdb.ErrNotFound) {
			return fmt.Errorf("layer index %s: %w", layerDigest, os.ErrNotExist)
		}
		if err != nil {
			return err
		}
		// Decode inside the transaction: the value is borrowed.
		l, err = decodeLayer(v)
		if err != nil {
			return fmt.Errorf("layer index %s: %w", layerDigest, err)
		}
		return nil
	})
	return l, err
}

// LayerIdxPut publishes the index for layerDigest.
func (b *bookkeeping) LayerIdxPut(ctx context.Context, layerDigest v1.Hash, l layer.Layer) error {
	return b.db.Update(ctx, func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksLayerIdx)
		if err != nil {
			return err
		}
		return ks.Put(layerKey(layerDigest), encodeLayer(l))
	})
}
