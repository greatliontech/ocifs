package store

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"

	"github.com/greatliontech/gmdb"
	"github.com/greatliontech/gmdb/oslock"

	"github.com/greatliontech/ocifs/internal/projection"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

// --- network-free harness: in-process registry + crafted layers ---

// testHost is the registry host every test reference names; requests
// to it never touch a socket — they are dispatched straight to an
// in-process registry handler through handlerTransport.
const testHost = "registry.invalid"

// handlerTransport serves HTTP round trips by invoking a handler
// in-process. No listener, no port, no /proc network state: the
// harness stays deterministic for observation-based tooling.
type handlerTransport struct {
	h http.Handler
}

func (ht handlerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Client requests may carry a nil Body; server handlers are
	// entitled to a non-nil one (net/http normalizes this on a real
	// wire).
	if req.Body == nil {
		req = req.Clone(req.Context())
		req.Body = http.NoBody
	}
	rec := httptest.NewRecorder()
	ht.h.ServeHTTP(rec, req)
	resp := rec.Result()
	resp.Request = req
	return resp, nil
}

func newTestRegistry() handlerTransport {
	return handlerTransport{h: registry.New(registry.Logger(log.New(io.Discard, "", 0)))}
}

func scratchDir(t *testing.T) string {
	t.Helper()
	return scratchtest.Dir(t, "store")
}

// tarEntry describes one entry for tarBytes.
type tarEntry struct {
	hdr     tar.Header
	content []byte
}

func tfile(name, content string) tarEntry {
	return tarEntry{
		hdr:     tar.Header{Name: name, Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(content))},
		content: []byte(content),
	}
}

func tdir(name string) tarEntry {
	return tarEntry{hdr: tar.Header{Name: name, Typeflag: tar.TypeDir, Mode: 0o755}}
}

func tsymlink(name, target string) tarEntry {
	return tarEntry{hdr: tar.Header{Name: name, Typeflag: tar.TypeSymlink, Linkname: target, Mode: 0o777}}
}

func thardlink(name, target string) tarEntry {
	return tarEntry{hdr: tar.Header{Name: name, Typeflag: tar.TypeLink, Linkname: target}}
}

func tarBytes(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		if err := tw.WriteHeader(&e.hdr); err != nil {
			t.Fatal(err)
		}
		if len(e.content) > 0 {
			if _, err := tw.Write(e.content); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// rawLayer is a v1.Layer over explicit compressed bytes, so tests
// control the exact blob a registry serves — including blobs whose
// decompressed stream is not a valid tar.
type rawLayer struct {
	compressed []byte
	diffID     v1.Hash
}

func newRawLayer(t *testing.T, uncompressed []byte) *rawLayer {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(uncompressed); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(uncompressed)
	return &rawLayer{
		compressed: buf.Bytes(),
		diffID:     v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])},
	}
}

func (l *rawLayer) Digest() (v1.Hash, error) {
	sum := sha256.Sum256(l.compressed)
	return v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}, nil
}
func (l *rawLayer) DiffID() (v1.Hash, error) { return l.diffID, nil }
func (l *rawLayer) Compressed() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(l.compressed)), nil
}
func (l *rawLayer) Uncompressed() (io.ReadCloser, error) {
	zr, err := gzip.NewReader(bytes.NewReader(l.compressed))
	if err != nil {
		return nil, err
	}
	return zr, nil
}
func (l *rawLayer) Size() (int64, error)                { return int64(len(l.compressed)), nil }
func (l *rawLayer) MediaType() (types.MediaType, error) { return types.DockerLayer, nil }

func makeImage(t *testing.T, layers ...v1.Layer) v1.Image {
	t.Helper()
	img, err := mutate.AppendLayers(empty.Image, layers...)
	if err != nil {
		t.Fatal(err)
	}
	return img
}

func push(t *testing.T, rt http.RoundTripper, refStr string, img v1.Image) {
	t.Helper()
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img, remote.WithTransport(rt)); err != nil {
		t.Fatal(err)
	}
}

// anonKeychain resolves everything anonymously: the in-process
// registry needs no credentials, and the ambient default keychain
// would read machine-local files (~/.docker/config.json).
type anonKeychain struct{}

func (anonKeychain) Resolve(authn.Resource) (authn.Authenticator, error) {
	return authn.Anonymous, nil
}

func newTestStore(t *testing.T, policy PullPolicy, rt http.RoundTripper) (*Store, string) {
	t.Helper()
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, policy, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.transport = rt
	return s, dir
}

func readEntry(t *testing.T, s *Store, img *Image, path string) []byte {
	t.Helper()
	view, err := img.Unify()
	if err != nil {
		t.Fatal(err)
	}
	e, ok := view.Lookup(path)
	if !ok {
		t.Fatalf("path %q not in view", path)
	}
	data, err := os.ReadFile(s.BlobPath(e.Digest))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func descriptorCount(t *testing.T, storeDir string) int {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(storeDir, "oci", "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var idx v1.IndexManifest
	if err := json.Unmarshal(data, &idx); err != nil {
		t.Fatal(err)
	}
	return len(idx.Manifests)
}

// refRows snapshots every refs row (key=value) through a read-only
// database handle — a concurrent reader like any inspecting process.
func refRows(t testing.TB, storeDir string) map[string]string {
	t.Helper()
	db, err := gmdb.Open(context.Background(), filepath.Join(storeDir, "bookkeeping", "db"), gmdb.Options{ReadOnly: true})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]string{}
		}
		t.Fatal(err)
	}
	defer db.Close()
	rows := map[string]string{}
	err = db.View(context.Background(), func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksRefs)
		if err != nil {
			return err
		}
		for k, v := range ks.All() {
			rows[string(k)] = string(v)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func refFiles(t *testing.T, storeDir string) []string {
	t.Helper()
	var keys []string
	for k := range refRows(t, storeDir) {
		keys = append(keys, k)
	}
	return keys
}

// deleteLayerIdxRow removes one layeridx row through a second
// read-write database handle — the damage self-heal recovers from.
func deleteLayerIdxRow(t testing.TB, storeDir string, ld v1.Hash) {
	t.Helper()
	db, err := gmdb.Open(context.Background(), filepath.Join(storeDir, "bookkeeping", "db"), gmdb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(context.Background(), func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksLayerIdx)
		if err != nil {
			return err
		}
		err = ks.Delete(layerKey(ld))
		if errors.Is(err, gmdb.ErrNotFound) {
			return nil
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// writeLayerIdxRow plants raw bytes at a layeridx key — the damaged
// or foreign-format state self-heal must not serve.
func writeLayerIdxRow(t testing.TB, storeDir string, ld v1.Hash, raw []byte) {
	t.Helper()
	db, err := gmdb.Open(context.Background(), filepath.Join(storeDir, "bookkeeping", "db"), gmdb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(context.Background(), func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksLayerIdx)
		if err != nil {
			return err
		}
		return ks.Put(layerKey(ld), raw)
	}); err != nil {
		t.Fatal(err)
	}
}

// clearRefRows deletes every refs row through a second read-write
// database handle — a concurrent writer like any other process.
func clearRefRows(t testing.TB, storeDir string) {
	t.Helper()
	db, err := gmdb.Open(context.Background(), filepath.Join(storeDir, "bookkeeping", "db"), gmdb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(context.Background(), func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksRefs)
		if err != nil {
			return err
		}
		var keys [][]byte
		for k := range ks.All() {
			keys = append(keys, append([]byte(nil), k...))
		}
		for _, k := range keys {
			if err := ks.Delete(k); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// --- tests ---

func TestIngestRoundtrip(t *testing.T) {
	rt := newTestRegistry()
	l1 := newRawLayer(t, tarBytes(t,
		tdir("etc"),
		tfile("etc/hosts", "127.0.0.1 localhost"),
		tfile("bin", "binary bits"),
		tsymlink("link", "etc/hosts"),
	))
	l2 := newRawLayer(t, tarBytes(t,
		tfile("etc/hosts", "overridden"),
		thardlink("hosts.hard", "etc/hosts"),
	))
	refStr := testHost + "/test/roundtrip:v1"
	push(t, rt, refStr, makeImage(t, l1, l2))

	s, dir := newTestStore(t, PullIfNotPresent, rt)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}

	if got := string(readEntry(t, s, img, "etc/hosts")); got != "overridden" {
		t.Fatalf("etc/hosts = %q", got)
	}
	if got := string(readEntry(t, s, img, "bin")); got != "binary bits" {
		t.Fatalf("bin = %q", got)
	}
	// The hardlink was resolved at placement over the same-layer
	// override.
	if got := string(readEntry(t, s, img, "hosts.hard")); got != "overridden" {
		t.Fatalf("hosts.hard = %q", got)
	}

	// Tier layout (REQ-store-layout): oci layout marker, disjoint
	// blobs/ and layers/ roots, ref recorded.
	for _, p := range []string{
		filepath.Join(dir, "oci", "oci-layout"),
		filepath.Join(dir, "oci", "index.json"),
		filepath.Join(dir, "blobs"),
		filepath.Join(dir, "bookkeeping"),
		filepath.Join(dir, "mounts"),
		filepath.Join(dir, "exports"),
	} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("missing tier path %s: %v", p, err)
		}
	}
	if n := len(refFiles(t, dir)); n != 1 {
		t.Fatalf("%d ref files, want 1", n)
	}

	// Layer indexes are keyed by the digests the manifest lists.
	for _, l := range []*rawLayer{l1, l2} {
		ld, _ := l.Digest()
		if _, err := s.bk.LayerIdxGet(context.Background(), ld); err != nil {
			t.Fatalf("missing layer index for %s: %v", ld, err)
		}
	}

	// Content blobs hash to their keys (REQ-store-cas-content).
	view, err := img.Unify()
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range view.Entries() {
		if e.Digest == (v1.Hash{}) {
			continue
		}
		data, err := os.ReadFile(s.BlobPath(e.Digest))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(data)
		if hex.EncodeToString(sum[:]) != e.Digest.Hex {
			t.Fatalf("blob %s does not hash to its key", e.Digest)
		}
	}
}

func TestIngestIdempotent(t *testing.T) {
	rt := newTestRegistry()
	refStr := testHost + "/test/idem:v1"
	push(t, rt, refStr, makeImage(t, newRawLayer(t, tarBytes(t, tfile("a", "aa")))))

	s, dir := newTestStore(t, PullIfNotPresent, rt)
	if _, err := s.Image(context.Background(), refStr, nil); err != nil {
		t.Fatal(err)
	}
	if n := descriptorCount(t, dir); n != 1 {
		t.Fatalf("%d descriptors after first ingest, want 1", n)
	}

	// Drop the ref so the second call re-runs the full ingest
	// against already-present content.
	clearRefRows(t, dir)
	if _, err := s.Image(context.Background(), refStr, nil); err != nil {
		t.Fatal(err)
	}
	if n := descriptorCount(t, dir); n != 1 {
		t.Fatalf("%d descriptors after re-ingest, want 1 (REQ-store-ingest-idempotent)", n)
	}
	if n := len(refFiles(t, dir)); n != 1 {
		t.Fatalf("%d ref files after re-ingest, want 1", n)
	}
}

func TestLayerIndexAndContentKeyspacesDisjoint(t *testing.T) {
	rt := newTestRegistry()

	// A layer whose compressed bytes also occur as a regular file
	// inside a higher layer: the same hex then names a layer index
	// and a content blob (REQ-store-ns).
	l1 := newRawLayer(t, tarBytes(t, tfile("seed", "seed content")))
	l2 := newRawLayer(t, tarBytes(t, tarEntry{
		hdr:     tar.Header{Name: "embedded.tgz", Typeflag: tar.TypeReg, Mode: 0o644, Size: int64(len(l1.compressed))},
		content: l1.compressed,
	}))
	refStr := testHost + "/test/collision:v1"
	push(t, rt, refStr, makeImage(t, l1, l2))

	s, dir := newTestStore(t, PullIfNotPresent, rt)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}

	ld, _ := l1.Digest() // == sha256 of embedded.tgz's content
	idx, err := s.bk.LayerIdxGet(context.Background(), ld)
	if err != nil || len(idx) == 0 {
		t.Fatalf("layer index missing at colliding key: %v", err)
	}
	blobPath := filepath.Join(dir, "blobs", ld.Algorithm, ld.Hex)
	blobData, err := os.ReadFile(blobPath)
	if err != nil {
		t.Fatalf("content blob missing at colliding key: %v", err)
	}
	if !bytes.Equal(blobData, l1.compressed) {
		t.Fatalf("blob at colliding key is not the embedded content")
	}
	if got := readEntry(t, s, img, "embedded.tgz"); !bytes.Equal(got, l1.compressed) {
		t.Fatalf("embedded file bytes corrupted by keyspace collision")
	}
	if got := string(readEntry(t, s, img, "seed")); got != "seed content" {
		t.Fatalf("seed = %q", got)
	}
}

func TestRefWrittenLast(t *testing.T) {
	rt := newTestRegistry()

	// A layer whose decompressed stream is not a tar archive:
	// append succeeds (the blob's digest is fine), unpack fails.
	garbage := &rawLayer{compressed: gzipBytes(t, []byte("this is not a tar archive")), diffID: v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(bytes.Repeat([]byte{0xab}, 32))}}
	good := newRawLayer(t, tarBytes(t, tfile("ok", "fine")))
	refStr := testHost + "/test/reflast:v1"
	push(t, rt, refStr, makeImage(t, good, garbage))

	s, dir := newTestStore(t, PullIfNotPresent, rt)
	if _, err := s.Image(context.Background(), refStr, nil); err == nil {
		t.Fatal("ingest of an unparseable layer succeeded")
	}
	// The crash story (REQ-store-ingest-order): failure before
	// completion leaves no ref entry.
	if files := refFiles(t, dir); len(files) != 0 {
		t.Fatalf("ref written despite failed ingest: %v", files)
	}

	// The store is not poisoned: a good image ingests afterwards.
	goodRef := testHost + "/test/reflast-good:v1"
	push(t, rt, goodRef, makeImage(t, good))
	if _, err := s.Image(context.Background(), goodRef, nil); err != nil {
		t.Fatal(err)
	}
}

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestSelfHeal(t *testing.T) {
	rt := newTestRegistry()
	l := newRawLayer(t, tarBytes(t, tfile("data", "precious")))
	refStr := testHost + "/test/heal:v1"
	push(t, rt, refStr, makeImage(t, l))

	s, dir := newTestStore(t, PullIfNotPresent, rt)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	blobKey := func() v1.Hash {
		view, err := img.Unify()
		if err != nil {
			t.Fatal(err)
		}
		e, ok := view.Lookup("data")
		if !ok {
			t.Fatal("data not in view")
		}
		return e.Digest
	}()
	ld, _ := l.Digest()

	// Every heal below must be network-free: cut the transport so
	// any registry round trip fails loudly.
	s.transport = handlerTransport{h: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("self-heal hit the network: %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusServiceUnavailable)
	})}

	// 1. Missing index.
	deleteLayerIdxRow(t, dir, ld)
	img, err = s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatalf("heal of missing index: %v", err)
	}
	if got := string(readEntry(t, s, img, "data")); got != "precious" {
		t.Fatalf("data = %q after index heal", got)
	}

	// 2. Corrupt index: a foreign-version row is the corrupt form a
	// versioned record admits.
	writeLayerIdxRow(t, dir, ld, []byte{layerIdxVersion + 1, 0xde, 0xad})
	img, err = s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatalf("heal of corrupt index: %v", err)
	}
	if got := string(readEntry(t, s, img, "data")); got != "precious" {
		t.Fatalf("data = %q after corrupt-index heal", got)
	}

	// 3. Missing content blob.
	if err := os.Remove(filepath.Join(dir, "blobs", blobKey.Algorithm, blobKey.Hex)); err != nil {
		t.Fatal(err)
	}
	img, err = s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatalf("heal of missing blob: %v", err)
	}
	if got := string(readEntry(t, s, img, "data")); got != "precious" {
		t.Fatalf("data = %q after blob heal", got)
	}
}

func TestRelocatedStoreServesFully(t *testing.T) {
	rt := newTestRegistry()
	refStr := testHost + "/test/reloc:v1"
	push(t, rt, refStr, makeImage(t, newRawLayer(t, tarBytes(t,
		tdir("d"),
		tfile("d/f", "relocatable"),
	))))

	parent := scratchDir(t)
	oldDir := filepath.Join(parent, "old")
	if err := os.MkdirAll(oldDir, 0o755); err != nil {
		t.Fatal(err)
	}
	s1, err := NewStore(oldDir, anonKeychain{}, PullIfNotPresent, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	s1.transport = rt
	if _, err := s1.Image(context.Background(), refStr, nil); err != nil {
		t.Fatal(err)
	}

	// Move the whole store root; a layer index recording paths
	// (rather than CAS keys) would keep pointing into old/.
	newDir := filepath.Join(parent, "new")
	if err := os.Rename(oldDir, newDir); err != nil {
		t.Fatal(err)
	}

	s2, err := NewStore(newDir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	img, err := s2.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(readEntry(t, s2, img, "d/f")); got != "relocatable" {
		t.Fatalf("d/f = %q from relocated store", got)
	}
}

func TestPullNeverUncachedFailsWithoutNetwork(t *testing.T) {
	// Port 1 on localhost: any network attempt would error with a
	// connection failure, not the policy error asserted here.
	s, _ := newTestStore(t, PullNever, nil)
	_, err := s.Image(context.Background(), "127.0.0.1:1/test/absent:v1", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !bytes.Contains([]byte(err.Error()), []byte("pull policy is 'Never'")) {
		t.Fatalf("unexpected error (network attempted?): %v", err)
	}
}

func TestPullAlwaysRevalidates(t *testing.T) {
	rt := newTestRegistry()
	refStr := testHost + "/test/always:v1"
	push(t, rt, refStr, makeImage(t, newRawLayer(t, tarBytes(t, tfile("v", "one")))))

	s, _ := newTestStore(t, PullAlways, rt)
	img1, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(readEntry(t, s, img1, "v")); got != "one" {
		t.Fatalf("v = %q", got)
	}

	// Unchanged remote: HEAD matches, cached content used.
	img2, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if img2.Hash() != img1.Hash() {
		t.Fatalf("digest changed without remote change")
	}

	// Tag moved: Always must serve the new content.
	push(t, rt, refStr, makeImage(t, newRawLayer(t, tarBytes(t, tfile("v", "two")))))
	img3, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if img3.Hash() == img1.Hash() {
		t.Fatalf("stale digest served under PullAlways")
	}
	if got := string(readEntry(t, s, img3, "v")); got != "two" {
		t.Fatalf("v = %q after tag move", got)
	}
}

func TestConcurrentPullsSameRef(t *testing.T) {
	rt := newTestRegistry()
	refStr := testHost + "/test/conc:v1"
	push(t, rt, refStr, makeImage(t, newRawLayer(t, tarBytes(t, tfile("c", "concurrent")))))

	s, dir := newTestStore(t, PullIfNotPresent, rt)
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			img, err := s.Image(context.Background(), refStr, nil)
			if err == nil {
				if got := string(readEntry(t, s, img, "c")); got != "concurrent" {
					err = errors.New("wrong content: " + got)
				}
			}
			errs[i] = err
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := descriptorCount(t, dir); n != 1 {
		t.Fatalf("%d descriptors after concurrent pulls, want 1", n)
	}
}

func TestConcurrentPullsDistinctRefs(t *testing.T) {
	// Distinct images racing through ingest: without the ingest
	// mutex the index.json read-modify-write loses descriptors
	// (REQ-store-single-writer's in-process clause).
	rt := newTestRegistry()
	const n = 6
	refs := make([]string, n)
	for i := range refs {
		refs[i] = testHost + "/test/conc-distinct:v" + strconv.Itoa(i)
		push(t, rt, refs[i], makeImage(t, newRawLayer(t, tarBytes(t,
			tfile("id", strconv.Itoa(i)),
		))))
	}

	s, dir := newTestStore(t, PullIfNotPresent, rt)
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range refs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			img, err := s.Image(context.Background(), refs[i], nil)
			if err == nil {
				if got := string(readEntry(t, s, img, "id")); got != strconv.Itoa(i) {
					err = errors.New("wrong content: " + got)
				}
			}
			errs[i] = err
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := descriptorCount(t, dir); got != n {
		t.Fatalf("%d descriptors after %d distinct concurrent pulls (lost update)", got, n)
	}
}

func TestConcurrentInstancesOneRoot(t *testing.T) {
	// Two Store instances over one root are still one ingesting
	// process: the ingest lock is shared per root, so racing distinct
	// ingests cannot lose index.json descriptors.
	rt := newTestRegistry()
	const n = 6
	refs := make([]string, n)
	for i := range refs {
		refs[i] = testHost + "/test/conc-inst:v" + strconv.Itoa(i)
		push(t, rt, refs[i], makeImage(t, newRawLayer(t, tarBytes(t,
			tfile("id", strconv.Itoa(i)),
		))))
	}

	dir := scratchDir(t)
	stores := make([]*Store, 2)
	for i := range stores {
		s, err := NewStore(dir, anonKeychain{}, PullIfNotPresent, v1.Platform{}, nil, false, 0)
		if err != nil {
			t.Fatal(err)
		}
		s.transport = rt
		stores[i] = s
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range refs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := stores[i%len(stores)]
			img, err := s.Image(context.Background(), refs[i], nil)
			if err == nil {
				if got := string(readEntry(t, s, img, "id")); got != strconv.Itoa(i) {
					err = errors.New("wrong content: " + got)
				}
			}
			errs[i] = err
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := descriptorCount(t, dir); got != n {
		t.Fatalf("%d descriptors after cross-instance pulls, want %d", got, n)
	}
}

func TestWhiteoutAcrossLayersEndToEnd(t *testing.T) {
	rt := newTestRegistry()
	l1 := newRawLayer(t, tarBytes(t,
		tfile("keep", "kept"),
		tfile("gone", "deleted"),
	))
	l2 := newRawLayer(t, tarBytes(t,
		tarEntry{hdr: tar.Header{Name: ".wh.gone", Typeflag: tar.TypeReg, Mode: 0o644}},
	))
	refStr := testHost + "/test/whiteout:v1"
	push(t, rt, refStr, makeImage(t, l1, l2))

	s, _ := newTestStore(t, PullIfNotPresent, rt)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := img.Unify()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := view.Lookup("gone"); ok {
		t.Fatal("whited-out entry survived end to end")
	}
	if got := string(readEntry(t, s, img, "keep")); got != "kept" {
		t.Fatalf("keep = %q", got)
	}
}

// tamperTransport wraps a registry transport so a GET of the blob at
// digest ld serves genuine with one byte flipped at pos — same
// length as the original, so only digest verification (not a size
// check) can reject it.
func tamperTransport(inner handlerTransport, ld v1.Hash, pos int, genuine []byte) handlerTransport {
	return handlerTransport{h: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, ld.Hex) {
			tampered := bytes.Clone(genuine)
			tampered[pos] ^= 0xff
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(tampered)
			return
		}
		inner.h.ServeHTTP(w, r)
	})}
}

func TestTamperedBlobNotPersisted(t *testing.T) {
	l := newRawLayer(t, tarBytes(t, tfile("x", "trustworthy")))
	ld, err := l.Digest()
	if err != nil {
		t.Fatal(err)
	}

	// A registry that serves the wrong bytes for the layer's digest
	// on download while accepting the push untouched.
	rt := tamperTransport(newTestRegistry(), ld, len(l.compressed)/2, l.compressed)
	refStr := testHost + "/test/tampered:v1"
	push(t, rt, refStr, makeImage(t, l))

	s, dir := newTestStore(t, PullIfNotPresent, rt)
	if _, err := s.Image(context.Background(), refStr, nil); err == nil {
		t.Fatal("ingest of tampered content succeeded")
	}
	// Nothing failing verification was persisted
	// (REQ-store-ingest-verified), and no ref was recorded.
	if _, err := os.Stat(filepath.Join(dir, "oci", "blobs", ld.Algorithm, ld.Hex)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("tampered blob persisted in oci/: %v", err)
	}
	if files := refFiles(t, dir); len(files) != 0 {
		t.Fatalf("ref written despite failed verification: %v", files)
	}
}

func TestNewStoreWritesConformantEmptyIndex(t *testing.T) {
	// The OCI image-index schema requires `manifests` to be an
	// array; a fresh store must not persist `"manifests": null`.
	_, dir := newTestStore(t, PullIfNotPresent, nil)
	data, err := os.ReadFile(filepath.Join(dir, "oci", "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	m, ok := raw["manifests"]
	if !ok || strings.TrimSpace(string(m)) == "null" {
		t.Fatalf("manifests is %q, want an array", m)
	}
	if v, ok := raw["schemaVersion"]; !ok || strings.TrimSpace(string(v)) != "2" {
		t.Fatalf("schemaVersion = %q", raw["schemaVersion"])
	}
}

func TestCrashedFirstCreationHeals(t *testing.T) {
	// A crash between the two layout-file writes leaves the marker
	// without index.json; the next open must complete the layout
	// instead of wedging.
	dir := scratchDir(t)
	if err := os.MkdirAll(filepath.Join(dir, "oci"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "oci", "oci-layout"), []byte(`{"imageLayoutVersion": "1.0.0"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := NewStore(dir, anonKeychain{}, PullIfNotPresent, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n := descriptorCount(t, dir); n != 0 {
		t.Fatalf("healed index has %d descriptors, want 0", n)
	}
	// The healed store ingests normally.
	rt := newTestRegistry()
	refStr := testHost + "/test/healed-creation:v1"
	push(t, rt, refStr, makeImage(t, newRawLayer(t, tarBytes(t, tfile("ok", "fine")))))
	s.transport = rt
	if _, err := s.Image(context.Background(), refStr, nil); err != nil {
		t.Fatal(err)
	}
}

func TestPreLayoutStoreRejected(t *testing.T) {
	dir := scratchDir(t)
	// A pre-layout store: oci/index.json exists, oci-layout marker
	// does not.
	if err := os.MkdirAll(filepath.Join(dir, "oci"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "oci", "index.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := NewStore(dir, anonKeychain{}, PullIfNotPresent, v1.Platform{}, nil, false, 0)
	if !errors.Is(err, ErrPreLayoutStore) {
		t.Fatalf("err = %v, want ErrPreLayoutStore", err)
	}
}

func TestNoTemporariesAfterIngest(t *testing.T) {
	rt := newTestRegistry()
	refStr := testHost + "/test/tmp:v1"
	push(t, rt, refStr, makeImage(t, newRawLayer(t, tarBytes(t,
		tfile("a", "1"), tfile("b", "2"), tfile("c", "3"),
	))))

	s, dir := newTestStore(t, PullIfNotPresent, rt)
	if _, err := s.Image(context.Background(), refStr, nil); err != nil {
		t.Fatal(err)
	}
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && len(d.Name()) > 5 && d.Name()[:5] == ".tmp-" {
			t.Fatalf("leftover temporary: %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestLayerIndexBinaryRoundTrip pins the layeridx record encoding
// (REQ-store-bookkeeping): header strings are arbitrary bytes
// (binary xattr values like security.capability blobs, unusual
// names and link targets) and survive the persisted record
// byte-exactly; a foreign version heals like a missing row.
func TestLayerIndexBinaryRoundTrip(t *testing.T) {
	bk, err := openBookkeeping(scratchtest.Dir(t, "store"))
	if err != nil {
		t.Fatal(err)
	}
	defer bk.Close()
	capBlob := string([]byte{0x00, 0x00, 0x00, 0x03, 0x00, 0x20, 0xff, 0xfe, 0x00, 0x00, 0xe8, 0x03})
	in := layer.Layer{
		{Header: tar.Header{
			Typeflag: tar.TypeChar,
			Name:     "bin/cap\xffbin",
			Linkname: "tgt\xfe",
			Mode:     0o755,
			Uid:      12, Gid: 34,
			Uname:      "u\xf1name",
			Gname:      "g\xf2name",
			Size:       5,
			ModTime:    time.Date(2024, 1, 2, 3, 4, 5, 678, time.UTC),
			AccessTime: time.Date(2024, 2, 3, 4, 5, 6, 789, time.UTC),
			ChangeTime: time.Date(2024, 3, 4, 5, 6, 7, 891, time.UTC),
			Devmajor:   1, Devminor: 3,
			PAXRecords: map[string]string{
				"SCHILY.xattr.security.capability": capBlob,
				"SCHILY.xattr.user.\xf0odd":        "v\x00v",
			},
			Xattrs: map[string]string{"security.capability": capBlob}, //nolint:staticcheck
		}, Digest: v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("ab", 32)}},
	}
	ld := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("cd", 32)}
	if err := bk.LayerIdxPut(t.Context(), ld, in); err != nil {
		t.Fatal(err)
	}
	out, err := bk.LayerIdxGet(t.Context(), ld)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("entries %d", len(out))
	}
	g, w := out[0].Header, in[0].Header
	if g.Typeflag != w.Typeflag || g.Name != w.Name || g.Linkname != w.Linkname ||
		g.Uname != w.Uname || g.Gname != w.Gname ||
		g.PAXRecords["SCHILY.xattr.security.capability"] != capBlob ||
		g.PAXRecords["SCHILY.xattr.user.\xf0odd"] != "v\x00v" ||
		g.Xattrs["security.capability"] != capBlob { //nolint:staticcheck
		t.Fatalf("binary round trip mangled:\n got  %+v\n want %+v", g, w)
	}
	if !g.ModTime.Equal(w.ModTime) || !g.AccessTime.Equal(w.AccessTime) ||
		!g.ChangeTime.Equal(w.ChangeTime) ||
		g.Mode != w.Mode || g.Uid != w.Uid || g.Gid != w.Gid ||
		g.Size != w.Size || g.Devmajor != w.Devmajor || g.Devminor != w.Devminor {
		t.Fatalf("attrs mangled: %+v", g)
	}
	if out[0].Digest != in[0].Digest {
		t.Fatalf("digest mangled")
	}

	// A foreign-version record heals as missing — write one directly
	// through a second handle, as a future format would leave it.
	foreign := append([]byte{layerIdxVersion + 1}, encodeLayer(in)[1:]...)
	db2, err := gmdb.Open(context.Background(), filepath.Join(bk.dir, "db"), gmdb.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if err := db2.Update(context.Background(), func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksLayerIdx)
		if err != nil {
			return err
		}
		return ks.Put(layerKey(ld), foreign)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := bk.LayerIdxGet(t.Context(), ld); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("foreign version served: %v", err)
	}
	// A truncated record is unparseable, not a panic, and not served.
	if _, err := decodeLayer(encodeLayer(in)[:7]); err == nil {
		t.Fatal("truncated record decoded")
	}
}

// TestPreDatabaseStoreRejected pins REQ-store-adopt's refusal of the
// pre-database layout: a refs/ file tier is never adopted, migrated,
// or deleted.
func TestPreDatabaseStoreRejected(t *testing.T) {
	dir := scratchDir(t)
	if err := os.MkdirAll(filepath.Join(dir, "refs"), 0o755); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(dir, "refs", "old-row")
	if err := os.WriteFile(sentinel, []byte("sha256:deadbeef"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(dir, nil, PullNever, v1.Platform{}, nil, false, 0); !errors.Is(err, ErrPreDatabaseStore) {
		t.Fatalf("NewStore over a refs/ tier: %v, want ErrPreDatabaseStore", err)
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("refusal touched the old store's state: %v", err)
	}
}

// TestCloseLifecycle pins the Close counterpart of construction
// (api.md REQ-api-construction): Close is idempotent, and store
// operations after Close fail rather than hang or corrupt.
func TestCloseLifecycle(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if _, err := s.Image(context.Background(), "r.io/x:y", nil); err == nil {
		t.Fatal("Image after Close succeeded")
	}
}

// TestAdoptRefusalOrder pins the refusal precedence: a directory
// carrying BOTH pre-layout and pre-database signatures reports the
// older, more specific pre-layout condition.
func TestAdoptRefusalOrder(t *testing.T) {
	dir := scratchDir(t)
	// Pre-layout signature: index.json without the oci-layout marker.
	if err := os.MkdirAll(filepath.Join(dir, "oci"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "oci", "index.json"), []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "refs"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(dir, nil, PullNever, v1.Platform{}, nil, false, 0); !errors.Is(err, ErrPreLayoutStore) {
		t.Fatalf("both signatures: %v, want ErrPreLayoutStore first", err)
	}
}

// TestMountRecordRoundTrip pins the mounts-row encoding
// (REQ-store-bookkeeping, projection.md REQ-proj-report): report
// paths are exact bytes — including non-UTF-8 names no JSON
// encoding could carry — and every field survives; a foreign
// version heals as an absent row; an empty report decodes present.
func TestMountRecordRoundTrip(t *testing.T) {
	in := MountRecord{
		Owner:      LivenessIdentity{Pid: 42, StartTime: 987654, PidNS: "pid:[4026531836]", BootID: "boot-uuid"},
		Image:      v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("ef", 32)},
		UpperName:  "scratch-upper",
		Mountpoint: "/mnt/with\xffbyte",
		Report: projection.Report{Entries: []projection.ReportEntry{{
			Path:        "dir/bad\xf0\x00name",
			Disposition: projection.DispositionOmitted,
			Reason:      projection.ReasonNameUnrepresentable,
			Detail:      "detail with \x00 nul",
		}}},
		Published: true,
	}
	out, err := decodeMountRecord(encodeMountRecord(in))
	if err != nil {
		t.Fatal(err)
	}
	if out.Owner != in.Owner || out.Image != in.Image ||
		out.UpperName != in.UpperName || out.Mountpoint != in.Mountpoint ||
		out.Published != in.Published ||
		len(out.Report.Entries) != 1 || out.Report.Entries[0] != in.Report.Entries[0] {
		t.Fatalf("round trip mangled:\n got  %+v\n want %+v", out, in)
	}

	empty := MountRecord{Owner: in.Owner, Image: in.Image}
	out2, err := decodeMountRecord(encodeMountRecord(empty))
	if err != nil {
		t.Fatal(err)
	}
	if out2.Report.Entries == nil || len(out2.Report.Entries) != 0 {
		t.Fatalf("empty report decoded as %+v, want present empty entries", out2.Report)
	}
	if out2.Published {
		t.Fatal("zero-value record decoded as published")
	}

	foreign := append([]byte{mountRecVersion + 1}, encodeMountRecord(in)[1:]...)
	if _, err := decodeMountRecord(foreign); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("foreign version: %v", err)
	}
	if _, err := decodeMountRecord(encodeMountRecord(in)[:5]); err == nil {
		t.Fatal("truncated record decoded")
	}
}

// TestMountReportPublicationLifecycle pins the publication flag
// (REQ-store-bookkeeping): a fresh registration starts unpublished
// — a reader mid-mount must not take "no omissions" from a report
// that does not exist yet — publication marks it atomically with
// the report, republication is an idempotent set, and a fresh
// re-registration of the id starts unpublished again.
func TestMountReportPublicationLifecycle(t *testing.T) {
	s, _ := newTestStore(t, PullNever, nil)
	ctx := context.Background()
	img := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("ab", 32)}
	if err := s.bk.MountPut(ctx, "pub", newMountRecord(img, "", "/mp")); err != nil {
		t.Fatal(err)
	}
	rec, err := s.MountRecord(ctx, "pub")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Published {
		t.Fatal("fresh registration marked published")
	}
	rep := projection.Report{Entries: []projection.ReportEntry{}}
	if err := s.PublishMountReport(ctx, "pub", rep); err != nil {
		t.Fatal(err)
	}
	if rec, err = s.MountRecord(ctx, "pub"); err != nil || !rec.Published {
		t.Fatalf("published: rec=%+v err=%v", rec, err)
	}
	// Republication (accumulated residuals) keeps the flag set.
	if err := s.PublishMountReport(ctx, "pub", rep); err != nil {
		t.Fatal(err)
	}
	if rec, err = s.MountRecord(ctx, "pub"); err != nil || !rec.Published {
		t.Fatalf("republished: rec=%+v err=%v", rec, err)
	}
	// A remount is a fresh registration and publishes anew.
	if err := s.bk.MountPut(ctx, "pub", newMountRecord(img, "", "/mp")); err != nil {
		t.Fatal(err)
	}
	if rec, err = s.MountRecord(ctx, "pub"); err != nil || rec.Published {
		t.Fatalf("re-registration kept published: rec=%+v err=%v", rec, err)
	}
}

// TestArbitratedRegistrationOverDeadRowStartsUnpublished pins the
// fresh-registration arm on the production registration path
// (store.md REQ-store-bookkeeping): a remount of an id over a DEAD
// published row must not inherit the dead mount's report or its
// publication — the window between registration and publication
// would otherwise serve the previous mount's report as this
// mount's published clean one.
func TestArbitratedRegistrationOverDeadRowStartsUnpublished(t *testing.T) {
	s, _ := newTestStore(t, PullNever, nil)
	ctx := context.Background()
	img := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("cd", 32)}
	stale := MountRecord{
		Owner: deadIdentity(),
		Image: img,
		Report: projection.Report{Entries: []projection.ReportEntry{{
			Path:        "old/path",
			Disposition: projection.DispositionOmitted,
			Reason:      projection.ReasonNameUnrepresentable,
		}}},
		Published: true,
	}
	if err := s.bk.MountPut(ctx, "reuse", stale); err != nil {
		t.Fatal(err)
	}
	claim, err := s.RegisterMountRecordArbitrated(ctx, "reuse", img, "", "/mp", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(claim.release)
	rec, err := s.MountRecord(ctx, "reuse")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Published || len(rec.Report.Entries) != 0 {
		t.Fatalf("registration over dead row inherited report state: %+v", rec)
	}
}

// TestOldLayoutMountRowRejected pins the clean break's failure
// mode: mounts rows in the pre-publication-flag layout (same
// version byte, no flag byte) decode as errors in both common
// shapes — the empty report every registration writes and an
// entry-bearing published one. The decoder carries no layout
// discriminator, so rejection is per-shape misparse, not
// structural; wiping is the documented remedy for a pre-change
// store.
func TestOldLayoutMountRowRejected(t *testing.T) {
	oldPrefix := func() *binWriter {
		w := &binWriter{}
		w.byteVal(mountRecVersion)
		w.i64(42)
		w.u64(987654)
		w.str("pid:[4026531836]")
		w.str("boot-uuid")
		w.str("sha256")
		w.str(strings.Repeat("ab", 32))
		w.str("")
		w.str("/mnt/old")
		return w
	}
	empty := oldPrefix()
	empty.u64(0) // old layout: entry count directly after mountpoint
	if _, err := decodeMountRecord(empty.buf); err == nil {
		t.Fatal("old-layout empty-report row decoded as a valid record")
	}
	entry := oldPrefix()
	entry.u64(1)
	entry.str("old/path")
	entry.str(string(projection.DispositionOmitted))
	entry.str(string(projection.ReasonNameUnrepresentable))
	entry.str("")
	if _, err := decodeMountRecord(entry.buf); err == nil {
		t.Fatal("old-layout entry-bearing row decoded as a valid record")
	}
}

// TestCorruptCountRowsHealNotPanic pins the decoders' count bound:
// a row whose entry count exceeds what its bytes can hold errors —
// self-heal territory — and never allocates or panics.
func TestCorruptCountRowsHealNotPanic(t *testing.T) {
	huge := binary.AppendUvarint([]byte{layerIdxVersion}, 1<<62)
	if _, err := decodeLayer(huge); err == nil {
		t.Fatal("huge-count layer row decoded")
	}
	big := binary.AppendUvarint([]byte{layerIdxVersion}, 1<<30)
	if _, err := decodeLayer(big); err == nil {
		t.Fatal("large-count layer row decoded")
	}
	hugeRec := append(encodeMountRecord(MountRecord{})[:0], mountRecVersion)
	hugeRec = append(hugeRec, encodeMountRecord(MountRecord{})[1:]...)
	// Replace the trailing (zero) entry count with a huge one.
	hugeRec = append(hugeRec[:len(hugeRec)-1], binary.AppendUvarint(nil, 1<<62)...)
	if _, err := decodeMountRecord(hugeRec); err == nil {
		t.Fatal("huge-count mount record decoded")
	}

	// End to end: a count-corrupt layeridx row heals by re-unpack.
	reg := newTestRegistry()
	refStr := testHost + "/heal/count:v1"
	l := newRawLayer(t, tarBytes(t, tfile("data", "precious")))
	push(t, reg, refStr, makeImage(t, l))
	s, dir := newTestStore(t, PullIfNotPresent, reg)
	if _, err := s.Image(context.Background(), refStr, nil); err != nil {
		t.Fatal(err)
	}
	ld, _ := l.Digest()
	writeLayerIdxRow(t, dir, ld, huge)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatalf("heal of count-corrupt index: %v", err)
	}
	if got := string(readEntry(t, s, img, "data")); got != "precious" {
		t.Fatalf("data = %q after count-corrupt heal", got)
	}
}

// TestSelfIdentityCollected pins the liveness-identity collection
// (REQ-store-bookkeeping): a registered mount record carries this
// process's pid, a nonzero kernel start time, and the namespace and
// boot discriminators exactly as the kernel reports them.
func TestSelfIdentityCollected(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("identity discriminators are linux-collected; other platforms record pid only")
	}
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	h := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("aa", 32)}
	if err := s.bk.MountPut(context.Background(), "ident", newMountRecord(h, "", "/mnt/x")); err != nil {
		t.Fatal(err)
	}
	rec, err := s.MountRecord(context.Background(), "ident")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Owner.Pid != int64(os.Getpid()) {
		t.Fatalf("pid %d, want %d", rec.Owner.Pid, os.Getpid())
	}
	if rec.Owner.StartTime == 0 {
		t.Fatal("start time not collected")
	}
	wantNS, err := os.Readlink("/proc/self/ns/pid")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Owner.PidNS != wantNS {
		t.Fatalf("pidns %q, want %q", rec.Owner.PidNS, wantNS)
	}
	wantBoot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Owner.BootID != strings.TrimSpace(string(wantBoot)) {
		t.Fatalf("boot id %q, want %q", rec.Owner.BootID, strings.TrimSpace(string(wantBoot)))
	}
}

// deadIdentity fabricates an identity that is definitively dead on
// this system: our boot and namespace, a pid from the far end of
// the space with a start time no live process carries.
func deadIdentity() LivenessIdentity {
	self := selfIdentity()
	return LivenessIdentity{Pid: 1<<30 - 3, StartTime: 1, PidNS: self.PidNS, BootID: self.BootID}
}

// TestReclaimDeadMounts pins REQ-store-mount-registry's
// reclamation under held-lock liveness: rows with no held claim —
// this binary's dead rows AND foreign-namespace or foreign-version
// corpses, all equally judgeable by lock — reclaim with their
// state directories; a row whose claim is held stays untouched.
func TestReclaimDeadMounts(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	h := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("aa", 32)}

	// Dead row with state directory: a row with no held lock IS the
	// corpse — SIGKILL leaves exactly this.
	if _, _, err := s.NewMountState("deadmount"); err != nil {
		t.Fatal(err)
	}
	if err := s.bk.MountPut(context.Background(), "deadmount", MountRecord{
		Owner: deadIdentity(), Image: h,
		Mountpoint: filepath.Join(dir, "mounts", "deadmount", "mnt"),
	}); err != nil {
		t.Fatal(err)
	}
	// Live mount: registration holds the claim.
	claim, err := s.RegisterMountRecordArbitrated(context.Background(), "livemount", h, "", "/x", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(claim.release)
	// The formerly unjudgeable class: a same-boot foreign-namespace
	// corpse. Its lock is held by nobody — judged dead and
	// reclaimed, the leak this rework exists to close.
	foreign := deadIdentity()
	foreign.PidNS = "pid:[999999]"
	if err := s.bk.MountPut(context.Background(), "foreignmount", MountRecord{Owner: foreign, Image: h}); err != nil {
		t.Fatal(err)
	}

	reclaimed, err := s.ReclaimDeadMounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, id := range reclaimed {
		got[id] = true
	}
	if !got["deadmount"] || !got["foreignmount"] || got["livemount"] {
		t.Fatalf("reclaimed %v, want deadmount and foreignmount only", reclaimed)
	}
	if _, err := os.Stat(filepath.Join(dir, "mounts", "deadmount")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dead mount's state directory survived: %v", err)
	}
	if _, err := s.MountRecord(context.Background(), "deadmount"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dead row survived: %v", err)
	}
	if _, err := s.MountRecord(context.Background(), "livemount"); err != nil {
		t.Fatalf("live row reclaimed: %v", err)
	}
	// The claim files retired with their rows.
	if _, err := os.Stat(s.mountLockPath("deadmount")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dead mount's lock file survived reclamation: %v", err)
	}
}

// TestUpperArbitrationIgnoresDeadHolder pins the crash arm of the
// upper arbitration (REQ-writable-base-binding): a dead row naming
// the upper — no held upper lock — never blocks a new writable
// mount; the live holder's lock refuses a second one.
func TestUpperArbitrationIgnoresDeadHolder(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	h := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("bb", 32)}
	if err := s.bk.MountPut(context.Background(), "crashed", MountRecord{
		Owner: deadIdentity(), Image: h, UpperName: "shared-upper",
	}); err != nil {
		t.Fatal(err)
	}
	upClaim, err := s.ClaimUpper("shared-upper")
	if err != nil {
		t.Fatalf("dead holder blocked a new upper claim: %v", err)
	}
	claim, err := s.RegisterMountRecordArbitrated(context.Background(), "fresh", h, "shared-upper", "/y", upClaim)
	if err != nil {
		t.Fatalf("dead holder blocked a new writable mount: %v", err)
	}
	t.Cleanup(claim.release)
	// And the LIVE holder's upper lock refuses the next claim — at
	// ClaimUpper, BEFORE any bookkeeping write for the upper.
	if _, err := s.ClaimUpper("shared-upper"); err == nil {
		t.Fatal("live holder did not refuse a second writable mount")
	}
}

// TestSameIDLiveRowRefusedAtRegistration pins the mount-id claim
// lock as the same-id serialization point
// (REQ-store-mount-registry): a held claim refuses; a dead row —
// no held lock — is overwritten.
func TestSameIDLiveRowRefusedAtRegistration(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	h := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("cc", 32)}
	claim, err := s.RegisterMountRecordArbitrated(context.Background(), "dup", h, "", "/a", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(claim.release)
	if _, err := s.RegisterMountRecordArbitrated(context.Background(), "dup", h, "", "/b", nil); err == nil {
		t.Fatal("second registration of a live id succeeded")
	}
	if err := s.bk.MountPut(context.Background(), "dup2", MountRecord{Owner: deadIdentity(), Image: h}); err != nil {
		t.Fatal(err)
	}
	claim2, err := s.RegisterMountRecordArbitrated(context.Background(), "dup2", h, "", "/c", nil)
	if err != nil {
		t.Fatalf("dead same-id row blocked registration: %v", err)
	}
	claim2.release()
}

// TestReclaimNeverTouchesHeldClaims pins the verdict/action
// atomicity reclamation inherits from held-lock liveness: the
// try-acquisition IS both the death verdict and the claim, so a
// mount whose lock is held — a live serve, or a racing holder that
// revived the id — is skipped untouched, and a registration racing
// a holder refuses as in-use (REQ-store-mount-registry).
func TestReclaimNeverTouchesHeldClaims(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	h := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("dd", 32)}
	// A dead-looking row whose claim a concurrent actor holds — the
	// exact state a mid-reclamation sweep or revived id presents.
	if err := s.bk.MountPut(context.Background(), "held", MountRecord{Owner: deadIdentity(), Image: h}); err != nil {
		t.Fatal(err)
	}
	holder, err := oslock.TryAcquire(s.mountLockPath("held"))
	if err != nil {
		t.Fatal(err)
	}
	reclaimed, err := s.ReclaimDeadMounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range reclaimed {
		if id == "held" {
			t.Fatal("reclamation touched a held claim")
		}
	}
	if _, err := s.MountRecord(context.Background(), "held"); err != nil {
		t.Fatalf("held row gone: %v", err)
	}
	// A registration racing the holder refuses as in-use — nothing
	// can serve paths mid-reclamation.
	if _, err := s.RegisterMountRecordArbitrated(context.Background(), "held", h, "", "/r", nil); err == nil {
		t.Fatal("registration succeeded against a held claim")
	}
	// Released, the id reclaims normally.
	holder.Close()
	reclaimed, err = s.ReclaimDeadMounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, id := range reclaimed {
		found = found || id == "held"
	}
	if !found {
		t.Fatal("released dead claim not reclaimed")
	}
}

// TestRootSetJudgesByLock pins the mark's own liveness reads
// (REQ-store-gc-roots): a row with no held claim is no root — even
// when reclamation has not run — and a dead foreign-version row
// halts nothing; only a HELD foreign row does. The mark itself is
// exercised directly because Collect reclaims debris first and
// would mask a mark that trusts rows over locks.
func TestRootSetJudgesByLock(t *testing.T) {
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	deadImg := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("0a", 32)}
	liveImg := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("0b", 32)}
	if err := s.bk.MountPut(context.Background(), "deadrow", MountRecord{Owner: deadIdentity(), Image: deadImg}); err != nil {
		t.Fatal(err)
	}
	claim, err := s.RegisterMountRecordArbitrated(context.Background(), "liverow", liveImg, "", "/l", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(claim.release)
	foreign := append([]byte{mountRecVersion + 1}, encodeMountRecord(MountRecord{Owner: deadIdentity()})[1:]...)
	if err := s.bk.db.Update(context.Background(), func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksMounts)
		if err != nil {
			return err
		}
		return ks.Put([]byte("deadforeign"), foreign)
	}); err != nil {
		t.Fatal(err)
	}

	// Ops rows, same treatment: a live op's pins root, a dead op
	// row (no held lock) pins nothing, a LIVE foreign op row halts.
	deadPin := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("0c", 32)}
	livePin := v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("0d", 32)}
	if err := writeOpRow(t, dir, "export-deadop", OpRecord{Kind: opKindExport, Owner: deadIdentity(), Pins: []v1.Hash{deadPin}}); err != nil {
		t.Fatal(err)
	}
	opClaim, err := s.BeginOp(context.Background(), opKindExport, []v1.Hash{livePin}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.EndOp(context.Background(), opClaim) })

	roots, foreignRows, err := s.rootSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(foreignRows) != 0 {
		t.Fatalf("dead foreign row halted the mark: %v", foreignRows)
	}
	seen := map[v1.Hash]bool{}
	for _, r := range roots {
		seen[r] = true
	}
	if seen[deadImg] {
		t.Fatal("dead row's image rooted")
	}
	if !seen[liveImg] {
		t.Fatal("live claim's image not rooted")
	}
	if seen[deadPin] {
		t.Fatal("dead op's pin rooted")
	}
	if !seen[livePin] {
		t.Fatal("live op claim's pin not rooted")
	}

	// A LIVE foreign op row halts image-tier collection.
	foreignOp := encodeOpRecord(OpRecord{Kind: opKindExport, Owner: deadIdentity()})
	foreignOp[0] = opRecVersion + 1
	if err := writeRawOpRow(t, dir, "future-op", foreignOp); err != nil {
		t.Fatal(err)
	}
	holder, err := oslock.TryAcquire(s.opLockPath("future-op"))
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	_, foreignRows, err = s.rootSet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, id := range foreignRows {
		found = found || id == "future-op"
	}
	if !found {
		t.Fatalf("live foreign op row did not halt: %v", foreignRows)
	}
}

// opsRows snapshots the ops keyspace through a second handle.
// opsRowsRaw snapshots the ops keyspace without decoding — for
// foreign-version rows.
func opsRowsRaw(t testing.TB, storeDir string) map[string][]byte {
	t.Helper()
	db, err := gmdb.Open(context.Background(), filepath.Join(storeDir, "bookkeeping", "db"), gmdb.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows := map[string][]byte{}
	err = db.View(context.Background(), func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksOps)
		if err != nil {
			return err
		}
		for k, v := range ks.All() {
			rows[string(k)] = append([]byte(nil), v...)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func opsRows(t testing.TB, storeDir string) map[string]OpRecord {
	t.Helper()
	db, err := gmdb.Open(context.Background(), filepath.Join(storeDir, "bookkeeping", "db"), gmdb.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows := map[string]OpRecord{}
	err = db.View(context.Background(), func(rtx *gmdb.ReadTx) error {
		ks, err := rtx.OpenKeyspaceReadOnly(ksOps)
		if err != nil {
			return err
		}
		for k, v := range ks.All() {
			rec, derr := decodeOpRecord(v)
			if derr != nil {
				t.Fatalf("undecodable op row %q: %v", k, derr)
			}
			rows[string(k)] = rec
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

// TestIngestLease pins REQ-store-single-writer's lease as the held
// locks/ingest lock: a live holder excludes a second acquirer —
// distinct open file descriptions exclude in-process exactly as
// across processes — until release frees the next; the lease is
// not a row at all; a stranded unheld lock file never wedges the
// next acquisition (it is recreated-in-place by the acquire).
func TestIngestLease(t *testing.T) {
	dir := scratchDir(t)
	s1, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s1.Close() })
	s2, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s2.Close() })

	l1, err := s1.AcquireIngestLease(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	// The lease is the lock, not a row.
	if n := len(opsRows(t, dir)); n != 0 {
		t.Fatalf("lease acquisition wrote %d ops rows", n)
	}
	// A live holder excludes — the second handle is a distinct open
	// file description in the SAME process, the arm the old
	// slot-based design needed extra machinery for.
	short, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := s2.AcquireIngestLease(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second acquire under a live lease: %v", err)
	}
	if err := s1.ReleaseIngestLease(context.Background(), l1); err != nil {
		t.Fatal(err)
	}
	ctx5, cancel5 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel5()
	l2, err := s2.AcquireIngestLease(ctx5)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if err := s2.ReleaseIngestLease(context.Background(), l2); err != nil {
		t.Fatal(err)
	}

	// A stranded unheld lock file (a crashed holder's leftover —
	// the kernel released with the process) never wedges the next
	// acquisition.
	if l, err := oslock.TryAcquire(s1.ingestLockPath()); err != nil {
		t.Fatal(err)
	} else {
		l.Close()
	}
	l3, err := s1.AcquireIngestLease(ctx5)
	if err != nil {
		t.Fatalf("acquire over a stranded lock file: %v", err)
	}
	if err := s1.ReleaseIngestLease(context.Background(), l3); err != nil {
		t.Fatal(err)
	}
}

func writeRawOpRow(t testing.TB, storeDir, id string, raw []byte) error {
	t.Helper()
	db, err := gmdb.Open(context.Background(), filepath.Join(storeDir, "bookkeeping", "db"), gmdb.Options{})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(context.Background(), func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksOps)
		if err != nil {
			return err
		}
		return ks.Put([]byte(id), raw)
	})
}

func writeOpRow(t testing.TB, storeDir, id string, rec OpRecord) error {
	t.Helper()
	db, err := gmdb.Open(context.Background(), filepath.Join(storeDir, "bookkeeping", "db"), gmdb.Options{})
	if err != nil {
		return err
	}
	defer db.Close()
	return db.Update(context.Background(), func(tx *gmdb.Tx) error {
		ks, err := tx.OpenKeyspace(ksOps)
		if err != nil {
			return err
		}
		return ks.Put([]byte(id), encodeOpRecord(rec))
	})
}

// TestOpRowLifecycle pins the ops rows (REQ-store-gc-roots): a
// begun op's pins and temporaries are recorded; End removes it; a
// completed or failed export leaves no rows; the record codec
// round-trips and refuses foreign versions.
func TestOpRowLifecycle(t *testing.T) {
	in := OpRecord{
		Kind:  opKindExport,
		Owner: LivenessIdentity{Pid: 7, StartTime: 9, PidNS: "pid:[1]", BootID: "b"},
		Pins:  []v1.Hash{{Algorithm: "sha256", Hex: strings.Repeat("ab", 32)}},
		Temps: []string{"/some/.export-x", "/other/with\xffbyte"},
	}
	out, err := decodeOpRecord(encodeOpRecord(in))
	if err != nil {
		t.Fatal(err)
	}
	if out.Kind != in.Kind || out.Owner != in.Owner ||
		len(out.Pins) != 1 || out.Pins[0] != in.Pins[0] ||
		len(out.Temps) != 2 || out.Temps[0] != in.Temps[0] || out.Temps[1] != in.Temps[1] {
		t.Fatalf("op record round trip mangled: %+v", out)
	}
	foreign := append([]byte{opRecVersion + 1}, encodeOpRecord(in)[1:]...)
	if _, err := decodeOpRecord(foreign); err == nil {
		t.Fatal("foreign op version decoded")
	}

	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	claim, err := s.BeginOp(context.Background(), opKindExport, in.Pins, in.Temps)
	if err != nil {
		t.Fatal(err)
	}
	rows := opsRows(t, dir)
	rec, ok := rows[claim.ID]
	if !ok || len(rec.Pins) != 1 || rec.Pins[0] != in.Pins[0] || len(rec.Temps) != 2 {
		t.Fatalf("begun op row: %+v", rows)
	}
	// The op's liveness is its held claim lock (lock-before-row).
	if _, err := oslock.TryAcquire(s.opLockPath(claim.ID)); !errors.Is(err, oslock.ErrHeld) {
		t.Fatalf("begun op's claim not held: %v", err)
	}
	if err := s.EndOp(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if len(opsRows(t, dir)) != 0 {
		t.Fatalf("ops rows after EndOp: %+v", opsRows(t, dir))
	}
	// EndOp retired the claim file (row, unlink-while-held, release).
	if _, err := os.Stat(s.opLockPath(claim.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("op claim file survived EndOp: %v", err)
	}
}

// TestExportLeavesNoOpRows pins the export op's lifecycle around
// both completion and cancellation.
func TestExportLeavesNoOpRows(t *testing.T) {
	reg := newTestRegistry()
	refStr := testHost + "/ops/export:v1"
	l := newRawLayer(t, tarBytes(t, tfile("f", "content")))
	push(t, reg, refStr, makeImage(t, l))
	s, dir := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Export(context.Background(), img); err != nil {
		t.Fatal(err)
	}
	if n := len(opsRows(t, dir)); n != 0 {
		t.Fatalf("%d ops rows after successful export", n)
	}
	view, err := img.Unify()
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.ExportTo(canceled, view, filepath.Join(scratchDir(t), "t"), img.Hash()); err == nil {
		t.Fatal("canceled export succeeded")
	}
	if n := len(opsRows(t, dir)); n != 0 {
		t.Fatalf("%d ops rows after canceled export", n)
	}
}

// TestLeaseWaitIsCancelable pins the lock order
// (REQ-store-single-writer): a request blocked behind another
// ingest waits on the LEASE — context-aware — never on the shared
// mutex; the reversed order would park uninterruptibly and, against
// the commit path, deadlock.
func TestLeaseWaitIsCancelable(t *testing.T) {
	gate := make(chan struct{})
	parked := make(chan struct{}, 4)
	var gateOn atomic.Bool
	reg := newTestRegistry()
	slow := handlerTransport{h: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gateOn.Load() && strings.Contains(r.URL.Path, "/blobs/") && r.Method == http.MethodGet {
			parked <- struct{}{}
			<-gate
		}
		reg.h.ServeHTTP(w, r)
	})}
	ref1 := testHost + "/lease/slow:v1"
	ref2 := testHost + "/lease/second:v1"
	push(t, reg, ref1, makeImage(t, newRawLayer(t, tarBytes(t, tfile("a", "1")))))
	img2 := makeImage(t, newRawLayer(t, tarBytes(t, tfile("b", "2"))))
	push(t, reg, ref2, img2)
	s, dir := newTestStore(t, PullIfNotPresent, slow)

	// Pre-pull ref2 (ungated: no gated blob GET runs while nothing
	// holds the lease), then damage it so the second Image below
	// reaches the heal branch DIRECTLY — cached resolution, no
	// resolveTop write, its first lease contact the branch's own
	// ensure. That is the call site whose lock order the test pins.
	if _, err := s.Image(context.Background(), ref2, nil); err != nil {
		t.Fatal(err)
	}
	child2 := mustDigest(t, img2)
	if err := os.Remove(filepath.Join(dir, "oci", "blobs", child2.Algorithm, child2.Hex)); err != nil {
		t.Fatal(err)
	}
	gateOn.Store(true)

	done := make(chan error, 1)
	go func() {
		_, err := s.Image(context.Background(), ref1, nil)
		done <- err
	}()
	// Wait until the slow ingest is PARKED in the gated blob fetch —
	// at that point it provably holds both the lease and the ingest
	// mutex (assemble runs under both).
	select {
	case <-parked:
	case <-time.After(5 * time.Second):
		close(gate)
		t.Fatal("slow ingest never reached the gated fetch")
	}

	short, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := s.Image(short, ref2, nil)
	elapsed := time.Since(start)
	close(gate)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second ingest under a held lease: %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("second ingest blocked %v — parked on the mutex, not the cancelable lease", elapsed)
	}
	if err := <-done; err != nil {
		t.Fatalf("slow ingest: %v", err)
	}
}
