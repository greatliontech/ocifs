package store

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/types"
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
	s, err := NewStore(dir, anonKeychain{}, policy, v1.Platform{}, nil)
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

func refFiles(t *testing.T, storeDir string) []string {
	t.Helper()
	var files []string
	err := filepath.WalkDir(filepath.Join(storeDir, "refs"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
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
		filepath.Join(dir, "layers"),
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
		if _, err := os.Stat(filepath.Join(dir, "layers", ld.Algorithm, ld.Hex)); err != nil {
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
	for _, f := range refFiles(t, dir) {
		if err := os.Remove(f); err != nil {
			t.Fatal(err)
		}
	}
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
	indexPath := filepath.Join(dir, "layers", ld.Algorithm, ld.Hex)
	blobPath := filepath.Join(dir, "blobs", ld.Algorithm, ld.Hex)
	indexData, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("layer index missing at colliding key: %v", err)
	}
	blobData, err := os.ReadFile(blobPath)
	if err != nil {
		t.Fatalf("content blob missing at colliding key: %v", err)
	}
	if bytes.Equal(indexData, blobData) {
		t.Fatalf("index and blob at colliding key hold identical bytes; keyspaces not disjoint")
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
	indexPath := filepath.Join(dir, "layers", ld.Algorithm, ld.Hex)

	// Every heal below must be network-free: cut the transport so
	// any registry round trip fails loudly.
	s.transport = handlerTransport{h: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("self-heal hit the network: %s %s", r.Method, r.URL)
		w.WriteHeader(http.StatusServiceUnavailable)
	})}

	// 1. Missing index.
	if err := os.Remove(indexPath); err != nil {
		t.Fatal(err)
	}
	img, err = s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatalf("heal of missing index: %v", err)
	}
	if got := string(readEntry(t, s, img, "data")); got != "precious" {
		t.Fatalf("data = %q after index heal", got)
	}

	// 2. Corrupt index.
	if err := os.WriteFile(indexPath, []byte("{torn"), 0o644); err != nil {
		t.Fatal(err)
	}
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
	s1, err := NewStore(oldDir, anonKeychain{}, PullIfNotPresent, v1.Platform{}, nil)
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

	s2, err := NewStore(newDir, anonKeychain{}, PullNever, v1.Platform{}, nil)
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
		s, err := NewStore(dir, anonKeychain{}, PullIfNotPresent, v1.Platform{}, nil)
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
	s, err := NewStore(dir, anonKeychain{}, PullIfNotPresent, v1.Platform{}, nil)
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
	_, err := NewStore(dir, anonKeychain{}, PullIfNotPresent, v1.Platform{}, nil)
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
