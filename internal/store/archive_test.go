package store

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
)

// archiveFixture pushes a two-layer image and returns what an
// archive must carry: the configuration's bytes and digest, and
// each layer's compressed bytes by its digest, base to top, as the
// registry serves them.
func archiveFixture(t *testing.T, rt handlerTransport, refStr string) (config []byte, configHex string, layers []v1.Hash, blobs map[string][]byte) {
	t.Helper()
	base := newRawLayer(t, tarBytes(t, tdir("bin"), tfile("bin/plugin", "#!/x\n")))
	top := newRawLayer(t, tarBytes(t, tfile("note", "n")))
	img := makeImage(t, base, top)
	// The configuration names a platform, as a registry's image's
	// does, for the OCI index's descriptor to carry.
	cf, err := img.ConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	cf = cf.DeepCopy()
	cf.OS, cf.Architecture = "linux", "arm64"
	img, err = mutate.ConfigFile(img, cf)
	if err != nil {
		t.Fatal(err)
	}
	push(t, rt, refStr, img)
	config, err = img.RawConfigFile()
	if err != nil {
		t.Fatal(err)
	}
	h, err := img.ConfigName()
	if err != nil {
		t.Fatal(err)
	}
	blobs = map[string][]byte{}
	for _, l := range []v1.Layer{base, top} {
		d, err := l.Digest()
		if err != nil {
			t.Fatal(err)
		}
		rc, err := l.Compressed()
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			t.Fatal(err)
		}
		layers = append(layers, d)
		blobs[d.Hex] = b
	}
	return config, h.Hex, layers, blobs
}

// readArchive reads an archive tar: the entries' names in order and
// each one's bytes by name, every entry a root-owned regular file
// of mode 0644 at the epoch.
func readArchive(t *testing.T, b []byte) (names []string, entries map[string][]byte) {
	t.Helper()
	entries = map[string][]byte{}
	tr := tar.NewReader(bytes.NewReader(b))
	for {
		h, err := tr.Next()
		if err == io.EOF {
			return names, entries
		}
		if err != nil {
			t.Fatal(err)
		}
		if h.Typeflag != tar.TypeReg || h.Mode != 0o644 || h.ModTime.Unix() != 0 || h.Uid != 0 || h.Gid != 0 {
			t.Errorf("%s: a root-owned regular file of mode 0644 at the epoch, got type %c mode %o time %v uid %d", h.Name, h.Typeflag, h.Mode, h.ModTime, h.Uid)
		}
		body, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, h.Name)
		entries[h.Name] = body
	}
}

// TestArchiveLoadsAsTheImage pins REQ-api-archive's classic form:
// manifest.json naming the configuration at <hex>.json and the
// layers at <hex>/layer.tar base to top, no tag; the configuration
// and the layers the registry's bytes; the identity returned the
// configuration's digest; the same image archives to the same
// bytes.
func TestArchiveLoadsAsTheImage(t *testing.T) {
	reg := newTestRegistry()
	refStr := testHost + "/archive/plain:v1"
	config, configHex, layers, blobs := archiveFixture(t, reg, refStr)
	s, _ := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	var a bytes.Buffer
	id, err := s.Archive(context.Background(), img, &a, DockerArchive)
	if err != nil {
		t.Fatal(err)
	}
	if id.Hex != configHex {
		t.Errorf("the identity returned: %s, want the configuration's %s", id.Hex, configHex)
	}
	names, entries := readArchive(t, a.Bytes())
	var manifest []struct {
		Config   string
		RepoTags []string
		Layers   []string
	}
	if err := json.Unmarshal(entries["manifest.json"], &manifest); err != nil || len(manifest) != 1 {
		t.Fatalf("manifest.json: %s %v", entries["manifest.json"], err)
	}
	m := manifest[0]
	wantLayers := []string{layers[0].Hex + "/layer.tar", layers[1].Hex + "/layer.tar"}
	if m.Config != configHex+".json" || m.RepoTags != nil || strings.Join(m.Layers, " ") != strings.Join(wantLayers, " ") {
		t.Errorf("manifest.json names %+v, want the configuration and the layers base to top %v", m, wantLayers)
	}
	if want := append([]string{"manifest.json", m.Config}, wantLayers...); strings.Join(names, " ") != strings.Join(want, " ") {
		t.Errorf("the entries: %v, want %v", names, want)
	}
	if !bytes.Equal(entries[m.Config], config) {
		t.Errorf("the configuration's bytes differ from the registry's")
	}
	for _, l := range layers {
		if !bytes.Equal(entries[l.Hex+"/layer.tar"], blobs[l.Hex]) {
			t.Errorf("layer %s: the bytes differ from the registry's compressed bytes", l.Hex)
		}
	}
	var b bytes.Buffer
	if _, err := s.Archive(context.Background(), img, &b, DockerArchive); err != nil || !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Errorf("a second archive of the image: %v, same bytes %v", err, bytes.Equal(a.Bytes(), b.Bytes()))
	}
}

// TestArchiveOCILayout pins REQ-api-archive's containerd form: an
// OCI image layout whose index names the image's manifest, the
// manifest, configuration and layers under blobs/ by digest, the
// identity returned the manifest's digest.
func TestArchiveOCILayout(t *testing.T) {
	reg := newTestRegistry()
	refStr := testHost + "/archive/oci:v1"
	config, configHex, layers, blobs := archiveFixture(t, reg, refStr)
	s, _ := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	var a bytes.Buffer
	id, err := s.Archive(context.Background(), img, &a, OCILayout)
	if err != nil {
		t.Fatal(err)
	}
	if id != img.Hash() {
		t.Errorf("the identity returned: %s, want the manifest's %s", id, img.Hash())
	}
	names, entries := readArchive(t, a.Bytes())
	if string(entries["oci-layout"]) != `{"imageLayoutVersion":"1.0.0"}` {
		t.Errorf("oci-layout: %s", entries["oci-layout"])
	}
	var index struct {
		SchemaVersion int
		Manifests     []struct {
			MediaType string
			Digest    string
			Size      int
			Platform  struct{ OS, Architecture string }
		}
	}
	if err := json.Unmarshal(entries["index.json"], &index); err != nil || index.SchemaVersion != 2 || len(index.Manifests) != 1 {
		t.Fatalf("index.json: %s %v", entries["index.json"], err)
	}
	d := index.Manifests[0]
	manifestEntry := "blobs/sha256/" + img.Hash().Hex
	if d.Digest != img.Hash().String() || d.Size != len(entries[manifestEntry]) || d.MediaType == "" || d.Platform.OS != "linux" || d.Platform.Architecture != "arm64" {
		t.Errorf("the index's descriptor: %+v, want the manifest's digest and size and the configuration's platform", d)
	}
	want := []string{"oci-layout", "index.json", manifestEntry, "blobs/sha256/" + configHex, "blobs/sha256/" + layers[0].Hex, "blobs/sha256/" + layers[1].Hex}
	if strings.Join(names, " ") != strings.Join(want, " ") {
		t.Errorf("the entries: %v, want %v", names, want)
	}
	if !bytes.Equal(entries["blobs/sha256/"+configHex], config) {
		t.Errorf("the configuration's bytes differ from the registry's")
	}
	for _, l := range layers {
		if !bytes.Equal(entries["blobs/sha256/"+l.Hex], blobs[l.Hex]) {
			t.Errorf("layer %s: the bytes differ from the registry's compressed bytes", l.Hex)
		}
	}
	var m v1.Manifest
	if err := json.Unmarshal(entries[manifestEntry], &m); err != nil || m.Config.Digest.Hex != configHex {
		t.Errorf("the manifest entry: %v, config %s", err, m.Config.Digest)
	}
	if _, err := s.Archive(context.Background(), img, io.Discard, ArchiveForm(7)); err == nil || !strings.Contains(err.Error(), "no archive form 7") {
		t.Errorf("a form the store does not know: %v", err)
	}
}

// A blob collected from under the image since its acquisition fails
// the archive as gone before any byte is written; a context ended
// before the first byte writes none, one ended between a blob's
// writes ends the archive.
func TestArchiveCollectedBlobFails(t *testing.T) {
	reg := newTestRegistry()
	refStr := testHost + "/archive/gone:v1"
	_, _, layers, _ := archiveFixture(t, reg, refStr)
	s, _ := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var early bytes.Buffer
	if _, err := s.Archive(ctx, img, &early, DockerArchive); !errors.Is(err, context.Canceled) || early.Len() != 0 {
		t.Errorf("a context ended before the first byte: %v, %d bytes written", err, early.Len())
	}
	// A context ended once the manifest is out: the first blob's
	// write refuses.
	ctx, cancel = context.WithCancel(context.Background())
	var late bytes.Buffer
	ending := &endingWriter{w: &late, after: 1, cancel: cancel}
	if _, err := s.Archive(ctx, img, ending, DockerArchive); !errors.Is(err, context.Canceled) {
		t.Errorf("a context ended between writes: %v", err)
	}
	if err := os.Remove(s.ociBlobPath(layers[1])); err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if _, err := s.Archive(context.Background(), img, &b, OCILayout); !errors.Is(err, ErrGone) || !strings.Contains(err.Error(), layers[1].Hex) || b.Len() != 0 {
		t.Errorf("a collected layer: %v, %d bytes written", err, b.Len())
	}
}

// endingWriter cancels its context after the given number of writes
// and refuses nothing itself.
type endingWriter struct {
	w      io.Writer
	after  int
	cancel context.CancelFunc
}

func (e *endingWriter) Write(p []byte) (int, error) {
	n, err := e.w.Write(p)
	e.after--
	if e.after == 0 {
		e.cancel()
	}
	return n, err
}

// TestArchiveCommittedImage pins REQ-api-archive for a committed
// image: its archive carries the base's layers as retained and the
// committed layer's plain tar last, the identity under each form
// the committed image's own.
func TestArchiveCommittedImage(t *testing.T) {
	s, dir, img, upperDir := commitFixture(t)
	digest, err := s.CommitUpper(context.Background(), img, upperDir)
	if err != nil {
		t.Fatal(err)
	}
	offline := newStoreAt(t, dir, PullIfNotPresent, linuxAMD64, cutTransport(t))
	committed, err := offline.Image(context.Background(), LocalRef(digest), nil)
	if err != nil {
		t.Fatal(err)
	}
	var a bytes.Buffer
	id, err := offline.Archive(context.Background(), committed, &a, DockerArchive)
	if err != nil {
		t.Fatal(err)
	}
	names, entries := readArchive(t, a.Bytes())
	var manifest []struct {
		Config string
		Layers []string
	}
	if err := json.Unmarshal(entries["manifest.json"], &manifest); err != nil || len(manifest) != 1 || len(manifest[0].Layers) != 2 {
		t.Fatalf("manifest.json: %s %v", entries["manifest.json"], err)
	}
	if id.Hex+".json" != manifest[0].Config || len(names) != 4 {
		t.Errorf("the identity %s against the manifest's %s; entries %v", id, manifest[0].Config, names)
	}
	// The committed layer is a plain tar: its first entry readable.
	last := entries[manifest[0].Layers[1]]
	if h, err := tar.NewReader(bytes.NewReader(last)).Next(); err != nil || h.Name == "" {
		t.Errorf("the committed layer as retained: %v %v", h, err)
	}
	var o bytes.Buffer
	if id, err := offline.Archive(context.Background(), committed, &o, OCILayout); err != nil || id != digest {
		t.Errorf("the committed image's OCI layout: %v, identity %s, want %s", err, id, digest)
	}
}

// TestArchivePinsAgainstCollection pins REQ-api-archive's snapshot:
// an archive in flight pins its image, so a collection landing
// between its first byte and its last leaves every blob, and the
// archive is whole; the same collection before the archive takes
// an unrooted image, which then fails as gone.
func TestArchivePinsAgainstCollection(t *testing.T) {
	reg := newTestRegistry()
	refStr := testHost + "/archive/pinned:v1"
	archiveFixture(t, reg, refStr)
	s, _ := newTestStore(t, PullIfNotPresent, reg)
	img, err := s.Image(context.Background(), refStr, nil)
	if err != nil {
		t.Fatal(err)
	}
	var whole bytes.Buffer
	if _, err := s.Archive(context.Background(), img, &whole, DockerArchive); err != nil {
		t.Fatal(err)
	}
	if err := s.RemoveRef(context.Background(), refStr); err != nil {
		t.Fatal(err)
	}
	// The reference gone, a collection ignoring grace would take the
	// image; one landing after the archive's first byte finds it
	// pinned.
	var during bytes.Buffer
	sweeping := &sweepingWriter{w: &during, after: 1, sweep: func() {
		if _, err := s.Collect(context.Background(), CollectOpts{Grace: -1}); err != nil {
			t.Errorf("a collection during the archive: %v", err)
		}
	}}
	if _, err := s.Archive(context.Background(), img, sweeping, DockerArchive); err != nil || !bytes.Equal(during.Bytes(), whole.Bytes()) {
		t.Errorf("an archive with a collection landing inside it: %v, whole %v", err, bytes.Equal(during.Bytes(), whole.Bytes()))
	}
	if _, err := s.Collect(context.Background(), CollectOpts{Grace: -1}); err != nil {
		t.Fatal(err)
	}
	var after bytes.Buffer
	if _, err := s.Archive(context.Background(), img, &after, DockerArchive); !errors.Is(err, ErrGone) || after.Len() != 0 {
		t.Errorf("an archive of a collected image: %v, %d bytes written", err, after.Len())
	}
}

// sweepingWriter runs sweep after the given number of writes.
type sweepingWriter struct {
	w     io.Writer
	after int
	sweep func()
}

func (e *sweepingWriter) Write(p []byte) (int, error) {
	n, err := e.w.Write(p)
	e.after--
	if e.after == 0 {
		e.sweep()
	}
	return n, err
}
