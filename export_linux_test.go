//go:build linux

package ocifs

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

// TestExportPrivilegedOwnership exercises export's privileged chown
// arm inside a WIDE user namespace (subuid-delegated via
// unshare --map-auto): the mapped root applies DISTINCT recorded
// ownership natively, and recorded setuid survives — pinning both
// the chown itself and the chown-before-chmod ordering (a
// wrongly-ordered chown by root would clear the bit).
func TestExportPrivilegedOwnership(t *testing.T) {
	if os.Getenv("OCIFS_USERNS_EXPORT_CHILD") != "" {
		exportPrivilegedChild(t)
		return
	}
	if err := exec.Command("unshare", "--user", "--map-root-user", "--map-auto", "true").Run(); err != nil {
		t.Skipf("deferred: wide user-namespace mappings unavailable (unshare --map-auto: %v)", err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("unshare", "--user", "--map-root-user", "--map-auto", exe,
		"-test.run", "^TestExportPrivilegedOwnership$", "-test.v")
	cmd.Env = append(os.Environ(), "OCIFS_USERNS_EXPORT_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "child-ownership-ok") {
		t.Fatalf("userns export child: %v\n%s", err, out)
	}
}

// exportPrivilegedChild runs as wide-mapped ns-root: exports an
// image with distinct recorded ownership and a setuid entry,
// asserting both are applied natively.
func exportPrivilegedChild(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Fatal("child expected to run as ns-root")
	}
	srv := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	defer srv.Close()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, hdr := range []tar.Header{
		{Name: "owned", Typeflag: tar.TypeReg, Mode: 0o644, Uid: 12, Gid: 34, Size: 2},
		{Name: "suidbin", Typeflag: tar.TypeReg, Mode: 0o4755, Uid: 12, Gid: 34, Size: 2},
	} {
		h := hdr
		if err := tw.WriteHeader(&h); err != nil {
			t.Fatal(err)
		}
		io.WriteString(tw, "xx")
	}
	tw.Close()
	tarData := buf.Bytes()
	l, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(tarData)), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	img, err := mutate.AppendLayers(empty.Image, l)
	if err != nil {
		t.Fatal(err)
	}
	refStr := u.Host + "/test/uns-export:v1"
	ref, err := name.ParseReference(refStr)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(ref, img); err != nil {
		t.Fatal(err)
	}
	scratch := scratchtest.In(t, filepath.Join(".scratch", "ocifs-wexport-uns"))
	ofs, err := New(WithWorkDir(filepath.Join(scratch, "work")))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ofs.Pull(t.Context(), refStr); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(scratch, "out")
	if _, err := ofs.Export(t.Context(), refStr, ExportWithTargetPath(target)); err != nil {
		t.Fatalf("export as wide ns-root: %v", err)
	}
	var st syscall.Stat_t
	if err := syscall.Lstat(filepath.Join(target, "owned"), &st); err != nil {
		t.Fatal(err)
	}
	if st.Uid != 12 || st.Gid != 34 {
		t.Fatalf("recorded ownership not applied natively: %d:%d", st.Uid, st.Gid)
	}
	if err := syscall.Lstat(filepath.Join(target, "suidbin"), &st); err != nil {
		t.Fatal(err)
	}
	if st.Uid != 12 || st.Mode&0o6777 != 0o4755 {
		t.Fatalf("setuid ordering: uid %d mode %o", st.Uid, st.Mode&0o7777)
	}
	fmt.Println("child-ownership-ok")
}
