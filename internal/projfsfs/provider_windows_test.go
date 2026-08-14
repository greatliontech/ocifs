//go:build windows && amd64

package projfsfs

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	projfs "github.com/greatliontech/projfs-go"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/projection"
)

// This suite runs on a real windows machine with ProjFS enabled
// (Enable-WindowsOptionalFeature -Online -FeatureName
// Client-ProjFS). It validates the ProjFS column of
// docs/specs/projection.md end to end: run `go test ./...` at the
// repository root and report the full output.

type fixture struct {
	srv        *Server
	root       string
	reportPath string
	symlinks   bool
	proj       *projection.Projection
}

// digestFor derives the content key the store would derive.
func digestFor(content []byte) v1.Hash {
	sum := sha256.Sum256(content)
	return v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}
}

type fileSpec struct {
	name    string
	content []byte
	mode    int64
	flag    byte
	link    string
}

// serveFixture builds a view from specs, backs regular files with an
// on-disk blob directory, and serves it at a temp virtualization
// root — mirroring the store's blobPath contract without a store.
func serveFixture(t *testing.T, specs []fileSpec) *fixture {
	t.Helper()
	blobDir := t.TempDir()
	var entries []layer.Entry
	for _, s := range specs {
		flag := s.flag
		if flag == 0 {
			flag = tar.TypeReg
		}
		e := layer.Entry{Header: tar.Header{
			Name: s.name, Typeflag: flag, Mode: s.mode,
			Size: int64(len(s.content)), Linkname: s.link,
			ModTime: time.Date(2021, 5, 6, 7, 8, 9, 0, time.UTC),
		}}
		if flag == tar.TypeReg {
			d := digestFor(s.content)
			e.Digest = d
			if err := os.WriteFile(filepath.Join(blobDir, d.Hex), s.content, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		entries = append(entries, e)
	}
	view, err := layer.Unify([]layer.Layer{layer.Layer(entries)})
	if err != nil {
		t.Fatal(err)
	}

	probeDir := filepath.Join(t.TempDir(), "probe")
	symlinks, err := ProbeSymlinkSupport(probeDir)
	if err != nil {
		t.Fatalf("symlink probe: %v", err)
	}
	proj, err := projection.New(view, nil, Capabilities(symlinks))
	if err != nil {
		t.Fatal(err)
	}

	root := t.TempDir()
	reportPath := filepath.Join(t.TempDir(), projection.ReportFileName)
	if err := proj.Report().WriteFile(reportPath); err != nil {
		t.Fatal(err)
	}
	blobPath := func(h v1.Hash) string { return filepath.Join(blobDir, h.Hex) }
	srv, err := Serve(proj, blobPath, reportPath, root)
	if err != nil {
		t.Fatalf("Serve: %v", err)
	}
	t.Cleanup(func() { srv.Unmount() })
	return &fixture{srv: srv, root: root, reportPath: reportPath, symlinks: symlinks, proj: proj}
}

func basicSpecs() []fileSpec {
	return []fileSpec{
		{name: "docs", flag: tar.TypeDir, mode: 0o755},
		{name: "docs/a.txt", content: []byte("hello projfs"), mode: 0o644},
		{name: "docs/B.txt", content: []byte("upper"), mode: 0o644},
		{name: "readme", content: []byte("root file"), mode: 0o644},
	}
}

func TestProjectionTreeAndCaseInsensitiveLookup(t *testing.T) {
	f := serveFixture(t, basicSpecs())

	got, err := os.ReadFile(filepath.Join(f.root, "docs", "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello projfs" {
		t.Fatalf("content = %q", got)
	}
	// The namespace folds case (REQ-proj-case): a different-case path
	// resolves to the same entry.
	got, err = os.ReadFile(filepath.Join(f.root, "DOCS", "A.TXT"))
	if err != nil {
		t.Fatalf("case-folded path did not resolve: %v", err)
	}
	if string(got) != "hello projfs" {
		t.Fatalf("case-folded content = %q", got)
	}
	fi, err := os.Stat(filepath.Join(f.root, "docs"))
	if err != nil || !fi.IsDir() {
		t.Fatalf("docs: %v %v", fi, err)
	}
}

func TestEnumerationOrderAndStability(t *testing.T) {
	f := serveFixture(t, basicSpecs())

	var passes [][]string
	for i := 0; i < 3; i++ {
		ents, err := os.ReadDir(filepath.Join(f.root, "docs"))
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, e := range ents {
			names = append(names, e.Name())
		}
		passes = append(passes, names)
	}
	for i := 1; i < len(passes); i++ {
		if strings.Join(passes[i], "|") != strings.Join(passes[0], "|") {
			t.Fatalf("enumeration unstable across passes: %v vs %v", passes[i], passes[0])
		}
	}
	// The backend supplies entries in PrjFileNameCompare order
	// (REQ-proj-enumeration).
	names := passes[0]
	for i := 1; i < len(names); i++ {
		if projfs.FileNameCompare(names[i-1], names[i]) >= 0 {
			t.Fatalf("enumeration order violates the platform comparator: %q before %q", names[i-1], names[i])
		}
	}

	// Pattern-filtered listing honors DOS wildcards end to end.
	matches, err := filepath.Glob(filepath.Join(f.root, "docs", "*.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 {
		t.Fatalf("glob *.txt = %v, want both txt entries", matches)
	}
}

func TestPaginationCompletes(t *testing.T) {
	specs := []fileSpec{}
	long := strings.Repeat("x", 120)
	for i := 0; i < 300; i++ {
		specs = append(specs, fileSpec{
			name:    fmt.Sprintf("f%03d-%s", i, long),
			content: []byte{byte(i)},
			mode:    0o644,
		})
	}
	f := serveFixture(t, specs)
	ents, err := os.ReadDir(f.root)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 300 {
		t.Fatalf("enumerated %d entries, want 300 (cursor lost across Get callbacks?)", len(ents))
	}
}

func TestLargeContentByteExact(t *testing.T) {
	content := make([]byte, 1<<20)
	for i := range content {
		content[i] = byte(i * 7)
	}
	f := serveFixture(t, []fileSpec{{name: "big.bin", content: content, mode: 0o644}})

	got, err := os.ReadFile(filepath.Join(f.root, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("1MiB content mismatch (len %d vs %d)", len(got), len(content))
	}

	// Odd-sized tail: the final chunk shortens to EOF
	// (REQ-proj-content).
	odd := content[:(1<<20)-13]
	f2 := serveFixture(t, []fileSpec{{name: "odd.bin", content: odd, mode: 0o644}})
	got, err = os.ReadFile(filepath.Join(f2.root, "odd.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, odd) {
		t.Fatalf("odd-size content mismatch (len %d vs %d)", len(got), len(odd))
	}
}

func TestUnrepresentableNameOmittedAndReported(t *testing.T) {
	f := serveFixture(t, append(basicSpecs(),
		fileSpec{name: "bad:name", content: []byte("x"), mode: 0o644},
		fileSpec{name: "trailing.", content: []byte("y"), mode: 0o644},
	))

	ents, err := os.ReadDir(f.root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.Contains(e.Name(), ":") || strings.HasSuffix(e.Name(), ".") {
			t.Fatalf("unrepresentable name enumerated: %q", e.Name())
		}
	}

	rep, err := projection.ReadReportFile(f.reportPath)
	if err != nil {
		t.Fatal(err)
	}
	reasons := map[string]projection.Reason{}
	for _, re := range rep.Entries {
		reasons[re.Path] = re.Reason
	}
	for _, p := range []string{"bad:name", "trailing."} {
		if reasons[p] != projection.ReasonNameUnrepresentable {
			t.Fatalf("%q not reported unrepresentable: %v", p, rep.Entries)
		}
	}
}

func TestSymlinkProjectionOrDeclaredFallback(t *testing.T) {
	f := serveFixture(t, append(basicSpecs(),
		fileSpec{name: "link", flag: tar.TypeSymlink, link: "readme", mode: 0o777},
	))

	if f.symlinks {
		fi, err := os.Lstat(filepath.Join(f.root, "link"))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode()&os.ModeSymlink == 0 {
			t.Fatalf("link mode = %v, want a symlink (feature probe passed)", fi.Mode())
		}
		target, err := os.Readlink(filepath.Join(f.root, "link"))
		if err != nil {
			t.Fatal(err)
		}
		if target != "readme" {
			t.Fatalf("target = %q, want verbatim %q", target, "readme")
		}
		return
	}

	// Feature probe failed (pre-2004 or non-NTFS root): the symlink
	// is omitted and reported (REQ-proj-fidelity).
	t.Logf("symlink projection unsupported on this machine; validating the declared fallback")
	if _, err := os.Lstat(filepath.Join(f.root, "link")); err == nil {
		t.Fatal("symlink presented despite failed feature probe")
	}
	rep, err := projection.ReadReportFile(f.reportPath)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, re := range rep.Entries {
		if re.Path == "link" && re.Reason == projection.ReasonSymlinkUnsupported {
			found = true
		}
	}
	if !found {
		t.Fatalf("omitted symlink not reported: %v", rep.Entries)
	}
}

// TestMain doubles as the foreign-process helper: ProjFS suppresses
// notifications (and therefore vetoes) for the provider's own
// process, so read-only enforcement is observable only from a
// separate process. Re-execing the test binary preserves raw Win32
// errnos, which cmd.exe exit codes notoriously do not.
func TestMain(m *testing.M) {
	if op := os.Getenv("OCIFS_PROJFS_HELPER"); op != "" {
		path := os.Getenv("OCIFS_PROJFS_PATH")
		var err error
		switch op {
		case "delete":
			err = os.Remove(path)
		case "rename":
			err = os.Rename(path, path+".renamed")
		case "rename-onto":
			// path is "src|dst": rename-replace src onto dst.
			src, dst, _ := strings.Cut(path, "|")
			err = os.Rename(src, dst)
		case "create":
			err = os.WriteFile(path, []byte("foreign"), 0o644)
		case "write":
			var f *os.File
			f, err = os.OpenFile(path, os.O_WRONLY, 0)
			if err == nil {
				_, err = f.Write([]byte("dirt"))
				f.Close()
			}
		case "attrib":
			err = exec.Command("attrib", "+r", path).Run()
		case "junction":
			// path is "link|target": create an NTFS junction.
			link, target, _ := strings.Cut(path, "|")
			err = exec.Command("cmd", "/c", "mklink", "/J", link, target).Run()
		default:
			err = fmt.Errorf("unknown helper op %q", op)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// childOp performs one filesystem operation from a foreign process.
func childOp(t *testing.T, op, path string) error {
	t.Helper()
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), "OCIFS_PROJFS_HELPER="+op, "OCIFS_PROJFS_PATH="+path)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func TestReadOnlyVetoesFromForeignProcess(t *testing.T) {
	f := serveFixture(t, basicSpecs())
	target := filepath.Join(f.root, "readme")

	if err := childOp(t, "delete", target); err == nil {
		t.Fatal("foreign delete succeeded (REQ-proj-ro PreDelete veto)")
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("readme gone after vetoed delete: %v", err)
	}

	if err := childOp(t, "rename", target); err == nil {
		t.Fatal("foreign rename succeeded (PreRename veto)")
	}
	if _, err := os.Stat(target); err != nil {
		t.Fatalf("readme gone after vetoed rename: %v", err)
	}

	// Writing into a projected file requires convert-to-full, which
	// the provider vetoes.
	if err := childOp(t, "write", target); err == nil {
		t.Fatal("foreign write-open succeeded (FilePreConvertToFull veto)")
	}
	got, err := os.ReadFile(target)
	if err != nil || string(got) != "root file" {
		t.Fatalf("content after veto attempts = %q, %v", got, err)
	}
}

// TestRenameReplaceOntoProjectedEntryVetoed: a foreign file must
// not be rename-replaced onto a projected name — the destination
// side of PreRename is vetoed, or the projected entry would be
// destroyed with no other deniable pre-operation firing
// (REQ-proj-ro).
func TestRenameReplaceOntoProjectedEntryVetoed(t *testing.T) {
	f := serveFixture(t, basicSpecs())
	foreign := filepath.Join(f.root, "own.txt")
	target := filepath.Join(f.root, "readme")

	if err := childOp(t, "create", foreign); err != nil {
		t.Fatalf("foreign create: %v", err)
	}
	if err := childOp(t, "rename-onto", foreign+"|"+target); err == nil {
		t.Fatal("rename-replace onto a projected entry succeeded")
	}
	got, err := os.ReadFile(target)
	if err != nil || string(got) != "root file" {
		t.Fatalf("projected entry after rename-replace attempt = %q, %v", got, err)
	}
}

// TestMoveInFromOutsideAllowedAndRecorded: a cross-boundary move-in
// (empty PreRename source per the platform contract) is foreign-file
// creation — tolerated at a fresh name, recorded via FileRenamed,
// and still vetoed onto a projected name.
func TestMoveInFromOutsideAllowedAndRecorded(t *testing.T) {
	f := serveFixture(t, basicSpecs())
	outside := filepath.Join(t.TempDir(), "outside.txt")
	if err := os.WriteFile(outside, []byte("from outside"), 0o644); err != nil {
		t.Fatal(err)
	}

	fresh := filepath.Join(f.root, "movedin.txt")
	if err := childOp(t, "rename-onto", outside+"|"+fresh); err != nil {
		t.Fatalf("move-in to a fresh name should succeed (foreign creation): %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		rep, err := projection.ReadReportFile(f.reportPath)
		found := false
		if err == nil {
			for _, re := range rep.Entries {
				if re.Disposition == projection.DispositionResidual &&
					strings.EqualFold(re.Path, "movedin.txt") {
					found = true
				}
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			rep, _ := projection.ReadReportFile(f.reportPath)
			t.Fatalf("move-in never recorded; report: %+v", rep)
		}
		time.Sleep(20 * time.Millisecond)
	}

	outside2 := filepath.Join(t.TempDir(), "outside2.txt")
	if err := os.WriteFile(outside2, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := childOp(t, "rename-onto", outside2+"|"+filepath.Join(f.root, "readme")); err == nil {
		t.Fatal("move-in onto a projected name succeeded")
	}
}

// TestForeignDeleteLeavesNoTombstoneAfterUnmount: a foreign file
// created and deleted under the mapped root may leave a platform
// tombstone hidden from live enumeration; unmount must sweep it so
// only declared residue remains (REQ-api-mountpoint).
func TestForeignDeleteLeavesNoTombstoneAfterUnmount(t *testing.T) {
	f := serveFixture(t, basicSpecs())
	foreign := filepath.Join(f.root, "ephemeral.txt")
	if err := childOp(t, "create", foreign); err != nil {
		t.Fatalf("foreign create: %v", err)
	}
	if err := childOp(t, "delete", foreign); err != nil {
		t.Fatalf("foreign delete of a foreign file: %v", err)
	}

	if err := f.srv.Unmount(); err != nil {
		t.Fatal(err)
	}
	ents, err := os.ReadDir(f.root)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 0 {
		t.Fatalf("post-unmount residue (tombstone?): %v", ents)
	}
}

// TestForeignJunctionSurvivesUnmount: the unmount sweep removes only
// ProjFS-tagged reparse state; a foreign junction is retained
// residual state like any foreign file.
func TestForeignJunctionSurvivesUnmount(t *testing.T) {
	f := serveFixture(t, basicSpecs())
	target := t.TempDir()
	link := filepath.Join(f.root, "junction")
	if err := childOp(t, "junction", link+"|"+target); err != nil {
		t.Skipf("junction creation unavailable: %v", err)
	}

	if err := f.srv.Unmount(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(link); err != nil {
		t.Fatalf("foreign junction removed by the unmount sweep: %v", err)
	}
}

// TestForeignFilesStayMutable pins the veto scope (REQ-proj-ro:
// projected entries only): a foreign process creates, modifies, and
// deletes its own file freely.
func TestForeignFilesStayMutable(t *testing.T) {
	f := serveFixture(t, basicSpecs())
	foreign := filepath.Join(f.root, "own.txt")

	if err := childOp(t, "create", foreign); err != nil {
		t.Fatalf("foreign create (declared residual): %v", err)
	}
	if err := childOp(t, "write", foreign); err != nil {
		t.Fatalf("foreign file not writable by its creator: %v", err)
	}
	if err := childOp(t, "delete", foreign); err != nil {
		t.Fatalf("foreign file not deletable by its creator: %v", err)
	}
}

// TestPlaceholderMetadataDirtTolerated probes the declared-but-
// unrecordable residual: a foreign attribute change on a projected
// placeholder is not deniable on the platform; served content must
// stay byte-identical (REQ-proj-ro's last sentence).
func TestPlaceholderMetadataDirtTolerated(t *testing.T) {
	f := serveFixture(t, basicSpecs())
	target := filepath.Join(f.root, "readme")
	if _, err := os.ReadFile(target); err != nil {
		t.Fatal(err)
	}

	// Outcome is platform-dependent (no pre-op exists); whatever
	// attrib reports, the projected content must be unaltered.
	attribErr := childOp(t, "attrib", target)
	t.Logf("attrib +r on placeholder: err=%v", attribErr)
	got, err := os.ReadFile(target)
	if err != nil || string(got) != "root file" {
		t.Fatalf("content after metadata dirt = %q, %v", got, err)
	}
}

// TestUnmountLeavesOnlyForeignResidue pins the amended
// REQ-api-mountpoint on ProjFS: unmount removes projected
// placeholder state; residual foreign files remain, and only those.
func TestUnmountLeavesOnlyForeignResidue(t *testing.T) {
	f := serveFixture(t, basicSpecs())

	// Hydrate part of the tree and leave one foreign file.
	if _, err := os.ReadFile(filepath.Join(f.root, "docs", "a.txt")); err != nil {
		t.Fatal(err)
	}
	foreign := filepath.Join(f.root, "keepme.txt")
	if err := childOp(t, "create", foreign); err != nil {
		t.Fatalf("foreign create: %v", err)
	}

	if err := f.srv.Unmount(); err != nil {
		t.Fatal(err)
	}
	// Idempotent: the fixture cleanup will call Unmount again.
	if err := f.srv.Unmount(); err != nil {
		t.Fatal(err)
	}

	ents, err := os.ReadDir(f.root)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if !strings.EqualFold(e.Name(), "keepme.txt") {
			t.Fatalf("post-unmount residue beyond foreign files: %q", e.Name())
		}
	}
	if _, err := os.Stat(foreign); err != nil {
		t.Fatalf("residual foreign file removed at unmount: %v", err)
	}
}

func TestResidualForeignFileRecorded(t *testing.T) {
	f := serveFixture(t, basicSpecs())
	foreign := filepath.Join(f.root, "intruder.txt")

	// Creation of a NEW file is a declared residual: not deniable,
	// but recorded (REQ-proj-ro).
	if err := childOp(t, "create", foreign); err != nil {
		t.Fatalf("foreign new-file creation should succeed (declared residual): %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		rep, err := projection.ReadReportFile(f.reportPath)
		if err == nil {
			for _, re := range rep.Entries {
				if re.Disposition == projection.DispositionResidual &&
					re.Reason == projection.ReasonResidualForeignFile &&
					strings.EqualFold(re.Path, "intruder.txt") {
					return
				}
			}
		}
		if time.Now().After(deadline) {
			rep, _ := projection.ReadReportFile(f.reportPath)
			t.Fatalf("residual foreign file never recorded; report: %+v", rep)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestReportPersistedBeforeServing(t *testing.T) {
	f := serveFixture(t, basicSpecs())
	rep, err := projection.ReadReportFile(f.reportPath)
	if err != nil {
		t.Fatalf("report unreadable while serving: %v", err)
	}
	if rep.Entries == nil {
		t.Fatal("report entries array absent")
	}
}
