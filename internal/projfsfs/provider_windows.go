//go:build windows && amd64

package projfsfs

import (
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	projfs "github.com/greatliontech/projfs-go"

	"github.com/greatliontech/ocifs/internal/projection"
)

// Capabilities is the ProjFS fidelity envelope (REQ-proj-fidelity):
// PrjFileNameCompare ordering and case folding (REQ-proj-case),
// NTFS-representable names only, no FIFOs or devices, symlinks per
// the platform feature probe.
func Capabilities(symlinks bool) projection.Capabilities {
	return projection.Capabilities{
		Compare:   projfs.FileNameCompare,
		ValidName: ValidName,
		Symlinks:  symlinks,
	}
}

// ProbeSymlinkSupport answers the platform symlink-projection probe
// (REQ-proj-fidelity's ProjFS symlink row) with a throwaway
// virtualization instance on scratchDir, before any projection is
// built — so the capability set is settled up front and never
// swapped under a live mount. The scratch directory is removed
// again.
func ProbeSymlinkSupport(scratchDir string) (bool, error) {
	if err := os.MkdirAll(scratchDir, 0o755); err != nil {
		return false, err
	}
	defer os.RemoveAll(scratchDir)
	inst, err := projfs.Start(scratchDir, probeProvider{}, nil)
	if err != nil {
		return false, fmt.Errorf("symlink support probe: %w", err)
	}
	defer inst.Stop()
	return inst.Supports(projfs.FeatureSymlinkProjection), nil
}

// probeProvider is the empty provider behind ProbeSymlinkSupport:
// the probe never serves an entry.
type probeProvider struct{}

func (probeProvider) StartDirectoryEnumeration(projfs.EnumContext, projfs.GUID) error {
	return nil
}
func (probeProvider) GetDirectoryEnumeration(projfs.EnumContext, projfs.GUID, string, projfs.DirEntryWriter) error {
	return nil
}
func (probeProvider) EndDirectoryEnumeration(projfs.EnumContext, projfs.GUID) error { return nil }
func (probeProvider) GetPlaceholderInfo(projfs.FileContext) error {
	return projfs.ErrFileNotFound
}
func (probeProvider) GetFileData(projfs.FileContext, uint64, uint32) error {
	return projfs.ErrFileNotFound
}

// provider serves one projection through the ProjFS callbacks. The
// kernel tree is immutable; the provider's only mutable state is the
// per-enumID cursor table and the residual-bearing report copy.
type provider struct {
	p        *projection.Projection
	blobPath func(v1.Hash) string
	symlinks bool

	instp *atomic.Pointer[projfs.Instance]

	mu    sync.Mutex
	enums map[projfs.GUID]*enumSession

	reportMu   sync.Mutex
	report     projection.Report
	reportPath string
	recorded   map[string]bool
}

// enumSession is one OS enumeration: the target directory and the
// index of the next unprocessed child in the kernel's immutable
// comparator-sorted snapshot — ProjFS calls Get repeatedly for the
// same enumID until the provider returns an empty buffer, so the
// cursor must advance across calls.
type enumSession struct {
	dir    *projection.Entry
	cursor int
	// search is the expression captured on the session's first Get
	// (or a RestartScan): the platform contract reuses it for
	// continuations, whatever later callbacks carry.
	search    string
	searchSet bool
}

// entryAt resolves a virtualization-root-relative, backslash-
// separated path ("" is the root) through the kernel — the kernel
// comparator is PrjFileNameCompare, so resolution case-folds and
// canonicalizes exactly as ProjFS expects. GetFileData paths for
// renamed placeholders arrive as the pre-rename creation name, which
// is precisely the immutable tree's key.
func (pr *provider) entryAt(path string) (*projection.Entry, bool) {
	e := pr.p.Root()
	if path == "" {
		return e, true
	}
	for _, part := range strings.Split(path, `\`) {
		child, ok := pr.p.Lookup(e, part)
		if !ok {
			return nil, false
		}
		e = child
	}
	return e, true
}

func (pr *provider) StartDirectoryEnumeration(ctx projfs.EnumContext, enumID projfs.GUID) error {
	dir, ok := pr.entryAt(ctx.FilePathName)
	if !ok || dir.Kind() != projection.KindDir {
		return projfs.ErrFileNotFound
	}
	pr.mu.Lock()
	pr.enums[enumID] = &enumSession{dir: dir}
	pr.mu.Unlock()
	return nil
}

func (pr *provider) GetDirectoryEnumeration(ctx projfs.EnumContext, enumID projfs.GUID, search string, out projfs.DirEntryWriter) error {
	// The whole body runs under the lock: callbacks arrive on the
	// platform's thread pool, and the cursor mutation needs a
	// happens-before edge even though Gets for one enumID never
	// logically overlap.
	pr.mu.Lock()
	defer pr.mu.Unlock()
	ses, ok := pr.enums[enumID]
	if !ok {
		return projfs.ErrFileNotFound
	}
	if ctx.RestartScan() || !ses.searchSet {
		ses.search, ses.searchSet = search, true
		if ctx.RestartScan() {
			ses.cursor = 0
		}
	}
	for ses.cursor < ses.dir.Len() {
		child := ses.dir.At(ses.cursor)
		// Kernel names reaching this point are NTFS-representable
		// (ValidName filtered NULs and illegal characters), so
		// FileNameMatch cannot panic.
		if ses.search != "" && !projfs.FileNameMatch(child.Name(), ses.search) {
			ses.cursor++
			continue
		}
		err := out.Add(child.Name(), basicInfo(child), pr.symlinkExt(child))
		if err == projfs.ErrInsufficientBuffer {
			if out.FirstEntryOverflowed() {
				// Nothing fit: hand the error back so ProjFS retries
				// with a larger buffer; the cursor stays on this
				// entry.
				return projfs.ErrInsufficientBuffer
			}
			// Partial batch committed; the next Get resumes here.
			return nil
		}
		if err != nil {
			return err
		}
		ses.cursor++
		if ctx.ReturnSingleEntry() {
			return nil
		}
	}
	// Empty buffer returned: the enumeration is complete.
	return nil
}

func (pr *provider) EndDirectoryEnumeration(ctx projfs.EnumContext, enumID projfs.GUID) error {
	pr.mu.Lock()
	delete(pr.enums, enumID)
	pr.mu.Unlock()
	return nil
}

// instance waits out the microseconds between projfs.Start
// dispatching the first callbacks and Serve storing the instance
// pointer (ProjFS probes can arrive before Start returns); a nil
// after the bound means the server never came up.
func (pr *provider) instance() (*projfs.Instance, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		if inst := pr.instp.Load(); inst != nil {
			return inst, nil
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("virtualization instance never became available")
		}
		time.Sleep(time.Millisecond)
	}
}

func (pr *provider) GetPlaceholderInfo(ctx projfs.FileContext) error {
	e, ok := pr.entryAt(ctx.FilePathName)
	if !ok {
		return projfs.ErrFileNotFound
	}
	inst, err := pr.instance()
	if err != nil {
		return err
	}
	return inst.WritePlaceholderInfo(ctx.FilePathName, placeholderInfo(e), pr.symlinkExt(e))
}

func (pr *provider) GetFileData(ctx projfs.FileContext, offset uint64, length uint32) error {
	e, ok := pr.entryAt(ctx.FilePathName)
	if !ok || e.Kind() != projection.KindFile {
		return projfs.ErrFileNotFound
	}
	inst, err := pr.instance()
	if err != nil {
		return err
	}
	f, err := os.Open(pr.blobPath(e.ContentDigest()))
	if err != nil {
		// Local trust boundary: a missing blob is store damage.
		return fmt.Errorf("content blob for %s: %w", ctx.FilePathName, err)
	}
	defer f.Close()

	buf, alignedOff, alignedLen, err := inst.CreateWriteBuffer(offset, length)
	if err != nil {
		return err
	}
	defer buf.Close()

	size := uint64(e.Size())
	if alignedOff >= size {
		return nil
	}
	writeLen := uint64(alignedLen)
	if alignedOff+writeLen > size {
		// The final chunk shortens to EOF; ProjFS accepts the short
		// write (REQ-proj-content: short reads only at EOF).
		writeLen = size - alignedOff
	}
	if _, err := f.ReadAt(buf.Bytes()[:writeLen], int64(alignedOff)); err != nil {
		return fmt.Errorf("content read for %s: %w", ctx.FilePathName, err)
	}
	return inst.WriteFileData(buf, ctx.DataStreamID, alignedOff, uint32(writeLen))
}

// QueryFileName answers existence probes without enumeration: nil
// means exists, ErrFileNotFound feeds the negative path cache. The
// final component may carry DOS wildcards.
func (pr *provider) QueryFileName(ctx projfs.FileContext) error {
	path := ctx.FilePathName
	dirPath, base := "", path
	if i := strings.LastIndex(path, `\`); i >= 0 {
		dirPath, base = path[:i], path[i+1:]
	}
	dir, ok := pr.entryAt(dirPath)
	if !ok || dir.Kind() != projection.KindDir {
		return projfs.ErrFileNotFound
	}
	if !projfs.ContainsWildcards(base) {
		if _, ok := pr.p.Lookup(dir, base); ok {
			return nil
		}
		return projfs.ErrFileNotFound
	}
	for _, child := range dir.Children() {
		if projfs.FileNameMatch(child.Name(), base) {
			return nil
		}
	}
	return projfs.ErrFileNotFound
}

// Notify enforces the read-only contract (REQ-proj-ro): the four
// deniable pre-operations are vetoed with access-denied for
// projected entries — foreign files stay freely mutable by their
// creators — and foreign-file residuals (creation, modification)
// are recorded in the projection report, never only observed.
// Metadata dirt on placeholders has no platform notification and is
// declared by the spec rather than recorded.
func (pr *provider) Notify(ctx projfs.NotificationContext, n projfs.Notification) error {
	switch n := n.(type) {
	case *projfs.PreRename:
		// Both ends are protected: renaming a projected entry away,
		// and rename-replacing a foreign file ONTO a projected name
		// (which would destroy the projected entry without any other
		// deniable pre-operation firing). An empty path on either
		// side is outside the virtualization instance: a move-in
		// (empty source) to a fresh name is foreign-file creation —
		// tolerated and recorded via FileRenamed — and a move-out
		// (empty destination) of a foreign file is its creator's
		// business.
		if ctx.FilePathName != "" {
			if _, projected := pr.entryAt(ctx.FilePathName); projected {
				return projfs.ErrAccessDenied
			}
		}
		if n.DestinationFileName != "" {
			if _, projected := pr.entryAt(n.DestinationFileName); projected {
				return projfs.ErrAccessDenied
			}
		}
	case *projfs.PreSetHardlink:
		// Same shape: an empty source is a cross-boundary link-in;
		// linking AT an existing name fails on its own, so only a
		// projected source needs the veto.
		if ctx.FilePathName != "" {
			if _, projected := pr.entryAt(ctx.FilePathName); projected {
				return projfs.ErrAccessDenied
			}
		}
	case *projfs.PreDelete, *projfs.FilePreConvertToFull:
		if _, projected := pr.entryAt(ctx.FilePathName); projected {
			return projfs.ErrAccessDenied
		}
	case *projfs.NewFileCreated:
		pr.recordResidual(ctx.FilePathName, "created by a foreign process")
	case *projfs.FileOverwritten:
		pr.recordResidual(ctx.FilePathName, "overwritten by a foreign process")
	case *projfs.FileHandleClosedFileModified:
		pr.recordResidual(ctx.FilePathName, "modified by a foreign process")
	case *projfs.FileRenamed:
		// A move-in from outside (or a foreign rename within the
		// root) lands a foreign file at the destination.
		if n.DestinationFileName != "" {
			pr.recordResidual(n.DestinationFileName, "moved in by a foreign process")
		}
	case *projfs.HardlinkCreated:
		if n.DestinationFileName != "" {
			pr.recordResidual(n.DestinationFileName, "hardlinked in by a foreign process")
		}
	}
	return nil
}

// recordResidual appends one residual row (deduplicated by path)
// and republishes the report atomically (REQ-proj-report). Every
// republication writes the whole document, so a failed write is
// healed by the next one — and by the unmount-time flush.
func (pr *provider) recordResidual(path, detail string) {
	pr.reportMu.Lock()
	defer pr.reportMu.Unlock()
	if pr.recorded[path] {
		return
	}
	pr.recorded[path] = true
	pr.report.Entries = append(pr.report.Entries, projection.ReportEntry{
		Path:        strings.ReplaceAll(path, `\`, "/"),
		Disposition: projection.DispositionResidual,
		Reason:      projection.ReasonResidualForeignFile,
		Detail:      detail,
	})
	_ = pr.report.WriteFile(pr.reportPath)
}

// flushReport republishes the accumulated report once more — the
// unmount-time retry for any residual whose immediate write failed.
func (pr *provider) flushReport() {
	pr.reportMu.Lock()
	defer pr.reportMu.Unlock()
	_ = pr.report.WriteFile(pr.reportPath)
}

func basicInfo(e *projection.Entry) *projfs.FileBasicInfo {
	h := e.Header()
	mt := h.ModTime
	if mt.IsZero() {
		mt = epoch
	}
	at, ct := h.AccessTime, h.ChangeTime
	if at.IsZero() {
		at = mt
	}
	if ct.IsZero() {
		ct = mt
	}
	attrs := uint32(fileAttributeNormal)
	if e.Kind() == projection.KindDir {
		attrs = fileAttributeDirectory
	}
	return &projfs.FileBasicInfo{
		IsDirectory: e.Kind() == projection.KindDir,
		FileSize:    e.Size(),
		// The platform's four slots carry the recorded times with the
		// spec's fallbacks; tar records no birth time, so creation
		// carries the modification time (REQ-proj-fidelity).
		CreationTime:   mt,
		LastWriteTime:  mt,
		LastAccessTime: at,
		ChangeTime:     ct,
		FileAttributes: attrs,
	}
}

const (
	fileAttributeDirectory = 0x10
	fileAttributeNormal    = 0x80
)

// placeholderInfo carries the entry's identity into the placeholder:
// the content digest fills the ContentID slot, so content-addressed
// refresh compares digests, never times (REQ-proj-content). The slot
// carries the algorithm name and the RAW digest bytes — any SHA-2
// digest fits the 128-byte field without truncation, unlike the hex
// string form.
func placeholderInfo(e *projection.Entry) *projfs.PlaceholderInfo {
	info := &projfs.PlaceholderInfo{FileBasicInfo: *basicInfo(e)}
	copy(info.VersionInfo.ProviderID[:], providerID)
	if e.Kind() == projection.KindFile {
		d := e.ContentDigest()
		id := append([]byte(d.Algorithm+":"), rawDigest(d.Hex)...)
		copy(info.VersionInfo.ContentID[:], id)
	}
	return info
}

func rawDigest(hexStr string) []byte {
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		// A digest that is not hex is impossible for store-derived
		// entries; fall back to the string bytes.
		return []byte(hexStr)
	}
	return b
}

var providerID = []byte("ocifs")

// symlinkExt returns the extended info for a symlink entry when the
// platform supports projection; nil otherwise, which routes every
// call to the 1809 v1 procs that cannot fail on feature grounds. A
// projection built without symlink capability contains no symlink
// entries, so ext is non-nil only when the probe passed.
func (pr *provider) symlinkExt(e *projection.Entry) *projfs.ExtendedInfo {
	if !pr.symlinks || e.Kind() != projection.KindSymlink {
		return nil
	}
	// The kernel serves targets verbatim (REQ-proj-fidelity); ProjFS
	// expects backslash separators for in-root relative targets.
	return &projfs.ExtendedInfo{SymlinkTargetName: strings.ReplaceAll(e.LinkTarget(), "/", `\`)}
}
