// Package fskitfs serves a projection kernel tree as an FSKit
// volume — the darwin backend (docs/specs/projection.md, FSKit
// column). The implementation is portable: everything except the
// ObjC bridge (which lives in fskit-go's darwin files) builds and
// tests on any platform, so the read-only contract is pinned on
// linux while the actual mount is validated on a darwin machine
// with a signed app extension.
//
// Items are the kernel's entries and item IDs are the kernel's
// view-derived IDs — FSKit's reserved values (root 2, first valid
// 16) are exactly the kernel's identity scheme (REQ-proj-identity).
// The volume declares itself case-sensitive (REQ-proj-case), serves
// enumeration from the kernel's immutable byte-ordered snapshots
// with positional cookies and a constant verifier (an immutable
// directory can never go stale, REQ-proj-enumeration), and answers
// every mutating operation with EROFS (REQ-proj-ro).
package fskitfs

import (
	"io"
	"math"
	"os"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	fskit "github.com/greatliontech/fskit-go"

	"github.com/greatliontech/ocifs/internal/projection"
)

// Capabilities is the FSKit fidelity envelope (REQ-proj-fidelity):
// byte-ordered case-sensitive namespace, every kind presented.
func Capabilities() projection.Capabilities {
	return projection.Capabilities{Symlinks: true, FIFOs: true, Devices: true}
}

// readOnlyVerifier is the constant change-verifier every enumeration
// returns: the projection is immutable, so no enumeration can ever
// be stale (REQ-proj-enumeration). A writable stage bumps a real
// verifier instead.
const readOnlyVerifier fskit.DirVerifier = 1

// Volume serves one projection.
type Volume struct {
	proj     *projection.Projection
	blobPath func(v1.Hash) string
}

// New builds the volume for a projection. blobPath resolves a
// regular-file entry's content digest to its on-disk CAS blob.
func New(proj *projection.Projection, blobPath func(v1.Hash) string) *Volume {
	return &Volume{proj: proj, blobPath: blobPath}
}

var _ fskit.Volume = (*Volume)(nil)
var _ fskit.ReadWriter = (*Volume)(nil)

// asEntry rejects items this volume never returned.
func asEntry(item fskit.Item) (*projection.Entry, error) {
	e, ok := item.(*projection.Entry)
	if !ok || e == nil {
		return nil, fskit.EINVAL
	}
	return e, nil
}

func (vol *Volume) Activate(opts fskit.TaskOptions) (fskit.Item, error) {
	return vol.proj.Root(), nil
}

func (vol *Volume) Deactivate(opts fskit.DeactivateOptions) error { return nil }
func (vol *Volume) Mount(opts fskit.TaskOptions) error            { return nil }
func (vol *Volume) Unmount() error                                { return nil }
func (vol *Volume) Synchronize(flags fskit.SyncFlags) error       { return nil }

func itemType(k projection.Kind) fskit.ItemType {
	switch k {
	case projection.KindDir:
		return fskit.TypeDirectory
	case projection.KindSymlink:
		return fskit.TypeSymlink
	case projection.KindFIFO:
		return fskit.TypeFIFO
	case projection.KindCharDevice:
		return fskit.TypeCharDevice
	case projection.KindBlockDevice:
		return fskit.TypeBlockDevice
	default:
		return fskit.TypeFile
	}
}

// entrySize is the size the entry presents: blob size for files, the
// target length for symlinks (POSIX convention), zero otherwise.
func entrySize(e *projection.Entry) uint64 {
	switch e.Kind() {
	case projection.KindFile:
		return uint64(e.Size())
	case projection.KindSymlink:
		return uint64(len(e.LinkTarget()))
	default:
		return 0
	}
}

func (vol *Volume) GetAttributes(item fskit.Item, req fskit.GetAttributesRequest) (fskit.Attributes, error) {
	e, err := asEntry(item)
	if err != nil {
		return fskit.Attributes{}, err
	}
	h := e.Header()

	// The timestamp fallbacks of REQ-proj-fidelity: unrecorded
	// atime/ctime present as mtime, unrecorded mtime as the epoch,
	// and the birth slot carries the modification time.
	mt := h.ModTime
	if mt.IsZero() {
		mt = time.Unix(0, 0)
	}
	at, ct := h.AccessTime, h.ChangeTime
	if at.IsZero() {
		at = mt
	}
	if ct.IsZero() {
		ct = mt
	}

	parentID := fskit.ItemIDParentOfRoot
	if p := e.Parent(); p != nil {
		parentID = fskit.ItemID(p.ID())
	}
	// Populating more than req.Wanted is harmless per the binding
	// contract; the full recorded envelope is returned.
	return fskit.Attributes{
		Type:       itemType(e.Kind()),
		Mode:       uint32(h.Mode) & 0o7777,
		UID:        uint32(h.Uid),
		GID:        uint32(h.Gid),
		LinkCount:  1,
		Size:       entrySize(e),
		AllocSize:  entrySize(e),
		FileID:     fskit.ItemID(e.ID()),
		ParentID:   parentID,
		AccessTime: at,
		ModifyTime: mt,
		ChangeTime: ct,
		BirthTime:  mt,
	}, nil
}

func (vol *Volume) SetAttributes(item fskit.Item, req fskit.SetAttributesRequest) (fskit.Attributes, error) {
	return fskit.Attributes{}, fskit.EROFS
}

func (vol *Volume) Lookup(dir fskit.Item, name string) (fskit.Item, string, error) {
	d, err := asEntry(dir)
	if err != nil {
		return nil, "", err
	}
	if d.Kind() != projection.KindDir {
		return nil, "", fskit.ENOTDIR
	}
	child, ok := vol.proj.Lookup(d, name)
	if !ok {
		return nil, "", fskit.ENOENT
	}
	// Case-sensitive: the canonical name is the requested name
	// (which equals the stored spelling on a hit).
	return child, child.Name(), nil
}

func (vol *Volume) Reclaim(item fskit.Item) error {
	_, err := asEntry(item)
	return err
}

func (vol *Volume) ReadSymlink(item fskit.Item) (string, error) {
	e, err := asEntry(item)
	if err != nil {
		return "", err
	}
	if e.Kind() != projection.KindSymlink {
		return "", fskit.EINVAL
	}
	return e.LinkTarget(), nil
}

func (vol *Volume) Create(dir fskit.Item, name string, typ fskit.ItemType, attrs fskit.SetAttributesRequest) (fskit.Item, string, error) {
	return nil, "", fskit.EROFS
}

func (vol *Volume) CreateSymlink(dir fskit.Item, name string, attrs fskit.SetAttributesRequest, target string) (fskit.Item, string, error) {
	return nil, "", fskit.EROFS
}

func (vol *Volume) CreateLink(item, dir fskit.Item, name string) (string, error) {
	return "", fskit.EROFS
}

func (vol *Volume) Remove(dir, item fskit.Item, name string) error {
	return fskit.EROFS
}

func (vol *Volume) Rename(item, srcDir fskit.Item, srcName string, dstDir fskit.Item, dstName string, over fskit.Item) (string, error) {
	return "", fskit.EROFS
}

// Enumerate serves the kernel's immutable comparator-sorted child
// snapshot: the cookie is the index of the next child (0 = start),
// each packed entry carries index+1 as its resume cookie, and a
// packer refusal re-serves the refused entry on the next call
// (REQ-proj-enumeration). No "." or ".." entries are packed.
func (vol *Volume) Enumerate(dir fskit.Item, cookie fskit.DirCookie, verifier fskit.DirVerifier, wanted fskit.Attribute, packer fskit.DirEntryPacker) (fskit.DirVerifier, error) {
	d, err := asEntry(dir)
	if err != nil {
		return 0, err
	}
	if d.Kind() != projection.KindDir {
		return 0, fskit.ENOTDIR
	}
	// The kernel forwards cookies verbatim; a wrapped or runaway
	// value is a completed (or nonsensical) enumeration, never an
	// index to crash on.
	start := int64(cookie)
	if start < 0 || start > int64(d.Len()) {
		return readOnlyVerifier, nil
	}
	for i := int(start); i < d.Len(); i++ {
		child := d.At(i)
		var attrs *fskit.Attributes
		if wanted != 0 {
			a, aerr := vol.GetAttributes(child, fskit.GetAttributesRequest{Wanted: wanted})
			if aerr != nil {
				return 0, aerr
			}
			attrs = &a
		}
		if !packer.PackEntry(child.Name(), itemType(child.Kind()), fskit.ItemID(child.ID()), fskit.DirCookie(i+1), attrs) {
			break
		}
	}
	return readOnlyVerifier, nil
}

func (vol *Volume) Capabilities() fskit.Capabilities {
	return fskit.Capabilities{
		// Declared case-sensitive: no collision handling applies
		// (REQ-proj-case).
		CaseFormat:               fskit.CaseSensitive,
		SymbolicLinks:            true,
		HardLinks:                true,
		FilesLargerThan2GB:       true,
		SixtyFourBitObjectIDs:    true,
		NoSettingFilePermissions: true,
		NoImmutableFiles:         true,
		// Statistics reports no volume sizes: the projection is a
		// cache-backed view, not a sized device.
		NoVolumeSizes: true,
	}
}

func (vol *Volume) Statistics() (fskit.StatFS, error) {
	return fskit.StatFS{BlockSize: 4096, IOSize: 1 << 16}, nil
}

func (vol *Volume) PathConf() fskit.PathConf {
	return fskit.PathConf{
		MaxLinkCount:  -1,
		MaxNameLength: -1,
		MaxFileSize:   math.MaxInt64,
		MaxXattrSize:  -1,
	}
}

// Read serves blob bytes at offset: a short count (including 0)
// with a nil error signals EOF (REQ-proj-content: short reads only
// at EOF).
func (vol *Volume) Read(item fskit.Item, offset int64, buf []byte) (int, error) {
	e, err := asEntry(item)
	if err != nil {
		return 0, err
	}
	if e.Kind() == projection.KindDir {
		return 0, fskit.EISDIR
	}
	if e.Kind() != projection.KindFile || offset < 0 {
		return 0, fskit.EINVAL
	}
	f, err := os.Open(vol.blobPath(e.ContentDigest()))
	if err != nil {
		// Local trust boundary: a missing blob is store damage.
		return 0, fskit.EIO
	}
	defer f.Close()
	n, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return 0, fskit.EIO
	}
	return n, nil
}

func (vol *Volume) Write(item fskit.Item, offset int64, data []byte) (int, error) {
	return 0, fskit.EROFS
}
