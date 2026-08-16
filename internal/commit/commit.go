//go:build linux || darwin

// Package commit serializes the canonical diff of a base view and an
// upper state as one deterministic uncompressed OCI tar layer
// (docs/specs/writable.md REQ-writable-commit): an entry is emitted
// exactly when its presented kind, content, or attributes differ
// from the base at that path — or when the hardlink rule forces it —
// whiteouts exactly for base paths the upper deletes, opaque markers
// where they have effect, everything in sorted path order with fixed
// header layout and no commit-time inputs, so equal (base, upper)
// pairs commit to byte-identical layers whatever write history or
// platform produced them (projection.md REQ-proj-commit-neutral).
// The diff consumes the abstract upper state (internal/upper), the
// same shape an OS-native dialect reader produces.
package commit

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/layer"
	"github.com/greatliontech/ocifs/internal/upper"
)

// epoch is the fixed timestamp marker entries carry: markers have no
// presented source, and their upper files' host attributes are pure
// write history (REQ-writable-commit).
var epoch = time.Unix(0, 0)

// Layer writes the canonical diff layer to w. The base view supplies
// the comparison truth; the upper state supplies the presented
// modifications. The result is deterministic: byte-identical for
// equal inputs.
func Layer(base *layer.View, up *upper.State, w io.Writer) error {
	tw := tar.NewWriter(w)

	type emission struct {
		path string
		hdr  tar.Header
		src  string // host path for file content ("" for none)
	}
	var out []emission
	emit := func(p string, hdr tar.Header, src string) {
		hdr.Name = p
		hdr.Format = tar.FormatPAX
		out = append(out, emission{path: p, hdr: hdr, src: src})
	}

	// Hardlink groups: paths sharing an upper inode with more than
	// one member emit as one content entry (sorted-first) plus link
	// entries (REQ-writable-hardlink).
	groups := map[uint64][]string{}
	for p, e := range up.Entries {
		if e.Kind == upper.KindFile && !e.StandIn && e.Nlink > 1 {
			groups[e.Ino] = append(groups[e.Ino], p)
		}
	}
	linkTarget := map[string]string{} // member path -> sorted-first path
	groupEmits := map[string]bool{}   // sorted-first paths force-emitted
	for _, members := range groups {
		if len(members) < 2 {
			continue
		}
		sort.Strings(members)
		anyDiffers := false
		for _, p := range members {
			if entryDiffers(base, up, p) {
				anyDiffers = true
			}
		}
		if !anyDiffers {
			continue
		}
		first := members[0]
		groupEmits[first] = true
		for _, p := range members[1:] {
			linkTarget[p] = first
		}
	}

	// Whiteouts: exactly where deleted base content's occlusion
	// depends on them — skipped when there is no base entry, when an
	// ancestor marker or opaque already hides it (a dismantled and an
	// undismantled rmdir interior must commit identically), or when a
	// non-directory upper entry at the path already finalizes it
	// (REQ-writable-delete's rendering through the layer dialect).
	for p := range up.Whiteouts {
		if _, ok := base.Lookup(p); !ok {
			continue // no base entry: the marker has no effect
		}
		if ancestorOccluded(up, p) {
			continue // base already hidden: the marker adds nothing
		}
		if e, ok := up.Entries[p]; ok && e.Kind != upper.KindDir && e.Kind != upper.KindSocket {
			// A non-directory entry finalizes the path — except a
			// socket, which never commits: its base shadow must
			// still be deleted or the committed image resurrects it.
			continue
		}
		dir, name := path.Split(p)
		emit(dir+upper.WhiteoutPrefix+name, tar.Header{
			Typeflag: tar.TypeReg,
			Mode:     0,
			Uid:      0,
			Gid:      0,
			ModTime:  epoch,
			Size:     0,
		}, "")
	}

	// A socket shadowing a base entry commits that entry's whiteout
	// even when the upper holds no marker (a caller-built upper can
	// place a bare socket at a base path): the socket itself never
	// commits, so only the marker keeps the base entry hidden
	// (REQ-writable-commit).
	for p, e := range up.Entries {
		if e.Kind != upper.KindSocket || up.Whiteouts[p] {
			continue
		}
		if _, ok := base.Lookup(p); !ok {
			continue
		}
		if ancestorOccluded(up, p) {
			continue
		}
		dir, name := path.Split(p)
		emit(dir+upper.WhiteoutPrefix+name, tar.Header{
			Typeflag: tar.TypeReg,
			Mode:     0,
			Uid:      0,
			Gid:      0,
			ModTime:  epoch,
			Size:     0,
		}, "")
	}

	// Opaque markers: emitted where they have effect — the upper
	// holds the directory, the base holds anything beneath it, and
	// that base content is not already hidden by the directory's own
	// whiteout or an ancestor's occlusion.
	for p := range up.Opaque {
		if _, ok := up.Entries[p]; !ok {
			continue
		}
		if !baseHasChildren(base, p) {
			continue
		}
		if up.Whiteouts[p] || ancestorOccluded(up, p) {
			continue
		}
		emit(p+"/"+upper.OpaqueMarker, tar.Header{
			Typeflag: tar.TypeReg,
			Mode:     0,
			Uid:      0,
			Gid:      0,
			ModTime:  epoch,
			Size:     0,
		}, "")
	}

	// Entries: emit-iff-differs, plus the hardlink force-emit.
	for p, e := range up.Entries {
		if e.Kind == upper.KindSocket {
			// The tar dialect has no socket type; sockets serve the
			// live mount only and are omitted from committed layers
			// (REQ-writable-commit).
			continue
		}
		if _, isLink := linkTarget[p]; isLink {
			// Link members: emitted iff they differ (or their group
			// forced), as TypeLink onto the sorted-first path.
			if !entryDiffers(base, up, p) && !groupEmits[linkTarget[p]] {
				continue
			}
			hdr := headerFor(e)
			hdr.Typeflag = tar.TypeLink
			hdr.Linkname = linkTarget[p]
			hdr.Size = 0
			emit(p, hdr, "")
			continue
		}
		if !groupEmits[p] && !entryDiffers(base, up, p) {
			continue
		}
		hdr := headerFor(e)
		src := ""
		if e.Kind == upper.KindFile && !e.StandIn {
			src = e.HostPath
		}
		emit(p, hdr, src)
	}

	// The root: a recorded root (REQ-writable-dialect) commits as the
	// layer's "./" entry exactly when its presented attributes differ
	// from the base root's; an unrecorded root commits nothing.
	if up.Root != nil && rootDiffers(base, up.Root) {
		hdr := headerFor(*up.Root)
		hdr.Typeflag = tar.TypeDir
		hdr.Size = 0
		emit("./", hdr, "")
	}

	sort.Slice(out, func(i, j int) bool { return out[i].path < out[j].path })
	for _, em := range out {
		if err := tw.WriteHeader(&em.hdr); err != nil {
			return err
		}
		if em.src != "" {
			f, err := os.Open(em.src)
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, f); err != nil {
				f.Close()
				return fmt.Errorf("commit: entry %q: %w", em.hdr.Name, err)
			}
			f.Close()
		}
	}
	return tw.Close()
}

// headerFor renders one presented upper entry as its genuine tar
// header: overrides resolved into fields, real xattr names, never
// the machinery namespace (REQ-writable-commit).
func headerFor(e upper.Entry) tar.Header {
	hdr := tar.Header{
		Mode:    int64(e.Mode),
		Uid:     e.UID,
		Gid:     e.GID,
		ModTime: e.ModTime,
	}
	switch e.Kind {
	case upper.KindFile:
		hdr.Typeflag = tar.TypeReg
		hdr.Size = e.Size
	case upper.KindDir:
		hdr.Typeflag = tar.TypeDir
	case upper.KindSymlink:
		hdr.Typeflag = tar.TypeSymlink
		hdr.Linkname = e.Target
	case upper.KindFifo:
		hdr.Typeflag = tar.TypeFifo
	case upper.KindCharDev:
		hdr.Typeflag = tar.TypeChar
		hdr.Devmajor = int64(e.Rdev.Major)
		hdr.Devminor = int64(e.Rdev.Minor)
	case upper.KindBlockDev:
		hdr.Typeflag = tar.TypeBlock
		hdr.Devmajor = int64(e.Rdev.Major)
		hdr.Devminor = int64(e.Rdev.Minor)
	}
	if len(e.Xattrs) > 0 {
		hdr.PAXRecords = map[string]string{}
		for k, v := range e.Xattrs {
			hdr.PAXRecords["SCHILY.xattr."+k] = v
		}
	}
	return hdr
}

// entryDiffers reports whether the upper entry at p presents
// differently from the base entry at p — kind, content, target,
// device, mode, ownership, mtime, or xattrs (REQ-writable-commit's
// emit-iff-differs).
func entryDiffers(base *layer.View, up *upper.State, p string) bool {
	e := up.Entries[p]
	if e.Kind == upper.KindSocket {
		return false // sockets never commit (headerFor)
	}
	be, ok := base.Lookup(p)
	if !ok {
		return true
	}
	if up.Whiteouts[p] {
		// A recreated entry beside its marker is a new object; the
		// base beneath is occluded — always a difference.
		return true
	}
	if ancestorOccluded(up, p) {
		// The base entry is hidden by an ancestor marker or opaque:
		// nothing shows through to be restored to, so the upper
		// entry is new content whatever its attributes.
		return true
	}
	bk, ok := baseKind(be.Header.Typeflag)
	if !ok || bk != e.Kind {
		return true
	}
	bmt := be.Header.ModTime
	if bmt.IsZero() {
		// Synthesized implied directories carry no recorded mtime;
		// their presented truth is the epoch, never a zero-time
		// artifact (projection.md's fidelity rules).
		bmt = epoch
	}
	if uint32(be.Header.Mode)&0o7777 != e.Mode ||
		be.Header.Uid != e.UID || be.Header.Gid != e.GID ||
		!bmt.Equal(e.ModTime) {
		return true
	}
	switch e.Kind {
	case upper.KindFile:
		if be.Header.Size != e.Size {
			return true
		}
		sum, err := hashFile(e.HostPath)
		if err != nil || sum != be.Digest.Hex {
			return true
		}
	case upper.KindSymlink:
		if be.Header.Linkname != e.Target {
			return true
		}
	case upper.KindCharDev, upper.KindBlockDev:
		if int64(e.Rdev.Major) != be.Header.Devmajor || int64(e.Rdev.Minor) != be.Header.Devminor {
			return true
		}
	}
	return !xattrsEqual(baseXattrs(&be), e.Xattrs)
}

// rootDiffers compares a recorded root's presented attributes with
// the base root's — the base's own root record when a layer wrote
// one, the synthesized plain root otherwise (0755, 0:0, epoch).
func rootDiffers(base *layer.View, r *upper.Entry) bool {
	mode, uid, gid := uint32(0o755), 0, 0
	mtime := epoch
	var xattrs map[string]string
	if be, ok := base.Lookup("."); ok {
		mode = uint32(be.Header.Mode) & 0o7777
		uid, gid = be.Header.Uid, be.Header.Gid
		if !be.Header.ModTime.IsZero() {
			mtime = be.Header.ModTime
		}
		xattrs = baseXattrs(&be)
	}
	if mode != r.Mode || uid != r.UID || gid != r.GID || !mtime.Equal(r.ModTime) {
		return true
	}
	return !xattrsEqual(xattrs, r.Xattrs)
}

func baseKind(flag byte) (upper.Kind, bool) {
	switch flag {
	case tar.TypeReg, tar.TypeLink:
		return upper.KindFile, true
	case tar.TypeDir:
		return upper.KindDir, true
	case tar.TypeSymlink:
		return upper.KindSymlink, true
	case tar.TypeFifo:
		return upper.KindFifo, true
	case tar.TypeChar:
		return upper.KindCharDev, true
	case tar.TypeBlock:
		return upper.KindBlockDev, true
	}
	return 0, false
}

// baseXattrs collects a base entry's presented xattrs through the
// shared machinery-inert rule (REQ-writable-reserved's base-content
// arm).
func baseXattrs(e *layer.Entry) map[string]string {
	return upper.PresentedBaseXattrs(&e.Header)
}

func xattrsEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

func hashFile(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ancestorOccluded is upper.State.AncestorOccluded — the shared
// base-visibility rule commit's elision and marker minimality follow.
func ancestorOccluded(up *upper.State, p string) bool {
	return up.AncestorOccluded(p)
}

// baseHasChildren reports whether the base view holds any entry
// strictly beneath dir.
func baseHasChildren(base *layer.View, dir string) bool {
	prefix := dir + "/"
	for _, e := range base.Entries() {
		if strings.HasPrefix(e.Header.Name, prefix) {
			return true
		}
	}
	return false
}

// LayerBytes is Layer into memory, plus the digest of the produced
// tar — the blob digest and diff ID are the same for an uncompressed
// layer (REQ-writable-commit-image).
func LayerBytes(base *layer.View, up *upper.State) ([]byte, v1.Hash, error) {
	var buf bytes.Buffer
	if err := Layer(base, up, &buf); err != nil {
		return nil, v1.Hash{}, err
	}
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), v1.Hash{Algorithm: "sha256", Hex: hex.EncodeToString(sum[:])}, nil
}
