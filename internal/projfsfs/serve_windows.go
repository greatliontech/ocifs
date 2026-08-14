//go:build windows && amd64

package projfsfs

import (
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	projfs "github.com/greatliontech/projfs-go"
	"golang.org/x/sys/windows"

	"github.com/greatliontech/ocifs/internal/projection"
)

// epoch presents unrecorded modification times (REQ-proj-fidelity).
var epoch = time.Unix(0, 0)

// Server is one live ProjFS projection: Wait blocks until Unmount
// stops the virtualization instance. Unmount is idempotent.
type Server struct {
	inst *projfs.Instance
	pr   *provider
	root string
	done chan struct{}
	stop sync.Once
}

func (s *Server) Wait() { <-s.done }

// Unmount flushes the report, classifies on-disk placeholder residue
// while the instance can still be asked, stops serving, and removes
// the residue — the mountpoint retains only residual foreign files
// and the directory spine containing them (REQ-api-mountpoint; the
// root's reparse marking may persist and a later mount tolerates
// it). The classification walk materializes a placeholder per
// projected directory it descends — an unmount-time cost bounded by
// tree size; regular files stay virtual.
func (s *Server) Unmount() error {
	s.stop.Do(func() {
		s.pr.flushReport()
		residue := placeholderResidue(s.inst, s.root)
		s.inst.Stop()
		for _, p := range residue {
			// Deepest-first; virtual entries vanished with the
			// instance and directories above foreign files stay as
			// their spine, so not-found and not-empty are expected.
			if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
				// Metadata dirt can leave a read-only placeholder;
				// clear and retry once. A placeholder held open by a
				// foreign process can still survive — noted in the
				// validation checklist.
				os.Chmod(p, 0o666)
				os.Remove(p)
			}
		}
		// Tombstones (deleted foreign names) hide from the live walk
		// by design; after Stop they surface as reparse artifacts.
		// Sweep strictly by the platform's ProjFS reparse tags —
		// never by Go's irregular-mode class, which also covers
		// foreign junctions and cloud-file links that are retained
		// residual state.
		filepath.WalkDir(s.root, func(path string, d fs.DirEntry, err error) error {
			if err != nil || path == s.root {
				return nil
			}
			if tag, ok := reparseTagOf(path); ok && (tag == reparseTagProjFS || tag == reparseTagProjFSTombstone) {
				os.Remove(path)
			}
			return nil
		})
		close(s.done)
	})
	return nil
}

const (
	// The platform's ProjFS reparse tags: placeholder state and
	// tombstones — the only classes the unmount sweep may remove.
	reparseTagProjFS          = 0x9000001C
	reparseTagProjFSTombstone = 0xA0000022
)

// fileAttributeTagInfo mirrors the platform's
// FILE_ATTRIBUTE_TAG_INFO layout (x/sys exports the info class
// constant but not the struct).
type fileAttributeTagInfo struct {
	FileAttributes uint32
	ReparseTag     uint32
}

// reparseTagOf returns the reparse tag of path, opening the reparse
// point itself (never following it).
func reparseTagOf(path string) (uint32, bool) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, false
	}
	h, err := windows.CreateFile(p, 0,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING,
		windows.FILE_FLAG_OPEN_REPARSE_POINT|windows.FILE_FLAG_BACKUP_SEMANTICS, 0)
	if err != nil {
		return 0, false
	}
	defer windows.CloseHandle(h)
	var info fileAttributeTagInfo
	if err := windows.GetFileInformationByHandleEx(h, windows.FileAttributeTagInfo,
		(*byte)(unsafe.Pointer(&info)), uint32(unsafe.Sizeof(info))); err != nil {
		return 0, false
	}
	if info.FileAttributes&windows.FILE_ATTRIBUTE_REPARSE_POINT == 0 {
		return 0, false
	}
	return info.ReparseTag, true
}

// placeholderResidue walks the live projection root and collects
// every on-disk path the platform classifies as placeholder state —
// ours to remove; anything Full is a residual foreign file and
// stays (REQ-proj-ro). Tombstones hide from the live walk and are
// swept by tag after Stop. Classification errors keep the path
// (conservative: never delete what cannot be proven ours). The
// platform resolves state paths relative to the virtualization
// root.
func placeholderResidue(inst *projfs.Instance, root string) []string {
	var paths []string
	filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || path == root {
			return nil
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			return nil
		}
		st, serr := inst.GetOnDiskFileState(rel)
		if serr == nil && st.IsPlaceholder() {
			paths = append(paths, path)
		}
		return nil
	})
	sort.Slice(paths, func(i, j int) bool { return len(paths[i]) > len(paths[j]) })
	return paths
}

// Serve starts projecting p at root — an existing directory that
// becomes the virtualization root (the caller's mountpoint,
// REQ-api-mountpoint). The projection's report was already persisted
// at reportPath; the server republishes it as read-only residuals
// accumulate (REQ-proj-ro, REQ-proj-report).
func Serve(p *projection.Projection, blobPath func(v1.Hash) string, reportPath, root string) (*Server, error) {
	var instPtr atomic.Pointer[projfs.Instance]
	pr := &provider{
		p:          p,
		blobPath:   blobPath,
		symlinks:   p.Capabilities().Symlinks,
		instp:      &instPtr,
		enums:      map[projfs.GUID]*enumSession{},
		report:     p.Report(),
		reportPath: reportPath,
		recorded:   map[string]bool{},
	}
	inst, err := projfs.Start(root, pr, &projfs.Options{
		UseNegativePathCache: true,
		NotificationMappings: []projfs.NotificationMapping{{
			// The whole virtualization root: the four deniable
			// pre-operations for vetoing, and the observable residual
			// kinds for report recording (REQ-proj-ro).
			Mask: projfs.NotifyPreDelete | projfs.NotifyPreRename |
				projfs.NotifyPreSetHardlink | projfs.NotifyFilePreConvertToFull |
				projfs.NotifyNewFileCreated | projfs.NotifyFileOverwritten |
				projfs.NotifyFileHandleClosedFileModified |
				projfs.NotifyFileRenamed | projfs.NotifyHardlinkCreated,
			Root: "",
		}},
	})
	if err != nil {
		return nil, err
	}
	// ProjFS can dispatch callbacks before Start returns (OS probes
	// land within milliseconds); instance-dependent callbacks wait
	// out this window on the pointer with a bounded spin.
	instPtr.Store(inst)
	return &Server{inst: inst, pr: pr, root: root, done: make(chan struct{})}, nil
}
