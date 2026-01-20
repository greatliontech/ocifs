# OCIFS Architecture Review & Roadmap

## Executive Summary

OCIFS is a FUSE-based filesystem that mounts OCI container images. The writable layer implementation has several critical bugs that make it unreliable for production use. This document outlines the issues and a prioritized roadmap for fixes.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                      User Applications                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    FUSE Kernel Module                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    unionfs package                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │  UnionFS    │  │  unionDir   │  │     unionFile       │  │
│  │  (root)     │──│  (dirs)     │──│     (files)         │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                      store package                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐  │
│  │   Store     │  │   Image     │  │   WritableLayer     │  │
│  │  (OCI mgmt) │──│  (unified)  │──│   (CoW upper)       │  │
│  └─────────────┘  └─────────────┘  └─────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│            Disk: blobs/ (RO) + content/ (RW)                │
└─────────────────────────────────────────────────────────────┘
```

---

## Priority 1: Critical Bugs (Blocking)

### BUG-001: File Size Calculation is Fundamentally Broken
**Location:** `internal/unionfs/file.go:151`
**Severity:** Critical
**Impact:** File corruption, incorrect file sizes

**Current code:**
```go
uf.file.Hdr.Size = uf.file.Hdr.Size + int64(n)
```

**Problems:**
1. Uses `+=` which accumulates size on every write instead of tracking actual file size
2. Doesn't account for write offset - writing at offset 1000 with 100 bytes should result in size 1100, not oldSize+100
3. After CoW, size starts from original RO file size, not from 0 or actual copied size

**Fix:** Replace with:
```go
newSize := off + int64(n)
if newSize > uf.file.Hdr.Size {
    uf.file.Hdr.Size = newSize
}
```

---

### BUG-002: Stale File Reference After Copy-on-Write
**Location:** `internal/unionfs/file.go:105-157`
**Severity:** Critical
**Impact:** Writes may update wrong metadata, data loss

**Problem:** After CoW triggers:
1. `SetFile()` is called with RO file's header (line 112)
2. Returns a copy of the new writable File
3. But size update at line 151 uses `uf.file.Hdr.Size` which still has RO file's size
4. The flow doesn't properly sequence: copy file → update uf.file → then update size

**Current flow:**
```
1. Check if not writable
2. Copy file to writable layer
3. Set uf.isWritable = true
4. Write data
5. Update uf.file.Hdr.Size += n   ← Uses OLD size!
6. Call SetFile with updated header
```

**Fix:** After CoW, must update `uf.file` reference AND get actual size from disk:
```go
// After CoW copy completes
fi, _ := os.Stat(destPath)
uf.file = destFile
uf.file.Hdr.Size = fi.Size()  // Actual size after copy
uf.isWritable = true
```

---

### BUG-003: Race Condition on unionFile Fields
**Location:** `internal/unionfs/file.go`
**Severity:** Critical
**Impact:** Data corruption under concurrent access

**Problem:** `unionFile` has no synchronization:
- `uf.file` can be read/written concurrently
- `uf.isWritable` can be checked while another goroutine sets it
- Two concurrent writes can both trigger CoW, causing double-copy and data loss

**Fix:** Add mutex to `unionFile`:
```go
type unionFile struct {
    fs.Inode
    mu            sync.Mutex  // Protects all fields below
    pathInFs      string
    file          *store.File
    isWritable    bool
    // ...
}
```

---

### BUG-004: Truncate Not Implemented
**Location:** `internal/unionfs/file.go:73-76`
**Severity:** High
**Impact:** Many applications fail (editors, build tools)

**Current:** Returns `ENOSYS`

**Fix:** Implement truncate with CoW support:
1. If file is RO, trigger CoW first
2. Call `os.Truncate()` on writable path
3. Update `uf.file.Hdr.Size`

---

## Priority 2: Missing Core Operations

### MISSING-001: Setattr Not Implemented
**Impact:** `chmod`, `chown`, `touch` don't work

Need to implement `NodeSetattrer` interface:
- Mode changes (chmod)
- Owner changes (chown)
- Time updates (touch/utimens)
- Size changes (truncate via setattr)

---

### MISSING-002: Rename Not Implemented
**Impact:** `mv` command fails, many applications broken

Need to implement `NodeRenamer` interface:
- Same-directory rename
- Cross-directory rename (move)
- Handle RO→writable promotion
- Handle whiteouts for source if in RO layer

---

### MISSING-003: Rmdir Not Implemented
**Impact:** Cannot remove directories

Need to implement `NodeRmdirer` interface:
- Check directory is empty
- Handle whiteouts for RO directories

---

### MISSING-004: Symlink Support
**Impact:** Symlinks don't work correctly

Need to implement:
- `NodeSymlinker` - create symlinks
- `NodeReadlinker` - read symlink targets
- Proper handling of symlinks in RO layers

---

## Priority 3: Robustness & Reliability

### ROBUST-001: Periodic Metadata Persistence
**Current:** Metadata only persists on clean unmount
**Risk:** Crash = total metadata loss

**Fix:**
- Add periodic flush (every N seconds or M writes)
- Write to temp file, then atomic rename
- Consider write-ahead log for crash recovery

---

### ROBUST-002: Fsync Support
**Location:** Need new interface implementation
**Impact:** Data integrity guarantees broken

Implement `NodeFsyncer`:
- Flush file data to disk
- Update and persist metadata
- Return only after durable

---

### ROBUST-003: Atomic Metadata + Content
**Current:** Content written immediately, metadata only on unmount
**Risk:** Inconsistent state on crash

**Fix:**
- Keep content in staging until metadata updated
- Or use WAL approach
- At minimum: persist metadata after each SetFile

---

### ROBUST-004: File Handle Reference Counting
**Current:** File handles are independent
**Risk:** Stale handles after unlink

**Fix:**
- Track open handles per inode
- Defer actual deletion until handles closed
- Handle unlink-while-open pattern

---

## Priority 4: Performance & Polish

### PERF-001: Inefficient Full-File CoW
**Current:** Entire file copied on first write
**Impact:** Slow for large files

**Future:** Consider:
- Block-level CoW
- Lazy copy (copy blocks on demand)
- reflink if supported by underlying FS

---

### PERF-002: Metadata Caching Inefficiency
**Current:** Full copy on every GetFile/SetFile
**Impact:** Memory churn, GC pressure

**Fix:** Use copy-on-write semantics internally or reference counting

---

### POLISH-001: Hardlink Support
**Current:** Not supported
**Impact:** Some applications expect hardlinks

---

### POLISH-002: Extended Attributes (xattr)
**Current:** Not supported
**Impact:** Security contexts, capabilities don't work

---

## Implementation Roadmap

### Phase 1: Critical Bug Fixes (Must Do First)
1. [ ] BUG-001: Fix size calculation
2. [ ] BUG-002: Fix file reference after CoW
3. [ ] BUG-003: Add mutex to unionFile
4. [ ] BUG-004: Implement Truncate

### Phase 2: Core Operations
5. [ ] MISSING-001: Implement Setattr
6. [ ] MISSING-002: Implement Rename
7. [ ] MISSING-003: Implement Rmdir
8. [ ] MISSING-004: Symlink support

### Phase 3: Robustness
9. [ ] ROBUST-002: Implement Fsync
10. [ ] ROBUST-001: Periodic persistence
11. [ ] ROBUST-003: Atomic metadata
12. [ ] ROBUST-004: Handle reference counting

### Phase 4: Polish
13. [ ] PERF-001: Optimize CoW
14. [ ] PERF-002: Optimize metadata caching
15. [ ] POLISH-001: Hardlinks
16. [ ] POLISH-002: Extended attributes

---

## Testing Strategy

### Unit Tests Needed
- [ ] Write at various offsets, verify size
- [ ] Concurrent writes to same file
- [ ] CoW triggers correctly
- [ ] Truncate (grow and shrink)
- [ ] Unlink while file open
- [ ] Rename within and across directories

### Integration Tests
- [ ] Mount, write, unmount, remount, verify
- [ ] Crash simulation (kill -9), verify recovery
- [ ] Run real workloads (compile, extract tarball)

### Stress Tests
- [ ] Many concurrent writers
- [ ] Rapid mount/unmount cycles
- [ ] Large file operations

---

## Quick Reference: Current Limitations

| Operation | Status | Notes |
|-----------|--------|-------|
| Read | ✅ Works | Direct from blobs |
| Write | ⚠️ Buggy | Size tracking broken |
| Create | ✅ Works | |
| Mkdir | ✅ Works | |
| Unlink | ✅ Works | Whiteouts handled |
| Truncate | ❌ Missing | Returns ENOSYS |
| Setattr | ❌ Missing | chmod/chown broken |
| Rename | ❌ Missing | mv fails |
| Rmdir | ❌ Missing | |
| Symlink | ❌ Missing | |
| Readlink | ❌ Missing | |
| Fsync | ❌ Missing | |
| Hardlink | ❌ Missing | |
| Xattr | ❌ Missing | |
