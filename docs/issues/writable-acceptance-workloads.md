# Writable layer: real-workload acceptance and crash-consistency tests

The bar rises sharply when the union goes from serving read-only image
views to being a live container root. The honest acceptance test is a
package manager, not a unit suite:

- dpkg/apt install inside a container rooted on the mount: heavy
  rename+fsync discipline, hardlinks, maintainer scripts, and
  `security.capability` xattrs on installed binaries.
- Exec-from-written-file: install a binary through the mount, run it.
- mmap of written files; sockets and FIFOs created in the writable
  layer; concurrent writes under a parallel build (`make -j`).
- Crash consistency as property tests: kill the process at arbitrary
  points during write/rename/whiteout storms; on remount the layer is
  consistent — files may be missing, nothing is corrupt, the rebuilt
  index matches the dir (extends the branch's existing property_test
  coverage).
- Commit correctness under the same storms: a committed layer applied
  over the base image reproduces the presented tree exactly.

Lands: before the writable layer is declared production-ready.
