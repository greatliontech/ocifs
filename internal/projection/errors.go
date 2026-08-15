package projection

// Errno is the kernel's symbolic error space: the small set of
// failure meanings every backend maps onto its own vocabulary
// (syscall errno on FUSE, fskit.Errno on FSKit, HRESULT on ProjFS),
// so one kernel condition surfaces consistently on every platform.
type Errno int

const (
	// ErrNotFound: the addressed entry is not presented.
	ErrNotFound Errno = iota + 1
	// ErrNotDir: a directory operation addressed a non-directory.
	ErrNotDir
	// ErrNotSupported: the operation is outside the backend's
	// declared capabilities.
	ErrNotSupported
	// ErrReadOnly: a mutation was attempted against a read-only
	// projection (REQ-proj-ro).
	ErrReadOnly
	// ErrIO: content or state could not be read.
	ErrIO
	// ErrIdentityRange: an upper inode number lies at or above the
	// upper-born partition base and cannot be represented without
	// aliasing (REQ-proj-identity's derivation envelope).
	ErrIdentityRange
)

func (e Errno) Error() string {
	switch e {
	case ErrNotFound:
		return "entry not found"
	case ErrNotDir:
		return "not a directory"
	case ErrNotSupported:
		return "not supported"
	case ErrReadOnly:
		return "read-only projection"
	case ErrIO:
		return "i/o error"
	case ErrIdentityRange:
		return "upper inode outside the representable identity range"
	default:
		return "unknown projection error"
	}
}
