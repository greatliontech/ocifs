package store

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"

	"github.com/greatliontech/ocifs/internal/atomicfile"
)

type referenceStore string

var emptyHash = v1.Hash{}

// Get returns (digest, true, nil) if present; ("", false, nil) if missing.
func (rc referenceStore) Get(ref name.Reference) (v1.Hash, bool, error) {
	b, err := os.ReadFile(rc.pathForRef(ref))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return emptyHash, false, nil
		}
		return emptyHash, false, err
	}
	h, err := v1.NewHash(string(b))
	if err != nil {
		return emptyHash, false, fmt.Errorf("invalid hash in ref store %q: %w", ref, err)
	}
	return h, true, nil
}

// Put publishes the ref -> digest entry atomically and durably: it
// is ingest's completion barrier (REQ-store-ingest-order), so it must
// never be observable half-written and must not become durable ahead
// of the content written before it.
func (rc referenceStore) Put(ref name.Reference, hash v1.Hash) error {
	p := rc.pathForRef(ref)
	return atomicfile.Write(p, strings.NewReader(hash.String()), 0o644)
}

// pathForRef lays a reference out at a fixed depth of three encoded
// components — registry, whole repository path, identifier — so no
// reference's directory chain can be a prefix of another's file
// (variable-depth nesting let a tag file and a sub-repository
// directory claim the same path).
func (rc referenceStore) pathForRef(ref name.Reference) string {
	return filepath.Join(string(rc),
		encodeRefComponent(strings.ToLower(ref.Context().RegistryStr())),
		encodeRefComponent(ref.Context().RepositoryStr()),
		encodeRefComponent(ref.Identifier()))
}

// maxEncodedComponent bounds a plain-encoded component: every
// supported filesystem holds 255-byte names, and 200 leaves headroom.
// A longer encoding falls back to the hashed form below.
const maxEncodedComponent = 200

// encodeRefComponent maps one reference component to one portable
// path element (REQ-store-layout). Plain form: every byte outside
// [a-z0-9._-] — plus a leading or trailing '.', plus the first byte
// of a Windows reserved device name — as lowercase %xx. Escaping '%'
// itself is what makes the mapping injective; escaping a leading dot
// makes the output a single ordinary path element ("." and ".."
// would otherwise collapse in filepath.Join and let a hostile
// reference climb out of the refs tier); escaping a trailing dot
// keeps distinct names distinct on Windows, whose path normalization
// silently strips trailing dots; escaping reserved device names
// (con, nul, com1…) keeps every element creatable there. A component
// whose plain encoding exceeds maxEncodedComponent is stored as "%h"
// plus the SHA-256 of the raw component — "%h" begins no plain
// encoding (every plain escape is followed by two hex digits), so
// the two forms never collide, and hashed names stay within
// filesystem name limits.
func encodeRefComponent(s string) string {
	escFirst := windowsReservedName(s)
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '.' && (i == 0 || i == len(s)-1), i == 0 && escFirst:
			fmt.Fprintf(&b, "%%%02x", c)
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
			b.WriteByte(c)
		default:
			// Lowercase hex keeps the whole path lowercase, so no
			// two produced paths can collide under case folding.
			fmt.Fprintf(&b, "%%%02x", c)
		}
	}
	if b.Len() > maxEncodedComponent {
		sum := sha256.Sum256([]byte(s))
		return "%h" + hex.EncodeToString(sum[:])
	}
	return b.String()
}

// windowsReservedName reports whether the component's first
// dot-segment is a Windows reserved device name. Only all-lowercase
// spellings can reach the plain encoding — any uppercase byte is
// escaped already.
func windowsReservedName(s string) bool {
	seg, _, _ := strings.Cut(s, ".")
	switch seg {
	case "con", "prn", "aux", "nul":
		return true
	}
	if len(seg) == 4 && (strings.HasPrefix(seg, "com") || strings.HasPrefix(seg, "lpt")) &&
		seg[3] >= '0' && seg[3] <= '9' {
		return true
	}
	return false
}
