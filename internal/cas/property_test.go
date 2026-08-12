package cas

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	"pgregory.net/rapid"
)

// TestPropertyPutRoundtrip pins REQ-store-cas-content as a for-all:
// whatever bytes go in, the entry is stored at the key those bytes
// hash to and reads back identical.
func TestPropertyPutRoundtrip(t *testing.T) {
	c := mustNew(t)
	rapid.Check(t, func(rt *rapid.T) {
		content := rapid.SliceOfN(rapid.Byte(), 0, 4096).Draw(rt, "content")
		key, n, err := c.Put(bytes.NewReader(content))
		if err != nil {
			rt.Fatal(err)
		}
		if n != int64(len(content)) {
			rt.Fatalf("size %d, want %d", n, len(content))
		}
		sum := sha256.Sum256(content)
		if key.Algorithm != "sha256" || key.Hex != hex.EncodeToString(sum[:]) {
			rt.Fatalf("key %v is not the content hash", key)
		}
		got, err := os.ReadFile(c.Path(key))
		if err != nil {
			rt.Fatal(err)
		}
		if !bytes.Equal(got, content) {
			rt.Fatalf("stored bytes differ from input")
		}
	})
}
