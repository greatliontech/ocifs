package cas

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/greatliontech/ocifs/internal/scratchtest"
)

func mustNew(t *testing.T) *CAS {
	t.Helper()
	c, err := New(scratchtest.Dir(t, "cas"))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// listTemps returns leftover temporaries anywhere under the root.
func listTemps(t *testing.T, root string) []string {
	t.Helper()
	var temps []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), ".tmp-") {
			temps = append(temps, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return temps
}

func TestPutKeyIsContentHash(t *testing.T) {
	c := mustNew(t)
	content := []byte("hello cas")
	key, n, err := c.Put(bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(content)) {
		t.Fatalf("size %d, want %d", n, len(content))
	}
	sum := sha256.Sum256(content)
	if key.Algorithm != "sha256" || key.Hex != hex.EncodeToString(sum[:]) {
		t.Fatalf("key %v does not match content hash", key)
	}
	// The published bytes hash to the key (REQ-store-cas-content).
	got, err := os.ReadFile(c.Path(key))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("stored bytes differ from input")
	}
}

func TestPutEmpty(t *testing.T) {
	c := mustNew(t)
	key, n, err := c.Put(bytes.NewReader(nil))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("size %d, want 0", n)
	}
	f, err := c.Open(key)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 0 {
		t.Fatalf("stored size %d, want 0", fi.Size())
	}
}

func TestPutIdempotentKeepsFirst(t *testing.T) {
	c := mustNew(t)
	content := []byte("same bytes")
	key1, _, err := c.Put(bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	fi1, err := os.Stat(c.Path(key1))
	if err != nil {
		t.Fatal(err)
	}
	key2, _, err := c.Put(bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if key1 != key2 {
		t.Fatalf("keys differ: %v vs %v", key1, key2)
	}
	fi2, err := os.Stat(c.Path(key1))
	if err != nil {
		t.Fatal(err)
	}
	// Entries are immutable once written: the second Put must not
	// have replaced the file (same inode, not merely same-looking
	// metadata).
	if !os.SameFile(fi1, fi2) {
		t.Fatalf("existing entry was rewritten")
	}
	if temps := listTemps(t, c.root); len(temps) != 0 {
		t.Fatalf("leftover temporaries: %v", temps)
	}
}

type failReader struct{ err error }

func (r failReader) Read([]byte) (int, error) { return 0, r.err }

func TestPutErrorLeavesNoResidue(t *testing.T) {
	c := mustNew(t)
	boom := errors.New("boom")
	_, _, err := c.Put(io.MultiReader(strings.NewReader("partial"), failReader{boom}))
	if !errors.Is(err, boom) {
		t.Fatalf("err %v, want boom", err)
	}
	entries := 0
	err = filepath.WalkDir(c.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			entries++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// No temp litter and no partial blob published.
	if entries != 0 {
		t.Fatalf("%d files left after failed Put", entries)
	}
}

func TestOpenMissing(t *testing.T) {
	c := mustNew(t)
	_, err := c.Open(v1.Hash{Algorithm: "sha256", Hex: strings.Repeat("0", 64)})
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err %v, want not-exist", err)
	}
}

func TestConcurrentPutSameContent(t *testing.T) {
	c := mustNew(t)
	content := bytes.Repeat([]byte("racing"), 1024)
	var wg sync.WaitGroup
	keys := make([]v1.Hash, 8)
	errs := make([]error, 8)
	for i := range keys {
		wg.Add(1)
		go func() {
			defer wg.Done()
			keys[i], _, errs[i] = c.Put(bytes.NewReader(content))
		}()
	}
	wg.Wait()
	for i := range keys {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if keys[i] != keys[0] {
			t.Fatalf("key mismatch at %d", i)
		}
	}
	got, err := os.ReadFile(c.Path(keys[0]))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("stored bytes differ after concurrent puts")
	}
	if temps := listTemps(t, c.root); len(temps) != 0 {
		t.Fatalf("leftover temporaries: %v", temps)
	}
}
