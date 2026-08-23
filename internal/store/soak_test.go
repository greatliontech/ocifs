//go:build linux

package store

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
)

// The crash harness: child processes churn a SHARED store —
// ingesting or collecting — and die by SIGKILL at arbitrary points;
// after every kill the store must hold the crash model: rooted
// content serves whole, debris reclaims, and a fresh sweep
// completes. The children exec the test binary (env-gated);
// PDEATHSIG prevents leaks.

const (
	soakChildEnv  = "OCIFS_STORE_SOAK_CHILD"
	soakStoreEnv  = "OCIFS_STORE_SOAK_DIR"
	soakServerEnv = "OCIFS_STORE_SOAK_REGISTRY"
)

// soakChild runs one churn worker until killed.
func soakChild(t *testing.T) {
	mode := os.Getenv(soakChildEnv)
	dir := os.Getenv(soakStoreEnv)
	s, err := NewStore(dir, anonKeychain{}, PullIfNotPresent, v1.Platform{}, nil, true, 0)
	if err != nil {
		fmt.Println("child:", err)
		os.Exit(1)
	}
	defer s.Close()
	fmt.Println("ready")
	ctx := context.Background()
	switch mode {
	case "ingest":
		s.transport = remoteTransport(os.Getenv(soakServerEnv))
		for i := 0; ; i++ {
			ref := fmt.Sprintf("%s/soak/img%d:v1", testHost, i%8)
			_, _ = s.Image(ctx, ref, nil)
			if i%4 == 3 {
				_ = s.RemoveRef(ctx, ref)
			}
		}
	case "collect":
		for {
			_, _ = s.Collect(ctx, CollectOpts{Grace: -1})
		}
	}
	os.Exit(0)
}

// TestCrashStormSharedStore is the kill-storm harness: ingesting
// and collecting children die mid-flight repeatedly; the survivors'
// invariants hold after every round.
func TestCrashStormSharedStore(t *testing.T) {
	if os.Getenv(soakChildEnv) != "" {
		soakChild(t)
		return
	}
	if os.Getenv("OCIFS_MUTATION_CAMPAIGN") != "" {
		t.Skip("process-killing soak skipped under mutation campaign")
	}
	if testing.Short() {
		t.Skip("soak skipped in short mode")
	}

	reg := newTestRegistry()
	srv := serveRegistry(t, reg)
	for i := 0; i < 8; i++ {
		push(t, reg, fmt.Sprintf("%s/soak/img%d:v1", testHost, i),
			makeImage(t, newRawLayer(t, tarBytes(t, tfile("f", fmt.Sprintf("payload-%d", i))))))
	}
	// Per-round probe tags: uncached, so each round's post-kill
	// probe is a guaranteed FULL ingest through lease and root
	// publication.
	for r := 0; r < 6; r++ {
		push(t, reg, fmt.Sprintf("%s/soak/probe:r%d", testHost, r),
			makeImage(t, newRawLayer(t, tarBytes(t, tfile("p", fmt.Sprintf("probe-%d", r))))))
	}
	// The anchor image stays rooted throughout; its integrity is the
	// end-to-end crash-model check.
	anchor := testHost + "/soak/anchor:v1"
	push(t, reg, anchor, makeImage(t, newRawLayer(t, tarBytes(t, tfile("precious", "must-survive")))))

	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullIfNotPresent, v1.Platform{}, nil, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	s.transport = handlerTransport{h: reg.h}
	if _, err := s.Image(context.Background(), anchor, nil); err != nil {
		t.Fatal(err)
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	spawn := func(mode string) *exec.Cmd {
		cmd := exec.Command(exe, "-test.run", "^TestCrashStormSharedStore$")
		cmd.Env = append(os.Environ(),
			soakChildEnv+"="+mode,
			soakStoreEnv+"="+dir,
			soakServerEnv+"="+srv)
		// PDEATHSIG is thread-scoped (a retiring spawner thread
		// would kill the child early) — sub-second child lifetimes
		// make that acceptable here.
		cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
		var errBuf strings.Builder
		cmd.Stderr = &errBuf
		out, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cmd.Process.Kill()
			cmd.Wait()
		})
		buf := make([]byte, 64)
		n, rerr := out.Read(buf)
		if !strings.Contains(string(buf[:n]), "ready") {
			t.Fatalf("%s child never became ready: %q (read err %v, stderr %q)", mode, buf[:n], rerr, errBuf.String())
		}
		return cmd
	}

	for round := 0; round < 6; round++ {
		ingester := spawn("ingest")
		collector := spawn("collect")
		time.Sleep(time.Duration(50+round*40) * time.Millisecond)
		ingester.Process.Kill()
		collector.Process.Kill()
		ingester.Wait()
		collector.Wait()

		// Crash model: debris reclaims, a full sweep completes, the
		// rooted anchor serves whole — and the machinery the dead
		// children were exercising still works: a fresh ingest
		// succeeds after every kill (a wedged lease, a persistent
		// condemned backout, or a corrupt keyspace would surface
		// here, where the children's swallowed errors cannot hide
		// it).
		if _, err := s.DebrisSweep(context.Background()); err != nil {
			t.Fatalf("round %d debris: %v", round, err)
		}
		probeRef := fmt.Sprintf("%s/soak/probe:r%d", testHost, round)
		if _, err := s.Image(context.Background(), probeRef, nil); err != nil {
			t.Fatalf("round %d: ingest wedged after kills: %v", round, err)
		}
		if _, err := s.Collect(context.Background(), CollectOpts{Grace: -1}); err != nil {
			t.Fatalf("round %d collect: %v", round, err)
		}
		img, err := s.Image(context.Background(), anchor, nil)
		if err != nil {
			t.Fatalf("round %d: anchor lost: %v", round, err)
		}
		if got := string(readEntry(t, s, img, "precious")); got != "must-survive" {
			t.Fatalf("round %d: anchor content %q", round, got)
		}
	}

	// Post-storm, every churn image still ingests — end-to-end
	// proof the shared store's write machinery survived the storm.
	for i := 0; i < 8; i++ {
		ref := fmt.Sprintf("%s/soak/img%d:v1", testHost, i)
		if _, err := s.Image(context.Background(), ref, nil); err != nil {
			t.Fatalf("post-storm ingest of %s: %v", ref, err)
		}
		if err := s.RemoveRef(context.Background(), ref); err != nil {
			t.Fatal(err)
		}
	}

	// The offline arm: one final grace-ignored pass, then the rooted
	// anchor must serve from local state alone with the network cut —
	// self-heal-by-refetch cannot mask content the sweep wrongly
	// collects.
	if _, err := s.Collect(context.Background(), CollectOpts{Grace: -1}); err != nil {
		t.Fatal(err)
	}
	offline := newStoreAt(t, dir, PullNever, v1.Platform{}, cutTransport(t))
	img, err := offline.Image(context.Background(), anchor, nil)
	if err != nil {
		t.Fatalf("anchor lost offline after the storm: %v", err)
	}
	if got := string(readEntry(t, offline, img, "precious")); got != "must-survive" {
		t.Fatalf("anchor content offline: %q", got)
	}
}

// TestReadersSurviveCollection pins cross-process readers during
// collection: a reader loops over its rooted image while the parent
// collects aggressively with rotating removals of other refs — the
// reader must never fail.
func TestReadersSurviveCollection(t *testing.T) {
	if os.Getenv("OCIFS_MUTATION_CAMPAIGN") != "" {
		t.Skip("process soak skipped under mutation campaign")
	}
	reg := newTestRegistry()
	keep := testHost + "/soak/keeper:v1"
	push(t, reg, keep, makeImage(t, newRawLayer(t, tarBytes(t, tfile("k", "keeper-bytes")))))
	dir := scratchDir(t)
	s, err := NewStore(dir, anonKeychain{}, PullIfNotPresent, v1.Platform{}, nil, true, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	s.transport = reg
	if _, err := s.Image(context.Background(), keep, nil); err != nil {
		t.Fatal(err)
	}

	// The reader is a second handle — cross-process semantics over
	// gmdb coordination (same-process handles are gmdb's own
	// documented cross-process proxy).
	reader, err := NewStore(dir, anonKeychain{}, PullNever, v1.Platform{}, nil, false, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { reader.Close() })

	// One full writer cycle lands BEFORE the reader starts, so the
	// overlap floor is at least one collection under the reader's
	// feet regardless of relative speeds.
	writerCycle := func(i int) {
		ref := fmt.Sprintf("%s/soak/churn%d:v1", testHost, i%4)
		push(t, reg, ref, makeImage(t, newRawLayer(t, tarBytes(t, tfile("c", fmt.Sprintf("churn-%d", i))))))
		if _, err := s.Image(context.Background(), ref, nil); err != nil {
			t.Fatal(err)
		}
		if err := s.RemoveRef(context.Background(), ref); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Collect(context.Background(), CollectOpts{Grace: -1}); err != nil {
			t.Fatal(err)
		}
	}
	writerCycle(0)

	stop := make(chan struct{})
	done := make(chan error, 1)
	// The wait cleanup registers AFTER the reader's handle, so it
	// runs BEFORE the handle closes (LIFO): a writer-side Fatal
	// never leaves the goroutine reading through a closed store.
	t.Cleanup(func() {
		close(stop)
		<-done
	})
	go func() {
		for i := 0; i < 200; i++ {
			select {
			case <-stop:
				done <- nil
				return
			default:
			}
			img, err := reader.Image(context.Background(), keep, nil)
			if err != nil {
				done <- fmt.Errorf("iteration %d: %w", i, err)
				return
			}
			view, err := img.Unify()
			if err != nil {
				done <- err
				return
			}
			e, _ := view.Lookup("k")
			b, err := os.ReadFile(reader.BlobPath(e.Digest))
			if err != nil || string(b) != "keeper-bytes" {
				done <- fmt.Errorf("iteration %d: content %q %v", i, b, err)
				return
			}
		}
		done <- nil
	}()

	for i := 1; ; i++ {
		select {
		case err := <-done:
			done <- nil // re-arm for the cleanup's drain
			if err != nil {
				t.Fatal(err)
			}
			return
		default:
		}
		writerCycle(i)
	}
}

// remoteTransport adapts the child to the parent's in-process
// registry: the child dials the real HTTP listener the parent
// serves.
func remoteTransport(addr string) roundTripperFunc {
	return func(req *http.Request) (*http.Response, error) {
		req = req.Clone(req.Context())
		req.URL.Scheme = "http"
		req.URL.Host = addr
		return http.DefaultTransport.RoundTrip(req)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// serveRegistry exposes the in-process registry on a real listener
// for child processes.
func serveRegistry(t *testing.T, reg handlerTransport) string {
	t.Helper()
	srv := httptest.NewServer(reg.h)
	t.Cleanup(srv.Close)
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	return u.Host
}
