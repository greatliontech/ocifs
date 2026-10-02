package scratchtest

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/greatliontech/gmdb/oslock"
)

// childEnv tells a re-executed test binary to play the child: it
// announces itself, asks for the directory, announces that it is in,
// and stays until its stdin closes.
const childEnv = "OCIFS_SCRATCHTEST_CHILD"

// TestDirectoryHeldAcrossProcesses pins the directory hold: a
// stranger asking for a held scratch directory waits for the holder
// and enters once the holder lets go; the holder's own child, told
// of the hold through the environment, enters at once; a child told
// of a hold by a process not its parent waits like a stranger; a
// process told of a hold no one holds any more takes the hold
// itself.
func TestDirectoryHeldAcrossProcesses(t *testing.T) {
	const tier = "held-tier"
	dir := filepath.Join("..", "..", ".scratch", tier, "1")
	if os.Getenv(childEnv) == "1" {
		playChild(t, dir)
		return
	}
	held := filepath.Join(filepath.Dir(dir), ".held")
	requireHeld := func(what string) {
		t.Helper()
		if _, err := oslock.TryAcquire(held); !errors.Is(err, oslock.ErrHeld) {
			t.Fatalf("%s: the directory's lock: %v, want ErrHeld", what, err)
		}
	}
	In(t, dir)
	requireHeld("the holder")

	// The holder's child, under the holder's own hold.
	heir := spawn(t, os.Environ(), "1", mainWitness)
	heir.expect(t, "asking", 20*time.Second)
	heir.expect(t, "in", 20*time.Second)
	heir.release(t)

	// A stranger, told of no hold, and an impostor, told of the hold
	// by a process not its parent: held off until the holder lets go.
	stranger := spawn(t, withoutHeld(os.Environ()), "1", mainWitness)
	stranger.expect(t, "asking", 20*time.Second)
	parent, err := filepath.Abs(filepath.Dir(dir))
	if err != nil {
		t.Fatal(err)
	}
	impostor := spawn(t, append(withoutHeld(os.Environ()), heldEnv+"="+heldEntry(os.Getpid()+1, parent)), "1", mainWitness)
	impostor.expect(t, "asking", 20*time.Second)
	select {
	case line := <-stranger.lines:
		t.Fatalf("a stranger entered a held directory: %q", line)
	case line := <-impostor.lines:
		t.Fatalf("an impostor entered a held directory: %q", line)
	case <-time.After(500 * time.Millisecond):
	}
	releaseHoldForTest(dir)
	// Whichever of the two enters first holds; the other after it.
	waiting := []*child{stranger, impostor}
	for len(waiting) > 0 {
		entrant := firstToSay(t, "in", 20*time.Second, waiting...)
		requireHeld("the entrant")
		entrant.release(t)
		rest := waiting[:0:0]
		for _, c := range waiting {
			if c != entrant {
				rest = append(rest, c)
			}
		}
		waiting = rest
	}

	// A process told of a hold no one holds: the hold is its own.
	stale := spawn(t, os.Environ(), "1", mainWitness) // the bequest still names the directory
	stale.expect(t, "asking", 20*time.Second)
	stale.expect(t, "in", 20*time.Second)
	requireHeld("the stale heir")
	stale.release(t)
	if l, err := oslock.TryAcquire(held); err != nil {
		t.Fatalf("after the stale heir's exit: %v", err)
	} else {
		l.Close()
	}
}

// playChild is the child's part: it announces itself, asks for the
// directory, announces that it is in, and stays until stdin closes.
func playChild(t *testing.T, dir string) {
	fmt.Println("asking")
	In(t, dir)
	fmt.Println("in")
	io.Copy(io.Discard, os.Stdin)
}

// TestDirectoryHeldUnderSeparatorInPath pins the bequest's spelling:
// a scratch directory whose path holds the platform's list
// separator is one entry to the holder's child, which enters at
// once.
func TestDirectoryHeldUnderSeparatorInPath(t *testing.T) {
	dir := filepath.Join("..", "..", ".scratch", "held"+string(os.PathListSeparator)+"tier", "1")
	if os.Getenv(childEnv) == "2" {
		playChild(t, dir)
		return
	}
	In(t, dir)
	heir := spawn(t, os.Environ(), "2", "^TestDirectoryHeldUnderSeparatorInPath$")
	heir.expect(t, "asking", 20*time.Second)
	heir.expect(t, "in", 20*time.Second)
	heir.release(t)
}

// child is a re-executed test binary playing the child: its lines
// arrive on lines, drained closes at its stdout's end, stdin's close
// lets it go.
type child struct {
	cmd     *exec.Cmd
	stdin   io.WriteCloser
	lines   chan string
	drained chan struct{}
	mu      sync.Mutex
	output  strings.Builder
}

// text is the child's output so far.
func (c *child) text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.output.String()
}

// mainWitness is the main witness's test pattern; its child's part is "1".
const mainWitness = "^TestDirectoryHeldAcrossProcesses$"

// spawn starts the child playing the part named, running the test
// named.
func spawn(t *testing.T, env []string, role, run string) *child {
	t.Helper()
	c := &child{lines: make(chan string, 16), drained: make(chan struct{})}
	c.cmd = exec.Command(os.Args[0], "-test.run="+run, "-test.count=1")
	c.cmd.Env = append(env, childEnv+"="+role)
	stdin, err := c.cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	c.stdin = stdin
	out, err := c.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.cmd.Process.Kill()
		<-c.drained
		_ = c.cmd.Wait()
	})
	go func() {
		defer close(c.drained)
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			line := sc.Text()
			c.mu.Lock()
			c.output.WriteString(line + "\n")
			c.mu.Unlock()
			select {
			case c.lines <- line:
			default:
			}
		}
	}()
	return c
}

// expect waits for the child to say the line, failing on another
// line, on its exit, or on the bound.
func (c *child) expect(t *testing.T, want string, bound time.Duration) {
	t.Helper()
	select {
	case line := <-c.lines:
		if line != want {
			t.Fatalf("the child said %q, want %q", line, want)
		}
	case <-c.drained:
		t.Fatalf("the child exited before saying %q:\n%s", want, c.text())
	case <-time.After(bound):
		t.Fatalf("the child never said %q within %v:\n%s", want, bound, c.text())
	}
}

// firstToSay waits for the first of the children to say the line
// (one or two of them), failing on another line, an exit, or the
// bound.
func firstToSay(t *testing.T, want string, bound time.Duration, children ...*child) *child {
	t.Helper()
	var second *child
	if len(children) > 1 {
		second = children[1]
	}
	lines := func(c *child) chan string {
		if c == nil {
			return nil
		}
		return c.lines
	}
	drained := func(c *child) chan struct{} {
		if c == nil {
			return nil
		}
		return c.drained
	}
	first := children[0]
	select {
	case line := <-first.lines:
		if line != want {
			t.Fatalf("the child said %q, want %q", line, want)
		}
		return first
	case line := <-lines(second):
		if line != want {
			t.Fatalf("the child said %q, want %q", line, want)
		}
		return second
	case <-first.drained:
		t.Fatalf("the child exited before saying %q:\n%s", want, first.text())
	case <-drained(second):
		t.Fatalf("the child exited before saying %q:\n%s", want, second.text())
	case <-time.After(bound):
		t.Fatalf("no child said %q within %v", want, bound)
	}
	return nil
}

// release lets the child go and requires its clean exit.
func (c *child) release(t *testing.T) {
	t.Helper()
	c.stdin.Close()
	<-c.drained
	if err := c.cmd.Wait(); err != nil {
		t.Fatalf("the child's exit: %v\n%s", err, c.text())
	}
}

// withoutHeld is the environment with no hold bequeathed.
func withoutHeld(env []string) []string {
	var out []string
	for _, kv := range env {
		if !strings.HasPrefix(kv, heldEnv+"=") {
			out = append(out, kv)
		}
	}
	return out
}
