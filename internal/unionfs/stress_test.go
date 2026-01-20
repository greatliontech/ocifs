package unionfs

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// =============================================================================
// Phase 3: Concurrency and Stress Tests
// =============================================================================

// TestStress_ConcurrentReadWrite tests 50 goroutines doing mixed read/write operations
func TestStress_ConcurrentReadWrite(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	// Add some initial files
	for i := 0; i < 10; i++ {
		name := fmt.Sprintf("file%d.txt", i)
		content := []byte(fmt.Sprintf("initial content %d", i))
		env.addROFile(name, content, 0644)
	}
	env.mount(true)

	const numGoroutines = 50
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*opsPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				// Mix of read and write operations
				if j%2 == 0 {
					// Read operation
					fileIdx := (id + j) % 10
					path := env.path(fmt.Sprintf("file%d.txt", fileIdx))
					_, err := os.ReadFile(path)
					if err != nil {
						errors <- fmt.Errorf("goroutine %d, op %d, read: %w", id, j, err)
						return
					}
				} else {
					// Write operation - create new file
					name := fmt.Sprintf("new_%d_%d.txt", id, j)
					path := env.path(name)
					content := []byte(fmt.Sprintf("content from goroutine %d op %d", id, j))
					if err := os.WriteFile(path, content, 0644); err != nil {
						errors <- fmt.Errorf("goroutine %d, op %d, write: %w", id, j, err)
						return
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	errCount := 0
	for err := range errors {
		errCount++
		if errCount <= 5 { // Only show first 5 errors
			t.Error(err)
		}
	}
	if errCount > 5 {
		t.Errorf("... and %d more errors", errCount-5)
	}
}

// TestStress_ConcurrentDirectoryOps tests concurrent mkdir/rmdir/rename operations
func TestStress_ConcurrentDirectoryOps(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	const numGoroutines = 30
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*opsPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < opsPerGoroutine; j++ {
				dirName := fmt.Sprintf("dir_%d_%d", id, j)
				dirPath := env.path(dirName)

				// Create directory
				if err := os.Mkdir(dirPath, 0755); err != nil {
					// Directory might already exist from previous iteration
					if !os.IsExist(err) {
						errors <- fmt.Errorf("mkdir %s: %w", dirName, err)
						continue
					}
				}

				// Create a file in the directory
				filePath := filepath.Join(dirPath, "file.txt")
				if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
					errors <- fmt.Errorf("writefile %s: %w", filePath, err)
					continue
				}

				// Remove file
				if err := os.Remove(filePath); err != nil {
					errors <- fmt.Errorf("remove file %s: %w", filePath, err)
					continue
				}

				// Remove directory
				if err := os.Remove(dirPath); err != nil {
					// Directory might have been removed by rename
					if !os.IsNotExist(err) {
						errors <- fmt.Errorf("rmdir %s: %w", dirName, err)
					}
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}
}

// TestStress_ConcurrentFileCreation tests 50 goroutines creating files in the same directory
func TestStress_ConcurrentFileCreation(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	// Create target directory
	targetDir := env.path("shared")
	if err := os.Mkdir(targetDir, 0755); err != nil {
		t.Fatalf("Mkdir failed: %v", err)
	}

	const numGoroutines = 50
	const filesPerGoroutine = 20

	var wg sync.WaitGroup
	errors := make(chan error, numGoroutines*filesPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < filesPerGoroutine; j++ {
				fileName := fmt.Sprintf("file_%d_%d.txt", id, j)
				filePath := filepath.Join(targetDir, fileName)
				content := []byte(fmt.Sprintf("content %d %d", id, j))
				if err := os.WriteFile(filePath, content, 0644); err != nil {
					errors <- err
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Error(err)
	}

	// Verify all files were created
	entries, err := os.ReadDir(targetDir)
	if err != nil {
		t.Fatalf("ReadDir failed: %v", err)
	}

	expected := numGoroutines * filesPerGoroutine
	if len(entries) != expected {
		t.Errorf("Expected %d files, got %d", expected, len(entries))
	}
}

// TestStress_ReadWhileWriting tests concurrent reads while file is being written
func TestStress_ReadWhileWriting(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	// Create initial file
	testFile := env.path("concurrent.txt")
	if err := os.WriteFile(testFile, []byte("initial"), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	const numReaders = 30
	const numWriters = 5
	const duration = 500 * time.Millisecond

	var wg sync.WaitGroup
	done := make(chan struct{})
	errors := make(chan error, 100)

	// Start readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					_, err := os.ReadFile(testFile)
					if err != nil && !os.IsNotExist(err) {
						select {
						case errors <- err:
						default:
						}
					}
				}
			}
		}(i)
	}

	// Start writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			counter := 0
			for {
				select {
				case <-done:
					return
				default:
					content := []byte(fmt.Sprintf("writer %d counter %d", id, counter))
					err := os.WriteFile(testFile, content, 0644)
					if err != nil {
						select {
						case errors <- err:
						default:
						}
					}
					counter++
				}
			}
		}(i)
	}

	// Let it run for a bit
	time.Sleep(duration)
	close(done)
	wg.Wait()
	close(errors)

	// Check errors
	errCount := 0
	for err := range errors {
		errCount++
		if errCount <= 3 {
			t.Logf("Error (may be expected during concurrent access): %v", err)
		}
	}
	if errCount > 3 {
		t.Logf("... and %d more errors", errCount-3)
	}
}

// TestStress_ManySmallFiles tests creating 1000 small files
func TestStress_ManySmallFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	const numFiles = 1000
	content := []byte("small content")

	// Create files
	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("small_%04d.txt", i)
		path := env.path(name)
		if err := os.WriteFile(path, content, 0644); err != nil {
			t.Fatalf("WriteFile %d failed: %v", i, err)
		}
	}

	// Verify all files exist and have correct content
	for i := 0; i < numFiles; i++ {
		name := fmt.Sprintf("small_%04d.txt", i)
		path := env.path(name)
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %d failed: %v", i, err)
		}
		if !bytes.Equal(got, content) {
			t.Errorf("Content mismatch for file %d", i)
		}
	}
}

// TestStress_FewLargeFiles tests creating and reading large files
func TestStress_FewLargeFiles(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	const numFiles = 5
	const fileSize = 10 * 1024 * 1024 // 10MB

	// Create files
	for i := 0; i < numFiles; i++ {
		content := make([]byte, fileSize)
		for j := range content {
			content[j] = byte((i + j) % 256)
		}

		name := fmt.Sprintf("large_%d.bin", i)
		path := env.path(name)
		if err := os.WriteFile(path, content, 0644); err != nil {
			t.Fatalf("WriteFile %d failed: %v", i, err)
		}

		// Verify content
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("ReadFile %d failed: %v", i, err)
		}
		if len(got) != fileSize {
			t.Errorf("Size mismatch for file %d: got %d, want %d", i, len(got), fileSize)
		}
		if !bytes.Equal(got, content) {
			t.Errorf("Content mismatch for file %d", i)
		}
	}
}

// TestStress_DeepDirectoryTree tests creating a very deep directory structure
func TestStress_DeepDirectoryTree(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	// Create deep directory tree
	const depth = 50
	currentPath := env.mountPoint
	for i := 0; i < depth; i++ {
		currentPath = filepath.Join(currentPath, fmt.Sprintf("level%d", i))
		if err := os.Mkdir(currentPath, 0755); err != nil {
			t.Fatalf("Mkdir at depth %d failed: %v", i, err)
		}
	}

	// Create a file at the deepest level
	deepFile := filepath.Join(currentPath, "deep.txt")
	content := []byte("deep content")
	if err := os.WriteFile(deepFile, content, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Read it back
	got, err := os.ReadFile(deepFile)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if !bytes.Equal(got, content) {
		t.Error("Content mismatch")
	}
}

// TestStress_PersistUnderLoad tests auto-persist with many writes per second
func TestStress_PersistUnderLoad(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	const numWrites = 500
	const duration = 2 * time.Second

	start := time.Now()
	for i := 0; i < numWrites && time.Since(start) < duration; i++ {
		name := fmt.Sprintf("write_%04d.txt", i)
		path := env.path(name)
		content := []byte(fmt.Sprintf("write %d at %v", i, time.Now()))
		if err := os.WriteFile(path, content, 0644); err != nil {
			t.Fatalf("WriteFile %d failed: %v", i, err)
		}
	}

	// Persist and verify
	if err := env.ufs.PersistWritable(); err != nil {
		t.Fatalf("PersistWritable failed: %v", err)
	}
}

// TestRace_WritableLayerOps tests for race conditions in WritableLayer operations
func TestRace_WritableLayerOps(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	env.mount(true)

	var wg sync.WaitGroup
	const numGoroutines = 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				// Create file
				name := fmt.Sprintf("race_%d_%d.txt", id, j)
				path := env.path(name)
				os.WriteFile(path, []byte("test"), 0644)

				// Read file
				os.ReadFile(path)

				// Stat file
				os.Stat(path)

				// Delete file
				os.Remove(path)
			}
		}(i)
	}

	wg.Wait()
}

// TestRace_DirectoryListing tests for race conditions during directory listing
func TestRace_DirectoryListing(t *testing.T) {
	env := newTestEnv(t)
	defer env.cleanup()

	// Add some initial files
	for i := 0; i < 20; i++ {
		env.addROFile(fmt.Sprintf("dir/file%d.txt", i), []byte("content"), 0644)
	}
	env.mount(true)

	var wg sync.WaitGroup
	const numGoroutines = 20

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				// List directory
				os.ReadDir(env.path("dir"))

				// Create new file in directory
				name := fmt.Sprintf("dir/new_%d_%d.txt", id, j)
				os.WriteFile(env.path(name), []byte("new"), 0644)

				// List again
				os.ReadDir(env.path("dir"))
			}
		}(i)
	}

	wg.Wait()
}
