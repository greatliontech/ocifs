package ocifs

import (
	"os"
	"testing"
)

func TestMountBusybox(t *testing.T) {
	workDir, err := os.MkdirTemp(os.TempDir(), "ocifstest")
	if err != nil {
		t.Fatal(err)
	}

	ofs, err := New(WithWorkDir(workDir))
	if err != nil {
		t.Fatal(err)
	}

	im, err := ofs.Mount("docker.io/busybox:latest")
	if err != nil {
		t.Fatal(err)
	}

	if err := im.Unmount(); err != nil {
		t.Fatal(err)
	}

	os.RemoveAll(workDir)
}
