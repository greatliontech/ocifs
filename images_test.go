package ocifs

import (
	"fmt"
	"testing"
)

func TestPull(t *testing.T) {
	s, err := NewStore("testlp")
	if err != nil {
		t.Fatal(err)
	}

	h, err := s.Pull("ghcr.io/thegrumpylion/imagetest:latest")
	if err != nil {
		t.Fatal(err)
	}

	idx, err := s.Index(*h)
	if err != nil {
		t.Fatal(err)
	}

	for i, l := range idx {
		fmt.Println("layer:", i, l.h)
		for k, v := range l.files {
			fmt.Println(k, v.Typeflag)
		}
	}

	fmt.Println(h)
}
