//go:build linux

package scratchtest

import "testing"

func TestUnescapeMountPath(t *testing.T) {
	for in, want := range map[string]string{
		`/plain/path`:    "/plain/path",
		`/with\040space`: "/with space",
		`/tab\011nl\012`: "/tab\tnl\n",
		`/back\134slash`: `/back\slash`,
		`/trailing\04`:   `/trailing\04`,
		`/not\999octal`:  `/not\999octal`,
	} {
		if got := unescapeMountPath(in); got != want {
			t.Errorf("unescape(%q) = %q, want %q", in, got, want)
		}
	}
}
