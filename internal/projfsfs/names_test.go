package projfsfs

import (
	"strings"
	"testing"
)

func TestValidName(t *testing.T) {
	for name, want := range map[string]bool{
		strings.Repeat("a", 255): true,
		strings.Repeat("a", 256): false,
		// 130 two-unit runes exceed 255 UTF-16 units in 130 runes.
		strings.Repeat("😀", 130): false,
		"a.txt":                  true,
		"README":                 true,
		"a b":                    true,
		"...a":                   true,
		"console":                true,
		"com":                    true,
		"com10":                  true,
		"lptx":                   true,
		"nulls":                  true,
		"":                       false,
		"a<b":                    false,
		"a>b":                    false,
		"a:b":                    false,
		`a"b`:                    false,
		"a/b":                    false,
		`a\b`:                    false,
		"a|b":                    false,
		"a?b":                    false,
		"a*b":                    false,
		"a\x01b":                 false,
		"trailing.":              false,
		"trailing ":              false,
		"con":                    false,
		"CON":                    false,
		"Con.txt":                false,
		"nul.tar.gz":             false,
		"com1":                   false,
		"COM9.log":               false,
		"lpt0":                   false,
	} {
		if got := ValidName(name); got != want {
			t.Errorf("ValidName(%q) = %v, want %v", name, got, want)
		}
	}
}
