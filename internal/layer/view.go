package layer

// View is the unified filesystem view: the flat, path-sorted result
// of unification. It is immutable once built. Hardlink entries
// arrive already resolved — their Digest and Size carry the content
// captured at the link's extraction position — so consumers never
// re-resolve link targets.
type View struct {
	entries []Entry
	index   map[string]int
}

// Entries returns the view's entries sorted by cleaned path. The
// returned slice is shared; callers must not modify it.
func (v *View) Entries() []Entry { return v.entries }

// Len returns the number of entries in the view.
func (v *View) Len() int { return len(v.entries) }

// Lookup returns the entry at a cleaned, root-relative path.
func (v *View) Lookup(name string) (Entry, bool) {
	i, ok := v.index[name]
	if !ok {
		return Entry{}, false
	}
	return v.entries[i], true
}
