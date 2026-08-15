package upper

import "path"

// AncestorOccluded reports whether an upper marker or opaque on an
// ancestor of p already hides the base content at p: a whiteout at
// any ancestor, or an opaque on an ancestor directory the upper
// holds (docs/specs/writable.md's base-visible definition). An
// occluded base entry contributes nothing to the presented merge and
// is not comparison truth for commit's elision.
func (s *State) AncestorOccluded(p string) bool {
	for d := path.Dir(p); d != "." && d != "/"; d = path.Dir(d) {
		if s.Whiteouts[d] {
			return true
		}
		if s.Opaque[d] {
			if _, ok := s.Entries[d]; ok {
				return true
			}
		}
	}
	return false
}

// OccludesBase reports whether the upper hides the base entry at p
// entirely: the path's own whiteout, or an ancestor's occlusion.
// Shadowing by an upper entry is not occlusion — the path stays
// presented, from the upper.
func (s *State) OccludesBase(p string) bool {
	return s.Whiteouts[p] || s.AncestorOccluded(p)
}
