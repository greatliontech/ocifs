package store

type PullPolicy int

func (s PullPolicy) String() string {
	switch s {
	case PullIfNotPresent:
		return "IfNotPresent"
	case PullAlways:
		return "Always"
	case PullNever:
		return "Never"
	case pullPolicyUnset:
		return "Unset"
	default:
		return "Unknown"
	}
}

const (
	// pullPolicyUnset is the zero value, what a Config that names no
	// policy carries; construction refuses it.
	pullPolicyUnset PullPolicy = iota
	PullIfNotPresent
	PullAlways
	PullNever
)
