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
	default:
		return "Unknown"
	}
}

const (
	PullIfNotPresent PullPolicy = iota
	PullAlways
	PullNever
)
