//go:build !linux

package store

// hostARMVariant is empty off linux: 32-bit arm hosts needing
// variant detection (REQ-store-platform-default) are linux hosts.
func hostARMVariant() string { return "" }
