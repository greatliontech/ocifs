//go:build !darwin

// The FSKit app extension is macOS-only; this stub keeps
// `go build ./...` green elsewhere (the real entry point is in
// main_darwin.go).
package main

func main() {}
