package store

import "os"

// hostARMVariant reads the kernel's CPU architecture report for the
// built-in default platform on 32-bit arm hosts
// (REQ-store-platform-default). Only the kernel knows the hardware;
// any failure yields no variant, leaving the strict rule's loud
// ambiguity failure rather than a guess.
func hostARMVariant() string {
	data, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	return parseARMVariant(data)
}
