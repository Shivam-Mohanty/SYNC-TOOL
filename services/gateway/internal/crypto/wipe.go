package crypto

import "runtime"

// WipeBytes securely overwrites a byte slice with zeros.
// It uses runtime.KeepAlive to ensure the compiler does not optimize away the wiping.
func WipeBytes(b []byte) {
	if b == nil {
		return
	}
	for i := range b {
		b[i] = 0
	}
	runtime.KeepAlive(b)
}
