package util

import "math/rand"

// ported from k8s.io/apimachinery/pkg/util/rand/rand.go
const (
	// We omit vowels from the set of available characters to reduce the chances
	// of "bad words" being formed.
	alphanums = "bcdfghjklmnpqrstvwxz2456789"
	// No. of bits required to index into alphanums string.
	alphanumsIdxBits = 5
	// Mask used to extract last alphanumsIdxBits of an int.
	alphanumsIdxMask = 1<<alphanumsIdxBits - 1
	// No. of random letters we can extract from a single int63.
	maxAlphanumsPerInt = 63 / alphanumsIdxBits
)

// RandString generates a random alphanumeric string, without vowels, which is n
// characters long.  This will panic if n is less than zero.
// How the random string is created:
// - we generate random int63's
// - from each int63, we are extracting multiple random letters by bit-shifting and masking
// - if some index is out of range of alphanums we neglect it (unlikely to happen multiple times in a row)
func RandString(n int) string {
	b := make([]byte, n)

	randomInt63 := rand.Int63()
	remaining := maxAlphanumsPerInt
	for i := 0; i < n; {
		if remaining == 0 {
			randomInt63, remaining = rand.Int63(), maxAlphanumsPerInt
		}
		if idx := int(randomInt63 & alphanumsIdxMask); idx < len(alphanums) {
			b[i] = alphanums[idx]
			i++
		}
		randomInt63 >>= alphanumsIdxBits
		remaining--
	}
	return string(b)
}
