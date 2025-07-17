package stores

import (
	"bytes"
	"crypto/sha256"
)

// compareChunkContent efficiently compares the content of two encoded chunks for equality.
// It first compares SHA-256 hashes for a quick rejection of dissimilar chunks.
// If hashes match, it performs a byte-by-byte comparison to confirm equality,
// short-circuiting on the first difference.
func compareChunkContent(encoded1, encoded2 []byte) bool {
	// Step 1: Hash comparison
	hash1 := sha256.Sum256(encoded1)
	hash2 := sha256.Sum256(encoded2)

	if !bytes.Equal(hash1[:], hash2[:]) {
		return false // Hashes differ, chunks are not equal
	}

	// Step 2: Byte-by-byte comparison (if hashes match)
	if len(encoded1) != len(encoded2) {
		return false // Different lengths, chunks are not equal
	}

	return bytes.Equal(encoded1, encoded2)

	// We omit the segmented comparison as we will be comparing the encoded bytes for equality.
}
