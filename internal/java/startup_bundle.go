package java

import (
	"crypto/sha256"
	"encoding/hex"
)

// StartupBundleSHA256 binds the exact credential files to the randomized
// OpenSearch keystore container. Never publish a checksum of the password alone.
func StartupBundleSHA256(keystore, password []byte) string {
	framed := sha256.Sum256(keystore)
	digest := sha256.New()
	digest.Write(framed[:])
	digest.Write(password)
	return hex.EncodeToString(digest.Sum(nil))
}
