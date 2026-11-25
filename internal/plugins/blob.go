package plugins

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
)

// VerifyBlob copies a bounded OCI blob to a staging writer while checking its
// SHA-256 digest and descriptor size. The caller must discard the staged bytes
// on error and independently authenticate the manifest and publisher.
func VerifyBlob(destination io.Writer, source io.Reader, digest string, size int64, maxBytes int64) error {
	if destination == nil || source == nil || size < 0 || maxBytes <= 0 || size > maxBytes || maxBytes == int64(^uint64(0)>>1) {
		return fmt.Errorf("invalid blob source, destination or size")
	}
	if !strings.HasPrefix(digest, "sha256:") || len(digest) != len("sha256:")+64 {
		return fmt.Errorf("invalid SHA-256 digest")
	}
	expected, err := hex.DecodeString(strings.TrimPrefix(digest, "sha256:"))
	if err != nil || digest != strings.ToLower(digest) {
		return fmt.Errorf("invalid SHA-256 digest")
	}
	hash := sha256.New()
	count, err := io.Copy(io.MultiWriter(destination, hash), io.LimitReader(source, maxBytes+1))
	if err != nil {
		return fmt.Errorf("copy plugin blob: %w", err)
	}
	if count != size || !bytes.Equal(hash.Sum(nil), expected) {
		return fmt.Errorf("plugin blob size or digest mismatch")
	}
	return nil
}
