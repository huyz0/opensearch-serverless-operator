// Package plugins validates plugin bundles before installation.
package plugins

import (
	"archive/zip"
	"fmt"
	"path"
	"strings"
)

// ValidateArchive checks an OpenSearch plugin ZIP before extraction. The
// caller must also verify the OCI manifest, blob digest, publisher and plugin
// descriptor compatibility, and extract into a fresh isolated staging folder.
func ValidateArchive(archive *zip.Reader, maxBytes uint64, maxFiles int) error {
	if archive == nil || maxBytes == 0 || maxFiles <= 0 {
		return fmt.Errorf("archive and positive limits are required")
	}
	if len(archive.File) > maxFiles {
		return fmt.Errorf("plugin archive contains too many entries")
	}
	seen := map[string]bool{}
	var total uint64
	for _, entry := range archive.File {
		name := entry.Name
		clean := path.Clean(name)
		if name == "" || strings.Contains(name, "\\") || strings.Contains(name, ":") || strings.HasPrefix(name, "/") || clean == "." || clean == ".." || strings.HasPrefix(clean, "../") || clean != strings.TrimSuffix(name, "/") {
			return fmt.Errorf("invalid plugin archive path %q", name)
		}
		if seen[clean] {
			return fmt.Errorf("duplicate plugin archive path %q", clean)
		}
		seen[clean] = true
		if entry.Mode()&^0777 != 0 && !entry.FileInfo().IsDir() {
			return fmt.Errorf("unsupported plugin archive entry %q", name)
		}
		if entry.UncompressedSize64 > maxBytes-total {
			return fmt.Errorf("plugin archive exceeds size limit")
		}
		total += entry.UncompressedSize64
	}
	return nil
}
