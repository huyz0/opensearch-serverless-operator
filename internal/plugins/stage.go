package plugins

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"os"

	"oras.land/oras-go/v2/registry/remote"
)

// StagePlugin downloads, verifies and extracts a plugin into a fresh staging
// directory. qualify must inspect the extracted descriptor and dependencies.
// The caller owns the returned directory and must authorize the publisher
// before calling. Publishing the staged directory is a separate operation.
func StagePlugin(ctx context.Context, repository *remote.Repository, digest, parent string, maxBytes int64, maxFiles int, qualify func(string) error) (_ string, err error) {
	if qualify == nil || parent == "" || maxBytes <= 0 {
		return "", fmt.Errorf("qualification callback, staging parent and size limit are required")
	}
	blob, err := os.CreateTemp(parent, "plugin-blob-")
	if err != nil {
		return "", fmt.Errorf("create plugin blob staging file: %w", err)
	}
	defer os.Remove(blob.Name())
	defer blob.Close()
	if err := FetchPlugin(ctx, repository, digest, blob, maxBytes); err != nil {
		return "", err
	}
	info, err := blob.Stat()
	if err != nil {
		return "", fmt.Errorf("inspect plugin blob: %w", err)
	}
	archive, err := zip.NewReader(blob, info.Size())
	if err != nil {
		return "", fmt.Errorf("open plugin ZIP: %w", err)
	}
	directory, err := ExtractToStaging(ctx, archive, parent, uint64(maxBytes), maxFiles)
	if err != nil {
		return "", err
	}
	defer func() {
		if err != nil {
			if cleanupErr := os.RemoveAll(directory); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("remove staged plugin: %w", cleanupErr))
			}
		}
	}()
	if err := qualify(directory); err != nil {
		return "", fmt.Errorf("qualify plugin: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return directory, nil
}
