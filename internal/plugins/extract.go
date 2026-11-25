package plugins

import (
	"archive/zip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ExtractToStaging validates and extracts a bundle into a new directory under
// parent. The caller owns the returned directory and must validate descriptor
// compatibility before publishing it. On failure the partial directory is removed.
func ExtractToStaging(ctx context.Context, archive *zip.Reader, parent string, maxBytes uint64, maxFiles int) (_ string, err error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if parent == "" || maxBytes >= uint64(^uint64(0)>>1) {
		return "", fmt.Errorf("valid staging directory and size limit are required")
	}
	if err := ValidateArchive(archive, maxBytes, maxFiles); err != nil {
		return "", err
	}
	directory, err := os.MkdirTemp(parent, "plugin-")
	if err != nil {
		return "", fmt.Errorf("create staging directory: %w", err)
	}
	defer func() {
		if err != nil {
			if cleanupErr := os.RemoveAll(directory); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("remove partial plugin extraction: %w", cleanupErr))
			}
		}
	}()
	var written uint64
	for _, entry := range archive.File {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		target := filepath.Join(directory, filepath.FromSlash(strings.TrimSuffix(entry.Name, "/")))
		if entry.FileInfo().IsDir() {
			if err := os.MkdirAll(target, 0755); err != nil {
				return "", fmt.Errorf("create plugin directory: %w", err)
			}
			continue
		}
		if err := os.MkdirAll(filepath.Dir(target), 0755); err != nil {
			return "", fmt.Errorf("create plugin parent: %w", err)
		}
		input, err := entry.Open()
		if err != nil {
			return "", fmt.Errorf("open plugin entry: %w", err)
		}
		output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			_ = input.Close()
			return "", fmt.Errorf("create plugin entry: %w", err)
		}
		count, copyErr := io.Copy(output, io.LimitReader(cancelableReader{ctx, input}, int64(maxBytes-written)+1))
		if err := errors.Join(copyErr, output.Close(), input.Close()); err != nil {
			return "", fmt.Errorf("extract plugin entry %q: %w", entry.Name, err)
		}
		if uint64(count) > maxBytes-written {
			return "", fmt.Errorf("plugin archive exceeds size limit during extraction")
		}
		written += uint64(count)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return directory, nil
}

type cancelableReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r cancelableReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
