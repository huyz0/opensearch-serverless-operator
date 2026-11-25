package plugins

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"oras.land/oras-go/v2/registry/remote"
)

const PluginArtifactType = "application/vnd.opensearch-serverless.plugin.v1"

// FetchPlugin downloads and verifies the single ZIP layer of a pinned plugin
// artifact. The supplied repository client controls registry authentication
// and transport. The caller authorizes the publisher and must discard the
// destination on error before extracting or publishing any downloaded bytes.
func FetchPlugin(ctx context.Context, repository *remote.Repository, manifestDigest string, destination io.Writer, maxBlobBytes int64) error {
	if repository == nil || destination == nil || maxBlobBytes <= 0 {
		return fmt.Errorf("repository, destination and positive size limit are required")
	}
	descriptor, manifestStream, err := repository.FetchReference(ctx, manifestDigest)
	if err != nil {
		return fmt.Errorf("fetch plugin manifest: %w", err)
	}
	defer manifestStream.Close()
	if descriptor.Digest.String() != manifestDigest || descriptor.MediaType != ocispec.MediaTypeImageManifest {
		return fmt.Errorf("plugin manifest digest or media type mismatch")
	}
	var manifestBytes bytes.Buffer
	if err := VerifyBlob(&manifestBytes, manifestStream, manifestDigest, descriptor.Size, 1<<20); err != nil {
		return fmt.Errorf("verify plugin manifest: %w", err)
	}
	var manifest ocispec.Manifest
	if err := json.Unmarshal(manifestBytes.Bytes(), &manifest); err != nil {
		return fmt.Errorf("decode plugin manifest: %w", err)
	}
	if manifest.SchemaVersion != 2 || manifest.ArtifactType != PluginArtifactType || len(manifest.Layers) != 1 || manifest.Layers[0].MediaType != "application/zip" {
		return fmt.Errorf("unsupported plugin artifact format")
	}
	layer := manifest.Layers[0]
	if layer.Size < 0 || layer.Size > maxBlobBytes {
		return fmt.Errorf("plugin blob exceeds size limit")
	}
	stream, err := repository.Fetch(ctx, layer)
	if err != nil {
		return fmt.Errorf("fetch plugin blob: %w", err)
	}
	defer stream.Close()
	if err := VerifyBlob(destination, stream, layer.Digest.String(), layer.Size, maxBlobBytes); err != nil {
		return fmt.Errorf("verify plugin blob: %w", err)
	}
	return nil
}
