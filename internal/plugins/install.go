package plugins

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"oras.land/oras-go/v2/registry"
	"oras.land/oras-go/v2/registry/remote"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// Installation identifies a requested bundle. Authorization is provided
// independently through the approved artifact set.
type Installation struct {
	Name     string `json:"name"`
	Artifact string `json:"artifact"`
}

var pluginStageName = regexp.MustCompile(`^\.plugin-set-[0-9]+$`)

// The parent belongs to a single installer. A previous process can be killed
// before its deferred cleanup runs, so remove only its reserved temp dirs.
func removeAbandonedPluginStages(parent string) error {
	entries, err := os.ReadDir(parent)
	if err != nil {
		return fmt.Errorf("read plugin staging parent: %w", err)
	}
	for _, entry := range entries {
		if !pluginStageName.MatchString(entry.Name()) {
			continue
		}
		if !entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unexpected entry in reserved plugin staging namespace")
		}
		if err := os.RemoveAll(filepath.Join(parent, entry.Name())); err != nil {
			return fmt.Errorf("remove abandoned plugin staging directory: %w", err)
		}
	}
	return nil
}

// InstallSet stages every approved bundle, then publishes the complete set as
// a new directory. The parent must be a private Pod-local directory controlled
// by a single installer; concurrent or untrusted writers are not supported.
// An existing output is accepted only if its completion marker matches the
// requested set and policy versions. On error, no new output is published.
func InstallSet(ctx context.Context, output string, requested []Installation, approved map[string]bool, opensearchVersion, javaVersion string, maxBytes int64, maxFiles int, repository func(registry.Reference) (*remote.Repository, error)) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !filepath.IsAbs(output) || filepath.Clean(output) != output || filepath.Dir(output) == output || strings.HasPrefix(filepath.Base(output), ".plugin-set-") || len(requested) == 0 || repository == nil || maxBytes <= 0 || maxFiles <= 0 || opensearchVersion == "" || javaVersion == "" {
		return fmt.Errorf("absolute output, plugins, compatibility, limits and repository client are required")
	}
	type item struct {
		name      string
		reference registry.Reference
	}
	items := make([]item, 0, len(requested))
	seen := map[string]bool{}
	for _, plugin := range requested {
		if !v1alpha1.ValidPluginName(plugin.Name) || seen[plugin.Name] || !approved[plugin.Artifact] {
			return fmt.Errorf("plugin name is invalid, reserved, duplicated or artifact is not approved")
		}
		seen[plugin.Name] = true
		ref, err := registry.ParseReference(plugin.Artifact)
		if err != nil || !strings.Contains(plugin.Artifact, "@sha256:") || !strings.HasPrefix(ref.Reference, "sha256:") {
			return fmt.Errorf("plugin artifact must be an approved SHA-256 digest reference")
		}
		items = append(items, item{name: plugin.Name, reference: ref})
	}
	identity, err := json.Marshal(struct {
		Contract          string         `json:"contract"`
		Requested         []Installation `json:"requested"`
		OpenSearchVersion string         `json:"openSearchVersion"`
		JavaVersion       string         `json:"javaVersion"`
		MaxBytes          int64          `json:"maxBytes"`
		MaxFiles          int            `json:"maxFiles"`
	}{"v1", requested, opensearchVersion, javaVersion, maxBytes, maxFiles})
	if err != nil {
		return fmt.Errorf("encode installation identity: %w", err)
	}
	marker := fmt.Sprintf("%x\n", sha256.Sum256(identity))
	parent := filepath.Dir(output)
	if _, err := os.Lstat(output); err == nil {
		observed, err := os.ReadFile(filepath.Join(output, ".install-complete"))
		if err != nil || string(observed) != marker {
			return fmt.Errorf("existing plugin set does not match installation identity")
		}
		return removeAbandonedPluginStages(parent)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("cannot inspect output: %w", err)
	}
	if err := removeAbandonedPluginStages(parent); err != nil {
		return err
	}
	stage, err := os.MkdirTemp(parent, ".plugin-set-")
	if err != nil {
		return fmt.Errorf("create installation staging directory: %w", err)
	}
	defer func() {
		if err != nil {
			if cleanupErr := os.RemoveAll(stage); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("remove partial plugin set: %w", cleanupErr))
			}
		}
	}()
	for _, item := range items {
		repo, err := repository(item.reference)
		if err != nil {
			return fmt.Errorf("configure plugin repository: %w", err)
		}
		dir, err := StagePlugin(ctx, repo, item.reference.Reference, stage, maxBytes, maxFiles, func(directory string) error {
			return CheckExactCompatibility(directory, item.name, opensearchVersion, javaVersion)
		})
		if err != nil {
			return fmt.Errorf("install plugin %s: %w", item.name, err)
		}
		if err := os.Rename(dir, filepath.Join(stage, item.name)); err != nil {
			return fmt.Errorf("stage plugin %s: %w", item.name, err)
		}
	}
	if err := os.WriteFile(filepath.Join(stage, ".install-complete"), []byte(marker), 0444); err != nil {
		return fmt.Errorf("write installation completion marker: %w", err)
	}
	if _, err := os.Lstat(output); err == nil || !os.IsNotExist(err) {
		return fmt.Errorf("output appeared during installation")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Rename(stage, output); err != nil {
		return fmt.Errorf("publish plugin set: %w", err)
	}
	return nil
}
