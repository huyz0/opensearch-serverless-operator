package plugins

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/magiconair/properties"
)

// CheckDescriptor checks the descriptor's identity and required properties.
// Image-specific version ranges, extended plugins, native libraries and actual
// class loading must be qualified separately before publishing a plugin.
func CheckDescriptor(directory, expectedName string) error {
	_, err := loadDescriptor(directory, expectedName)
	return err
}

func loadDescriptor(directory, expectedName string) (*properties.Properties, error) {
	if directory == "" || expectedName == "" {
		return nil, fmt.Errorf("staged directory and plugin name are required")
	}
	file, err := os.Open(filepath.Join(directory, "plugin-descriptor.properties"))
	if err != nil {
		return nil, fmt.Errorf("open plugin descriptor: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err != nil {
		return nil, fmt.Errorf("read plugin descriptor: %w", err)
	}
	if len(data) > 64<<10 {
		return nil, fmt.Errorf("plugin descriptor exceeds size limit")
	}
	// OpenSearch reads the descriptor with java.util.Properties, which
	// preserves literal values and does not interpolate environment variables.
	loader := properties.Loader{Encoding: properties.ISO_8859_1, DisableExpansion: true}
	props, err := loader.LoadBytes(data)
	if err != nil {
		return nil, fmt.Errorf("parse plugin descriptor: %w", err)
	}
	if props.GetString("name", "") != expectedName {
		return nil, fmt.Errorf("plugin descriptor name does not match requested name")
	}
	for _, key := range []string{"description", "version", "java.version", "classname"} {
		if _, exists := props.Get(key); !exists {
			return nil, fmt.Errorf("plugin descriptor lacks %s", key)
		}
	}
	_, hasVersion := props.Get("opensearch.version")
	_, hasDependencies := props.Get("dependencies")
	if hasVersion == hasDependencies {
		return nil, fmt.Errorf("plugin descriptor must specify exactly one OpenSearch version form")
	}
	if props.GetString("has.native.controller", "false") != "false" {
		return nil, fmt.Errorf("native controller plugins require a separate package contract")
	}
	return props, nil
}

// CheckExactCompatibility limits installation to bundles declaring the exact
// qualified OpenSearch and Java versions and no extended-plugin dependencies.
// Successful checks do not prove class loading or runtime compatibility.
func CheckExactCompatibility(directory, name, opensearchVersion, javaVersion string) error {
	if opensearchVersion == "" || javaVersion == "" {
		return fmt.Errorf("qualified OpenSearch and Java versions are required")
	}
	props, err := loadDescriptor(directory, name)
	if err != nil {
		return err
	}
	if props.GetString("opensearch.version", "") != opensearchVersion || props.GetString("java.version", "") != javaVersion || props.GetString("extended.plugins", "") != "" {
		return fmt.Errorf("plugin descriptor does not match the qualified versions or has unsupported dependencies")
	}
	for _, key := range props.Keys() {
		switch key {
		case "name", "description", "version", "opensearch.version", "java.version", "classname", "custom.foldername", "extended.plugins", "has.native.controller":
		default:
			return fmt.Errorf("unsupported plugin descriptor property %q", key)
		}
	}
	return nil
}
