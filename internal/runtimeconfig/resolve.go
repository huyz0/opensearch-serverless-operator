// Package runtimeconfig resolves user-provided Java startup configuration.
package runtimeconfig

import (
	"fmt"
	"sort"
	"strings"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

// Catalog identifies settings qualified for an image and role overrides.
type Catalog struct {
	Settings         map[string]bool // value reports whether a role override is allowed
	SystemProperties map[string]bool
	JVMOptions       map[string]bool // exact tokens qualified for the selected image
	ValidateValue    func(key, value string) error
}

// Resolve merges common and role configuration. JVM options are returned as
// separate literal argument tokens; the caller must not pass them to a shell.
// A non-nil role option list replaces the common list, even when empty.
func Resolve(common, role *v1alpha1.RuntimeConfig, catalog Catalog) ([]string, error) {
	settings := map[string]string{}
	properties := map[string]string{}
	var options []string
	for index, config := range []*v1alpha1.RuntimeConfig{common, role} {
		if config == nil {
			continue
		}
		for key, value := range config.Settings {
			roleAllowed, known := catalog.Settings[key]
			if !known || (index == 1 && !roleAllowed) {
				return nil, fmt.Errorf("setting %q is not qualified for this scope", key)
			}
			if catalog.ValidateValue != nil {
				if err := catalog.ValidateValue(key, value); err != nil {
					return nil, err
				}
			}
			settings[key] = value
		}
		if config.JVM == nil {
			continue
		}
		for key, value := range config.JVM.SystemProperties {
			roleAllowed, known := catalog.SystemProperties[key]
			if !known || (index == 1 && !roleAllowed) {
				return nil, fmt.Errorf("JVM property %q is not qualified for this scope", key)
			}
			if catalog.ValidateValue != nil {
				if err := catalog.ValidateValue(key, value); err != nil {
					return nil, err
				}
			}
			properties[key] = value
		}
		if config.JVM.Options != nil {
			options = append([]string{}, (*config.JVM.Options)...)
		}
	}
	for key := range settings {
		if _, exists := properties[key]; exists {
			return nil, fmt.Errorf("setting %q is supplied twice", key)
		}
	}
	for _, option := range options {
		if !catalog.JVMOptions[option] || strings.ContainsAny(option, "\x00\r\n") {
			return nil, fmt.Errorf("JVM option is not qualified")
		}
	}
	args := append([]string{}, options...)
	appendSorted := func(values map[string]string) {
		keys := make([]string, 0, len(values))
		for key := range values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			args = append(args, "-D"+key+"="+values[key])
		}
	}
	appendSorted(settings)
	appendSorted(properties)
	return args, nil
}
