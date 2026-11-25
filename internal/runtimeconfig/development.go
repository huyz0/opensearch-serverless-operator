package runtimeconfig

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"
)

const warmupMaxWait = "serverless.search.warm_on_join.max_wait"
const timezone = "user.timezone"
const writerHandoffReader = "serverless.writer_handoff.reader"
const persistWriterHandoff = "serverless.writer_handoff.persist_on_shutdown"
const writerRetirementPreparer = "serverless.writer_handoff.prepare_user"

var startupDuration = regexp.MustCompile(`^[1-9][0-9]*(s|m)$`)
var timezoneName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_+/-]{0,99}$`)
var writerReaderName = regexp.MustCompile(`^[A-Za-z0-9_.@-]{1,64}$`)

// DevelopmentCatalog lists startup inputs qualified with the pinned shell.
// Node identity, heap, network and storage remain operator-managed.
func DevelopmentCatalog(role string) Catalog {
	catalog := Catalog{
		Settings:         map[string]bool{writerHandoffReader: true},
		SystemProperties: map[string]bool{timezone: true},
		JVMOptions: map[string]bool{
			"-XX:+ExitOnOutOfMemoryError": true,
			"-XX:+UseG1GC":                true,
		},
		ValidateValue: validateDevelopmentValue,
	}
	if role == "search" {
		catalog.Settings[warmupMaxWait] = true
	}
	if role == "ingest" {
		catalog.Settings[persistWriterHandoff] = true
		catalog.Settings[writerRetirementPreparer] = true
	}
	return catalog
}

func validateDevelopmentValue(key, value string) error {
	switch key {
	case writerHandoffReader, writerRetirementPreparer:
		if writerReaderName.MatchString(value) && value != "Unauthenticated" {
			return nil
		}
		return fmt.Errorf("setting %q requires a valid authenticated account name", key)
	case persistWriterHandoff:
		if value == "true" || value == "false" {
			return nil
		}
		return fmt.Errorf("setting %q requires true or false", key)
	case warmupMaxWait:
		if startupDuration.MatchString(value) {
			n, err := strconv.Atoi(value[:len(value)-1])
			if err == nil && ((strings.HasSuffix(value, "s") && n <= 600) || (strings.HasSuffix(value, "m") && n <= 10)) {
				return nil
			}
		}
		return fmt.Errorf("setting %q requires whole seconds or minutes between 1s and 10m", key)
	case timezone:
		if value != "Local" && timezoneName.MatchString(value) {
			if _, err := time.LoadLocation(value); err == nil {
				return nil
			}
		}
		return fmt.Errorf("JVM property %q requires UTC or a valid zoneinfo identifier", key)
	}
	return fmt.Errorf("no value rule for startup setting %q", key)
}
