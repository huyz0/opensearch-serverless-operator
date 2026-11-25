package runtimeconfig

import (
	"slices"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/api/v1alpha1"
)

func TestDevelopmentCatalogValueBoundaries(t *testing.T) {
	for _, value := range []string{"1s", "600s", "10m"} {
		args, err := Resolve(nil, &v1alpha1.RuntimeConfig{Settings: map[string]string{warmupMaxWait: value}}, DevelopmentCatalog("search"))
		if err != nil || !slices.Equal(args, []string{"-D" + warmupMaxWait + "=" + value}) {
			t.Fatalf("valid warm-up override %q did not render: %v", value, err)
		}
	}
	for _, value := range []string{"0s", "601s", "11m", "1m1s", "1.5m", "$(POD_IP)", "3m\n-Dserverless.roles=ingest"} {
		if _, err := Resolve(nil, &v1alpha1.RuntimeConfig{Settings: map[string]string{warmupMaxWait: value}}, DevelopmentCatalog("search")); err == nil {
			t.Fatalf("accepted invalid warm-up override %q", value)
		}
	}
	for _, value := range []string{"Local", "../UTC", "$(POD_IP)", "Not/AZone"} {
		config := &v1alpha1.RuntimeConfig{JVM: &v1alpha1.JVMConfig{SystemProperties: map[string]string{timezone: value}}}
		if _, err := Resolve(config, nil, DevelopmentCatalog("ingest")); err == nil {
			t.Fatalf("accepted invalid timezone %q", value)
		}
	}
	for _, option := range []string{"-Xmx1g", "-javaagent:/tmp/app.jar", "@options", "-Dnode.name=other"} {
		config := &v1alpha1.RuntimeConfig{JVM: &v1alpha1.JVMConfig{Options: &[]string{option}}}
		if _, err := Resolve(config, nil, DevelopmentCatalog("search")); err == nil {
			t.Fatalf("accepted unqualified JVM fragment %q", option)
		}
	}
}

func TestDevelopmentWriterReceiptSettings(t *testing.T) {
	config := &v1alpha1.RuntimeConfig{Settings: map[string]string{writerHandoffReader: "receipt-reader"}}
	args, err := Resolve(config, nil, DevelopmentCatalog("search"))
	if err != nil || !slices.Equal(args, []string{"-D" + writerHandoffReader + "=receipt-reader"}) {
		t.Fatalf("search could not configure a receipt reader: %v %v", args, err)
	}
	config.Settings[persistWriterHandoff] = "true"
	config.Settings[writerRetirementPreparer] = "retirement-operator"
	args, err = Resolve(nil, config, DevelopmentCatalog("ingest"))
	want := []string{"-D" + persistWriterHandoff + "=true", "-D" + writerRetirementPreparer + "=retirement-operator", "-D" + writerHandoffReader + "=receipt-reader"}
	if err != nil || !slices.Equal(args, want) {
		t.Fatalf("ingest receipt settings were not preserved: %v %v", args, err)
	}
	if _, err := Resolve(nil, config, DevelopmentCatalog("search")); err == nil {
		t.Fatal("search allowed writer receipt persistence")
	}
	config.Settings[writerHandoffReader] = "Unauthenticated"
	if _, err := Resolve(nil, config, DevelopmentCatalog("ingest")); err == nil {
		t.Fatal("accepted the unauthenticated subject as receipt reader")
	}
}
