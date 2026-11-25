package plugins_test

import (
	"archive/zip"
	"bytes"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
)

func TestValidateArchiveRejectsUnsafeEntries(t *testing.T) {
	for _, test := range []struct {
		name  string
		valid bool
	}{
		{"plugin-descriptor.properties", true},
		{"../escape", false},
		{"dir/../escape", false},
		{"C:/escape", false},
		{"dir\\escape", false},
	} {
		var buffer bytes.Buffer
		writer := zip.NewWriter(&buffer)
		entry, err := writer.Create(test.name)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = entry.Write([]byte("data"))
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		reader, err := zip.NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
		if err != nil {
			t.Fatal(err)
		}
		if err := plugins.ValidateArchive(reader, 1024, 10); (err == nil) != test.valid {
			t.Errorf("path %q valid=%v: %v", test.name, test.valid, err)
		}
	}
}
