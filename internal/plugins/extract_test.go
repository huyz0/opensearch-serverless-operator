package plugins_test

import (
	"archive/zip"
	"bytes"
	"compress/flate"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/huyz0/opensearch-serverless-operator/internal/plugins"
)

func TestExtractToStaging(t *testing.T) {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	entry, err := writer.Create("plugin-descriptor.properties")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = entry.Write([]byte("name=test"))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	archive, err := zip.NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	directory, err := plugins.ExtractToStaging(context.Background(), archive, parent, 1024, 10)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(directory, "plugin-descriptor.properties"))
	if err != nil || string(contents) != "name=test" {
		t.Fatalf("unexpected contents %q: %v", contents, err)
	}
	if _, err := plugins.ExtractToStaging(context.Background(), archive, parent, 1, 10); err == nil {
		t.Fatal("archive exceeding the size bound must fail")
	}
}

func TestExtractCleansUpAfterCorruptEntry(t *testing.T) {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	entry, err := writer.CreateHeader(&zip.FileHeader{Name: "plugin.jar", Method: zip.Store})
	if err != nil {
		t.Fatal(err)
	}
	_, _ = entry.Write([]byte("original-content"))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	data := buffer.Bytes()
	nameLength := int(binary.LittleEndian.Uint16(data[26:28]))
	extraLength := int(binary.LittleEndian.Uint16(data[28:30]))
	data[30+nameLength+extraLength] ^= 0xff
	archive, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	if _, err := plugins.ExtractToStaging(context.Background(), archive, parent, 1024, 10); !errors.Is(err, zip.ErrChecksum) {
		t.Fatalf("expected a checksum error, got %v", err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 0 {
		t.Fatalf("failed extraction left staging content: %v, %v", entries, err)
	}
}

func TestExtractCleansUpAfterCancellation(t *testing.T) {
	var buffer bytes.Buffer
	writer := zip.NewWriter(&buffer)
	entry, err := writer.Create("plugin.jar")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Write(bytes.Repeat([]byte("plugin data"), 1<<14)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	archive, err := zip.NewReader(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	archive.RegisterDecompressor(zip.Deflate, func(input io.Reader) io.ReadCloser {
		return cancelAfterRead{ReadCloser: flate.NewReader(input), cancel: cancel}
	})
	parent := t.TempDir()
	if directory, err := plugins.ExtractToStaging(ctx, archive, parent, 1<<20, 10); directory != "" || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled extraction returned directory %q and error %v", directory, err)
	}
	entries, err := os.ReadDir(parent)
	if err != nil || len(entries) != 0 {
		t.Fatalf("canceled extraction left staging content: %v, %v", entries, err)
	}
}

type cancelAfterRead struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (r cancelAfterRead) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.cancel()
	return n, err
}
