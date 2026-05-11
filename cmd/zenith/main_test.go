package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/compiler"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/wire"
)

func TestInitAndValidate(t *testing.T) {
	schemaPath := filepath.Join(t.TempDir(), "zenith.schema")

	if err := run([]string{"init", "-schema", schemaPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := run([]string{"validate", "-schema", schemaPath}); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestBenchCommand(t *testing.T) {
	schemaPath := filepath.Join(t.TempDir(), "zenith.schema")

	if err := run([]string{"init", "-schema", schemaPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := run([]string{"bench", "-schema", schemaPath, "-records", "100", "-queries", "100"}); err != nil {
		t.Fatalf("bench: %v", err)
	}
}

func TestGenerateCommand(t *testing.T) {
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "zenith.schema")
	outputPath := filepath.Join(dir, "zenith", "generated.go")

	if err := run([]string{"init", "-schema", schemaPath}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if err := run([]string{"generate", "-schema", schemaPath, "-out", outputPath, "-package", "zenith"}); err != nil {
		t.Fatalf("generate: %v", err)
	}

	code, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read generated code: %v", err)
	}
	if !strings.Contains(string(code), "type UserClient struct") {
		t.Fatalf("generated code does not contain typed user client:\n%s", code)
	}
}

func TestGenerateFromRemoteWireSchema(t *testing.T) {
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "pulled.schema")
	outputPath := filepath.Join(dir, "zenith", "generated.go")

	schema, err := compiler.ParseSchema(defaultSchema)
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	db, err := zenithdb.Open(context.Background(), schema, zenithdb.Options{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	listener := startWireServer(t, db, wire.Options{SchemaSource: defaultSchema})
	connectionURL := "zenith://" + listener.Addr().String()

	if err := run([]string{"schema", "pull", "-url", connectionURL, "-out", schemaPath}); err != nil {
		t.Fatalf("schema pull: %v", err)
	}
	if err := run([]string{"generate", "-url", connectionURL, "-out", outputPath, "-package", "zenith"}); err != nil {
		t.Fatalf("generate: %v", err)
	}
	code, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read generated code: %v", err)
	}
	if !strings.Contains(string(code), "type UserClient struct") {
		t.Fatalf("generated code does not contain typed user client:\n%s", code)
	}
}

func TestREPLCommandCreateFindAndList(t *testing.T) {
	schema, err := compiler.ParseSchema(defaultSchema)
	if err != nil {
		t.Fatalf("load schema: %v", err)
	}

	db, err := zenithdb.Open(context.Background(), schema, zenithdb.Options{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := runREPLCommand(context.Background(), db, "create User id=u1 email=ada@example.com name=Ada"); err != nil {
		t.Fatalf("create command: %v", err)
	}
	if err := runREPLCommand(context.Background(), db, "find User id=u1"); err != nil {
		t.Fatalf("find command: %v", err)
	}
	if err := runREPLCommand(context.Background(), db, "list User"); err != nil {
		t.Fatalf("list command: %v", err)
	}
}

func startWireServer(t *testing.T, db *zenithdb.DB, options wire.Options) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := wire.NewServer(db, options)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
	})
	return listener
}
