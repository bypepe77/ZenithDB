package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/compiler"
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
