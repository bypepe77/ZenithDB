package wire_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/remote"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/wire"
)

func TestRemoteClientRoundTripOverWire(t *testing.T) {
	ctx := context.Background()
	db, err := zenithdb.Open(ctx, testSchema(), zenithdb.Options{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	schemaHash := mustSchemaHash(t, testSchema())
	listener := startWireServer(t, db, wire.Options{SchemaHash: schemaHash})

	client, err := remote.OpenWithOptions(ctx, remote.OpenOptions{
		ConnectionURL: "zenith://" + listener.Addr().String(),
		SchemaHash:    schemaHash,
		PoolSize:      4,
	})
	if err != nil {
		t.Fatalf("open remote client: %v", err)
	}
	defer client.Close()

	_, err = client.Create(ctx, "User", zenithdb.Record{"id": "u1", "email": "ada@example.com", "name": "Ada"})
	if err != nil {
		t.Fatalf("remote create: %v", err)
	}

	record, ok, err := client.FindUnique(ctx, "User", map[string]any{"id": "u1"}, nil)
	if err != nil {
		t.Fatalf("remote find unique: %v", err)
	}
	if !ok {
		t.Fatal("expected remote record")
	}
	if record["email"] != "ada@example.com" {
		t.Fatalf("unexpected email: %v", record["email"])
	}

	var waitGroup sync.WaitGroup
	for i := 0; i < 32; i++ {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			record, ok, err := client.FindUnique(ctx, "User", map[string]any{"id": "u1"}, nil)
			if err != nil {
				t.Errorf("pooled find unique: %v", err)
				return
			}
			if !ok || record["email"] != "ada@example.com" {
				t.Errorf("unexpected pooled record: found=%v record=%v", ok, record)
			}
		}()
	}
	waitGroup.Wait()
}

func TestRemoteSchemaPullAndValidateOverWire(t *testing.T) {
	ctx := context.Background()
	schemaSource := `model User {
  id    String @id
  email String @unique
  name  String
}`
	db, err := zenithdb.Open(ctx, testSchema(), zenithdb.Options{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	listener := startWireServer(t, db, wire.Options{SchemaSource: schemaSource, SchemaHash: mustSchemaHash(t, testSchema())})

	client, err := remote.Open("zenith://" + listener.Addr().String())
	if err != nil {
		t.Fatalf("open remote client: %v", err)
	}
	defer client.Close()

	pulled, err := client.PullSchema(ctx)
	if err != nil {
		t.Fatalf("pull schema: %v", err)
	}
	if pulled != schemaSource {
		t.Fatalf("unexpected schema: %q", pulled)
	}
	if err := client.ValidateSchema(ctx, schemaSource); err != nil {
		t.Fatalf("validate schema: %v", err)
	}
}

func TestWireRejectsInvalidAuthToken(t *testing.T) {
	ctx := context.Background()
	db, err := zenithdb.Open(ctx, testSchema(), zenithdb.Options{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	listener := startWireServer(t, db, wire.Options{Token: "secret"})
	_, err = remote.OpenWithOptions(ctx, remote.OpenOptions{
		ConnectionURL: "zenith://" + listener.Addr().String() + "?token=wrong",
		PoolSize:      1,
	})
	if err == nil || !strings.Contains(err.Error(), "unauthorized") {
		t.Fatalf("expected unauthorized error, got %v", err)
	}
}

func TestWireRejectsSchemaHashMismatch(t *testing.T) {
	ctx := context.Background()
	schema := testSchema()
	db, err := zenithdb.Open(ctx, schema, zenithdb.Options{})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	listener := startWireServer(t, db, wire.Options{SchemaHash: mustSchemaHash(t, schema)})
	_, err = remote.OpenWithOptions(ctx, remote.OpenOptions{
		ConnectionURL: "zenith://" + listener.Addr().String(),
		SchemaHash:    "not-the-server-schema",
		PoolSize:      1,
	})
	if err == nil || !strings.Contains(err.Error(), "schema hash mismatch") {
		t.Fatalf("expected schema hash mismatch error, got %v", err)
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

func testSchema() zenithdb.Schema {
	return zenithdb.Schema{
		Models: []zenithdb.Model{
			{
				Name: "User",
				Fields: []zenithdb.Field{
					{Name: "id", Kind: zenithdb.FieldString, Required: true},
					{Name: "email", Kind: zenithdb.FieldString, Required: true},
					{Name: "name", Kind: zenithdb.FieldString, Required: true},
				},
				PrimaryKey: []string{"id"},
				Indexes: []zenithdb.Index{
					{Name: "user_email_unique", Fields: []string{"email"}, Unique: true},
				},
			},
		},
	}
}

func mustSchemaHash(t *testing.T, schema zenithdb.Schema) string {
	t.Helper()
	hash, err := schema.Hash()
	if err != nil {
		t.Fatalf("schema hash: %v", err)
	}
	return hash
}
