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
	upserted, created, err := client.Upsert(ctx, "User",
		map[string]any{"email": "grace@example.com"},
		zenithdb.Record{"id": "u2", "email": "grace@example.com", "name": "Grace"},
		zenithdb.Record{"name": "Grace Hopper"},
	)
	if err != nil {
		t.Fatalf("remote upsert create: %v", err)
	}
	if !created || upserted["name"] != "Grace" {
		t.Fatalf("unexpected remote upsert create: created=%v user=%+v", created, upserted)
	}
	upserted, created, err = client.Upsert(ctx, "User",
		map[string]any{"email": "grace@example.com"},
		zenithdb.Record{"id": "u3", "email": "grace@example.com", "name": "Duplicate"},
		zenithdb.Record{"name": "Grace Hopper"},
	)
	if err != nil {
		t.Fatalf("remote upsert update: %v", err)
	}
	if created || upserted["id"] != "u2" || upserted["name"] != "Grace Hopper" {
		t.Fatalf("unexpected remote upsert update: created=%v user=%+v", created, upserted)
	}
	for _, post := range []zenithdb.Record{
		{"id": "p1", "authorId": "u1", "title": "Alpha"},
		{"id": "p2", "authorId": "u1", "title": "Beta"},
		{"id": "p3", "authorId": "u1", "title": "Gamma"},
	} {
		if _, err := client.Create(ctx, "Post", post); err != nil {
			t.Fatalf("remote create post: %v", err)
		}
	}
	posts, err := client.FindMany(ctx, "Post", zenithdb.Query{
		Where: map[string]any{"authorId": "u1"},
		Filters: map[string]zenithdb.Filter{
			"title": {Contains: "a"},
		},
		OrderBy: []zenithdb.OrderBy{{Field: "title", Direction: zenithdb.SortAsc}},
		Skip:    1,
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("remote find many: %v", err)
	}
	if len(posts) != 1 || posts[0]["title"] != "Beta" {
		t.Fatalf("unexpected remote ordered page: %+v", posts)
	}
	nextPosts, err := client.FindMany(ctx, "Post", zenithdb.Query{
		Where:   map[string]any{"authorId": "u1"},
		OrderBy: []zenithdb.OrderBy{{Field: "title", Direction: zenithdb.SortAsc}},
		Cursor:  map[string]any{"id": "p1"},
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("remote cursor find many: %v", err)
	}
	if len(nextPosts) != 1 || nextPosts[0]["title"] != "Beta" {
		t.Fatalf("unexpected remote cursor page: %+v", nextPosts)
	}
	count, err := client.Count(ctx, "Post", zenithdb.Query{Where: map[string]any{"authorId": "u1"}})
	if err != nil {
		t.Fatalf("remote count: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected remote count 3, got %d", count)
	}
	bulkCreated, err := client.CreateMany(ctx, "Post", []zenithdb.Record{
		{"id": "p6", "authorId": "u1", "title": "Bulk Alpha"},
		{"id": "p7", "authorId": "u1", "title": "Bulk Beta"},
	})
	if err != nil {
		t.Fatalf("remote create many: %v", err)
	}
	if len(bulkCreated) != 2 {
		t.Fatalf("expected 2 remote create many results, got %d", len(bulkCreated))
	}
	_, err = client.CreateMany(ctx, "Post", []zenithdb.Record{
		{"id": "p8", "authorId": "u1", "title": "Partial"},
		{"id": "p1", "authorId": "u1", "title": "Duplicate"},
	})
	if err == nil {
		t.Fatal("expected remote create many conflict")
	}
	partialBulk, err := client.FindMany(ctx, "Post", zenithdb.Query{Where: map[string]any{"id": "p8"}})
	if err != nil {
		t.Fatalf("remote find partial create many: %v", err)
	}
	if len(partialBulk) != 0 {
		t.Fatalf("remote create many left partial post: %+v", partialBulk)
	}
	bulkUpdated, err := client.UpdateMany(ctx, "Post", zenithdb.Query{
		Filters: map[string]zenithdb.Filter{"title": {Contains: "Bulk"}},
	}, zenithdb.Record{"title": "Bulk Updated"})
	if err != nil {
		t.Fatalf("remote update many: %v", err)
	}
	if bulkUpdated.Count != 2 {
		t.Fatalf("expected 2 remote update many records, got %d", bulkUpdated.Count)
	}
	bulkDeleted, err := client.DeleteMany(ctx, "Post", zenithdb.Query{
		Where: map[string]any{"title": "Bulk Updated"},
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("remote delete many: %v", err)
	}
	if bulkDeleted.Count != 1 {
		t.Fatalf("expected 1 remote delete many record, got %d", bulkDeleted.Count)
	}
	batchResults, err := client.Batch(ctx, []zenithdb.BatchOperation{
		{Type: zenithdb.BatchCreate, Model: "Post", Record: zenithdb.Record{"id": "p4", "authorId": "u1", "title": "Delta"}},
		{Type: zenithdb.BatchUpdate, Model: "User", Where: map[string]any{"id": "u1"}, Record: zenithdb.Record{"name": "Ada Lovelace"}},
	})
	if err != nil {
		t.Fatalf("remote batch: %v", err)
	}
	if len(batchResults) != 2 {
		t.Fatalf("expected 2 remote batch results, got %d", len(batchResults))
	}
	_, err = client.Batch(ctx, []zenithdb.BatchOperation{
		{Type: zenithdb.BatchCreate, Model: "Post", Record: zenithdb.Record{"id": "p5", "authorId": "u1", "title": "Epsilon"}},
		{Type: zenithdb.BatchCreate, Model: "User", Record: zenithdb.Record{"id": "u2", "email": "ada@example.com", "name": "Duplicate Email"}},
	})
	if err == nil {
		t.Fatal("expected remote batch conflict")
	}
	partial, err := client.FindMany(ctx, "Post", zenithdb.Query{Where: map[string]any{"id": "p5"}})
	if err != nil {
		t.Fatalf("remote find partial post: %v", err)
	}
	if len(partial) != 0 {
		t.Fatalf("remote failed batch left partial post: %+v", partial)
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
			{
				Name: "Post",
				Fields: []zenithdb.Field{
					{Name: "id", Kind: zenithdb.FieldString, Required: true},
					{Name: "authorId", Kind: zenithdb.FieldString, Required: true},
					{Name: "title", Kind: zenithdb.FieldString, Required: true},
				},
				PrimaryKey: []string{"id"},
				Indexes: []zenithdb.Index{
					{Name: "post_author", Fields: []string{"authorId"}},
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
