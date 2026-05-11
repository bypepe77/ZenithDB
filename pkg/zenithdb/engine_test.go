package zenithdb

import (
	"context"
	"path/filepath"
	"testing"
)

func TestCreateFindUniqueAndFindManyByIndex(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	_, err := db.Create(ctx, "User", Record{
		"id":    "u1",
		"email": "ada@example.com",
		"name":  "Ada",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	user, ok, err := db.FindUnique(ctx, "User", map[string]any{"email": "ada@example.com"}, nil)
	if err != nil {
		t.Fatalf("find unique: %v", err)
	}
	if !ok {
		t.Fatal("expected user")
	}
	if user["id"] != "u1" {
		t.Fatalf("unexpected user id: %v", user["id"])
	}

	users, err := db.FindMany(ctx, "User", Query{
		Where: map[string]any{"email": "ada@example.com"},
		Index: "user_email_unique",
	})
	if err != nil {
		t.Fatalf("find many: %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("expected one user, got %d", len(users))
	}
}

func TestIncludeManyRelationUsesForeignKeyIndex(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	_, err := db.Create(ctx, "User", Record{"id": "u1", "email": "ada@example.com", "name": "Ada"})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	_, err = db.Create(ctx, "Post", Record{"id": "p1", "authorId": "u1", "title": "First"})
	if err != nil {
		t.Fatalf("create post: %v", err)
	}
	_, err = db.Create(ctx, "Post", Record{"id": "p2", "authorId": "u1", "title": "Second"})
	if err != nil {
		t.Fatalf("create post: %v", err)
	}

	user, ok, err := db.FindUnique(ctx, "User", map[string]any{"id": "u1"}, map[string]Include{
		"posts": {},
	})
	if err != nil {
		t.Fatalf("find with include: %v", err)
	}
	if !ok {
		t.Fatal("expected user")
	}

	posts, ok := user["posts"].([]Record)
	if !ok {
		t.Fatalf("expected posts relation, got %T", user["posts"])
	}
	if len(posts) != 2 {
		t.Fatalf("expected two posts, got %d", len(posts))
	}
}

func TestWALReplayRestoresRecords(t *testing.T) {
	ctx := context.Background()
	walPath := filepath.Join(t.TempDir(), "zenith.wal")

	db, err := Open(ctx, TestSchema(), Options{WALPath: walPath})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	_, err = db.Create(ctx, "User", Record{"id": "u1", "email": "ada@example.com", "name": "Ada"})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	_, err = db.Update(ctx, "User", map[string]any{"id": "u1"}, Record{"name": "Ada Lovelace"})
	if err != nil {
		t.Fatalf("update user: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := Open(ctx, TestSchema(), Options{WALPath: walPath})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	user, ok, err := reopened.FindUnique(ctx, "User", map[string]any{"id": "u1"}, nil)
	if err != nil {
		t.Fatalf("find unique: %v", err)
	}
	if !ok {
		t.Fatal("expected replayed user")
	}
	if user["name"] != "Ada Lovelace" {
		t.Fatalf("unexpected replayed name: %v", user["name"])
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "snapshot.json")
	db := openTestDB(t)

	_, err := db.Create(ctx, "User", Record{"id": "u1", "email": "ada@example.com", "name": "Ada"})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := db.Snapshot(ctx, path); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	loaded := openTestDB(t)
	if err := loaded.LoadSnapshot(ctx, path); err != nil {
		t.Fatalf("load snapshot: %v", err)
	}

	user, ok, err := loaded.FindUnique(ctx, "User", map[string]any{"id": "u1"}, nil)
	if err != nil {
		t.Fatalf("find loaded user: %v", err)
	}
	if !ok {
		t.Fatal("expected loaded user")
	}
	if user["name"] != "Ada" {
		t.Fatalf("unexpected loaded name: %v", user["name"])
	}
}

func TestDataDirRecoveryReplaysWAL(t *testing.T) {
	ctx := context.Background()
	dataDir := filepath.Join(t.TempDir(), ".zenithdb")

	db, err := Open(ctx, TestSchema(), Options{DataDir: dataDir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	_, err = db.Create(ctx, "User", Record{"id": "u1", "email": "ada@example.com", "name": "Ada"})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := Open(ctx, TestSchema(), Options{DataDir: dataDir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	user, ok, err := reopened.FindUnique(ctx, "User", map[string]any{"id": "u1"}, nil)
	if err != nil {
		t.Fatalf("find recovered user: %v", err)
	}
	if !ok {
		t.Fatal("expected recovered user")
	}
	if user["email"] != "ada@example.com" {
		t.Fatalf("unexpected recovered email: %v", user["email"])
	}
}

func TestDataDirRecoveryReplaysBinaryWAL(t *testing.T) {
	ctx := context.Background()
	dataDir := filepath.Join(t.TempDir(), ".zenithdb")

	db, err := Open(ctx, TestSchema(), Options{DataDir: dataDir, WALFormat: WALFormatBinary})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	_, err = db.Create(ctx, "User", Record{"id": "u1", "email": "ada@example.com", "name": "Ada"})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := Open(ctx, TestSchema(), Options{DataDir: dataDir, WALFormat: WALFormatBinary})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	user, ok, err := reopened.FindUnique(ctx, "User", map[string]any{"id": "u1"}, nil)
	if err != nil {
		t.Fatalf("find recovered user: %v", err)
	}
	if !ok {
		t.Fatal("expected recovered user")
	}
	if user["name"] != "Ada" {
		t.Fatalf("unexpected recovered name: %v", user["name"])
	}
}

func TestOpenURLUsesLocalDataDir(t *testing.T) {
	ctx := context.Background()
	dataDir := filepath.Join(t.TempDir(), ".zenithdb")
	connectionURL := "zenith://local?dataDir=" + dataDir + "&sync=always"

	db, err := OpenURL(ctx, TestSchema(), connectionURL)
	if err != nil {
		t.Fatalf("open url: %v", err)
	}
	_, err = db.Create(ctx, "User", Record{"id": "u1", "email": "ada@example.com", "name": "Ada"})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := OpenURL(ctx, TestSchema(), connectionURL)
	if err != nil {
		t.Fatalf("reopen url: %v", err)
	}
	defer reopened.Close()

	_, ok, err := reopened.FindUnique(ctx, "User", map[string]any{"id": "u1"}, nil)
	if err != nil {
		t.Fatalf("find recovered user: %v", err)
	}
	if !ok {
		t.Fatal("expected recovered user")
	}
}

func TestCheckpointLoadsSnapshotAndSkipsOldWAL(t *testing.T) {
	ctx := context.Background()
	dataDir := filepath.Join(t.TempDir(), ".zenithdb")

	db, err := Open(ctx, TestSchema(), Options{DataDir: dataDir})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	_, err = db.Create(ctx, "User", Record{"id": "u1", "email": "ada@example.com", "name": "Ada"})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := db.Checkpoint(ctx); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := Open(ctx, TestSchema(), Options{DataDir: dataDir})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()

	users, err := reopened.FindMany(ctx, "User", Query{Where: map[string]any{"id": "u1"}})
	if err != nil {
		t.Fatalf("find users: %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("expected one checkpointed user, got %d", len(users))
	}
}

func openTestDB(t *testing.T) *DB {
	t.Helper()

	db, err := Open(context.Background(), TestSchema(), Options{})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

// TestSchema is shared by package tests and benchmark packages.
func TestSchema() Schema {
	return Schema{
		Models: []Model{
			{
				Name: "User",
				Fields: []Field{
					{Name: "id", Kind: FieldString, Required: true},
					{Name: "email", Kind: FieldString, Required: true},
					{Name: "name", Kind: FieldString, Required: true},
				},
				PrimaryKey: []string{"id"},
				Indexes: []Index{
					{Name: "user_email_unique", Fields: []string{"email"}, Unique: true},
				},
				Relations: []Relation{
					{Name: "posts", Model: "Post", Fields: []string{"id"}, References: []string{"authorId"}, Many: true},
				},
			},
			{
				Name: "Post",
				Fields: []Field{
					{Name: "id", Kind: FieldString, Required: true},
					{Name: "authorId", Kind: FieldString, Required: true},
					{Name: "title", Kind: FieldString, Required: true},
				},
				PrimaryKey: []string{"id"},
				Indexes: []Index{
					{Name: "post_author", Fields: []string{"authorId"}},
				},
				Relations: []Relation{
					{Name: "author", Model: "User", Fields: []string{"authorId"}, References: []string{"id"}},
				},
			},
		},
	}
}
