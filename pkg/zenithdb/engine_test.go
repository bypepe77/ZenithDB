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

func TestFindManySupportsFiltersOrderAndPagination(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)

	_, err := db.Create(ctx, "User", Record{"id": "u1", "email": "ada@example.com", "name": "Ada"})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	for _, post := range []Record{
		{"id": "p1", "authorId": "u1", "title": "Alpha"},
		{"id": "p2", "authorId": "u1", "title": "Beta"},
		{"id": "p3", "authorId": "u1", "title": "Gamma"},
	} {
		if _, err := db.Create(ctx, "Post", post); err != nil {
			t.Fatalf("create post: %v", err)
		}
	}

	posts, err := db.FindMany(ctx, "Post", Query{
		Where: map[string]any{"authorId": "u1"},
		Filters: map[string]Filter{
			"title": {In: []any{"Alpha", "Gamma"}},
		},
		OrderBy: []OrderBy{{Field: "title", Direction: SortDesc}},
		Skip:    1,
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("find many: %v", err)
	}
	if len(posts) != 1 || posts[0]["title"] != "Alpha" {
		t.Fatalf("unexpected ordered page: %+v", posts)
	}
	nextPosts, err := db.FindMany(ctx, "Post", Query{
		Where:   map[string]any{"authorId": "u1"},
		OrderBy: []OrderBy{{Field: "title", Direction: SortAsc}},
		Cursor:  map[string]any{"id": "p1"},
		Limit:   1,
	})
	if err != nil {
		t.Fatalf("find many cursor: %v", err)
	}
	if len(nextPosts) != 1 || nextPosts[0]["title"] != "Beta" {
		t.Fatalf("unexpected cursor page: %+v", nextPosts)
	}
	count, err := db.Count(ctx, "Post", Query{
		Where: map[string]any{"authorId": "u1"},
		Filters: map[string]Filter{
			"title": {Contains: "a"},
		},
	})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected count 3, got %d", count)
	}

	contains, err := db.FindMany(ctx, "Post", Query{
		Filters: map[string]Filter{
			"title": {Contains: "amm"},
		},
	})
	if err != nil {
		t.Fatalf("find many contains: %v", err)
	}
	if len(contains) != 1 || contains[0]["title"] != "Gamma" {
		t.Fatalf("unexpected contains result: %+v", contains)
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

func TestUpsertCreatesUpdatesAndReplaysFromWAL(t *testing.T) {
	ctx := context.Background()
	walPath := filepath.Join(t.TempDir(), "zenith.wal")
	db, err := Open(ctx, TestSchema(), Options{WALPath: walPath})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	user, created, err := db.Upsert(ctx, "User",
		map[string]any{"email": "ada@example.com"},
		Record{"id": "u1", "email": "ada@example.com", "name": "Ada"},
		Record{"name": "Ada Lovelace"},
	)
	if err != nil {
		t.Fatalf("upsert create: %v", err)
	}
	if !created || user["name"] != "Ada" {
		t.Fatalf("unexpected created upsert: created=%v user=%+v", created, user)
	}
	user, created, err = db.Upsert(ctx, "User",
		map[string]any{"email": "ada@example.com"},
		Record{"id": "u2", "email": "ada@example.com", "name": "Duplicate"},
		Record{"name": "Ada Lovelace"},
	)
	if err != nil {
		t.Fatalf("upsert update: %v", err)
	}
	if created || user["id"] != "u1" || user["name"] != "Ada Lovelace" {
		t.Fatalf("unexpected updated upsert: created=%v user=%+v", created, user)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db: %v", err)
	}

	reopened, err := Open(ctx, TestSchema(), Options{WALPath: walPath})
	if err != nil {
		t.Fatalf("reopen db: %v", err)
	}
	defer reopened.Close()
	replayed, ok, err := reopened.FindUnique(ctx, "User", map[string]any{"id": "u1"}, nil)
	if err != nil {
		t.Fatalf("find replayed upsert: %v", err)
	}
	if !ok || replayed["name"] != "Ada Lovelace" {
		t.Fatalf("unexpected replayed upsert: ok=%v user=%+v", ok, replayed)
	}
}

func TestBatchIsAtomicAndReplaysFromWAL(t *testing.T) {
	ctx := context.Background()
	walPath := filepath.Join(t.TempDir(), "zenith.wal")
	db, err := Open(ctx, TestSchema(), Options{WALPath: walPath})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	results, err := db.Batch(ctx, []BatchOperation{
		{Type: BatchCreate, Model: "User", Record: Record{"id": "u1", "email": "ada@example.com", "name": "Ada"}},
		{Type: BatchCreate, Model: "Post", Record: Record{"id": "p1", "authorId": "u1", "title": "First"}},
	})
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 batch results, got %d", len(results))
	}

	_, err = db.Batch(ctx, []BatchOperation{
		{Type: BatchCreate, Model: "Post", Record: Record{"id": "p2", "authorId": "u1", "title": "Second"}},
		{Type: BatchCreate, Model: "User", Record: Record{"id": "u2", "email": "ada@example.com", "name": "Duplicate Email"}},
	})
	if err == nil {
		t.Fatal("expected batch conflict")
	}
	posts, err := db.FindMany(ctx, "Post", Query{Where: map[string]any{"id": "p2"}})
	if err != nil {
		t.Fatalf("find p2: %v", err)
	}
	if len(posts) != 0 {
		t.Fatalf("failed batch left partial post: %+v", posts)
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
		t.Fatalf("find replayed user: %v", err)
	}
	if !ok || user["email"] != "ada@example.com" {
		t.Fatalf("unexpected replayed user: found=%v user=%+v", ok, user)
	}
	replayedPosts, err := reopened.FindMany(ctx, "Post", Query{Where: map[string]any{"authorId": "u1"}})
	if err != nil {
		t.Fatalf("find replayed posts: %v", err)
	}
	if len(replayedPosts) != 1 {
		t.Fatalf("expected one replayed post, got %+v", replayedPosts)
	}
}

func TestManyMutationsAreAtomicAndReplayFromWAL(t *testing.T) {
	ctx := context.Background()
	walPath := filepath.Join(t.TempDir(), "zenith.wal")
	db, err := Open(ctx, TestSchema(), Options{WALPath: walPath})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	created, err := db.CreateMany(ctx, "User", []Record{
		{"id": "u1", "email": "ada@example.com", "name": "Ada"},
		{"id": "u2", "email": "grace@example.com", "name": "Grace"},
		{"id": "u3", "email": "delete@example.com", "name": "Delete Me"},
	})
	if err != nil {
		t.Fatalf("create many: %v", err)
	}
	if len(created) != 3 {
		t.Fatalf("expected 3 created records, got %d", len(created))
	}

	_, err = db.CreateMany(ctx, "User", []Record{
		{"id": "u4", "email": "new@example.com", "name": "New"},
		{"id": "u5", "email": "ada@example.com", "name": "Duplicate Email"},
	})
	if err == nil {
		t.Fatal("expected create many conflict")
	}
	partial, ok, err := db.FindUnique(ctx, "User", map[string]any{"id": "u4"}, nil)
	if err != nil {
		t.Fatalf("find partial create many: %v", err)
	}
	if ok {
		t.Fatalf("failed create many left partial record: %+v", partial)
	}

	updated, err := db.UpdateMany(ctx, "User", Query{
		Filters: map[string]Filter{"email": {Contains: "example.com"}},
	}, Record{"name": "Human"})
	if err != nil {
		t.Fatalf("update many: %v", err)
	}
	if updated.Count != 3 {
		t.Fatalf("expected 3 updated records, got %d", updated.Count)
	}

	deleted, err := db.DeleteMany(ctx, "User", Query{
		Where: map[string]any{"email": "delete@example.com"},
	})
	if err != nil {
		t.Fatalf("delete many: %v", err)
	}
	if deleted.Count != 1 {
		t.Fatalf("expected 1 deleted record, got %d", deleted.Count)
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
		t.Fatalf("find replayed user: %v", err)
	}
	if !ok || user["name"] != "Human" {
		t.Fatalf("unexpected replayed bulk update: found=%v user=%+v", ok, user)
	}
	deletedUser, ok, err := reopened.FindUnique(ctx, "User", map[string]any{"id": "u3"}, nil)
	if err != nil {
		t.Fatalf("find replayed deleted user: %v", err)
	}
	if ok {
		t.Fatalf("expected replayed bulk delete to remove u3, got %+v", deletedUser)
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
