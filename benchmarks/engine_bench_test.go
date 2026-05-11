package benchmarks

import (
	"context"
	"net"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/remote"
	"github.com/bypepe77/ZenithDB/pkg/zenithdb/wire"
)

const seedSize = 100_000

func BenchmarkGenericEngineFindUniqueByPrimaryKey(b *testing.B) {
	ctx := context.Background()
	db := seedUsers(b, seedSize)
	where := map[string]any{"id": "u4242"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok, err := db.FindUnique(ctx, "User", where, nil)
		if err != nil {
			b.Fatalf("find unique: %v", err)
		}
		if !ok {
			b.Fatal("expected user")
		}
	}
}

func BenchmarkGenericEngineFindUniqueByPrimaryKeyParallel(b *testing.B) {
	ctx := context.Background()
	db := seedUsers(b, seedSize)
	where := map[string]any{"id": "u4242"}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, ok, err := db.FindUnique(ctx, "User", where, nil)
			if err != nil {
				b.Fatalf("find unique: %v", err)
			}
			if !ok {
				b.Fatal("expected user")
			}
		}
	})
}

func BenchmarkWireFindUniqueByPrimaryKey(b *testing.B) {
	ctx := context.Background()
	db := seedUsers(b, seedSize)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatalf("listen: %v", err)
	}
	b.Cleanup(func() {
		_ = listener.Close()
	})
	server := wire.NewServer(db, wire.Options{})
	go func() {
		_ = server.Serve(listener)
	}()

	client, err := remote.Open("zenith://" + listener.Addr().String())
	if err != nil {
		b.Fatalf("open remote client: %v", err)
	}
	b.Cleanup(func() {
		_ = client.Close()
	})

	where := map[string]any{"id": "u4242"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok, err := client.FindUnique(ctx, "User", where, nil)
		if err != nil {
			b.Fatalf("find unique: %v", err)
		}
		if !ok {
			b.Fatal("expected user")
		}
	}
}

func BenchmarkGenericEngineFindManyBySecondaryIndexLimit50(b *testing.B) {
	ctx := context.Background()
	db := seedPosts(b, seedSize)
	query := zenithdb.Query{
		Where: map[string]any{"authorId": "u1"},
		Index: "post_author",
		Limit: 50,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		posts, err := db.FindMany(ctx, "Post", query)
		if err != nil {
			b.Fatalf("find many: %v", err)
		}
		if len(posts) != 50 {
			b.Fatalf("expected 50 posts, got %d", len(posts))
		}
	}
}

func BenchmarkRawMapBaselineFindUnique(b *testing.B) {
	users := make(map[string]zenithdb.Record, seedSize)
	for i := 0; i < seedSize; i++ {
		id := "u" + strconv.Itoa(i)
		users[id] = zenithdb.Record{
			"id":    id,
			"email": id + "@example.com",
			"name":  "User " + strconv.Itoa(i),
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		user := users["u4242"]
		if user == nil {
			b.Fatal("expected user")
		}
	}
}

func BenchmarkGeneratedShortcutFindUniqueByID(b *testing.B) {
	users := seedGeneratedUserClient(seedSize)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok, err := users.FindUniqueByID(context.Background(), "u4242")
		if err != nil {
			b.Fatalf("find unique by id: %v", err)
		}
		if !ok {
			b.Fatal("expected user")
		}
	}
}

func BenchmarkGeneratedPrismaLikeFindUnique(b *testing.B) {
	users := seedGeneratedUserClient(seedSize)
	args := benchmarkUserFindUniqueArgs{
		Where: benchmarkUserWhereUniqueInput{ID: "u4242"},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, ok, err := users.FindUnique(context.Background(), args)
		if err != nil {
			b.Fatalf("find unique: %v", err)
		}
		if !ok {
			b.Fatal("expected user")
		}
	}
}

func BenchmarkDataDirRecoveryFromWAL(b *testing.B) {
	ctx := context.Background()
	dataDir := seedPersistentUsers(b, 10_000, false)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := zenithdb.Open(ctx, benchmarkSchema(), zenithdb.Options{DataDir: dataDir})
		if err != nil {
			b.Fatalf("open db: %v", err)
		}
		if err := db.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
	}
}

func BenchmarkDataDirRecoveryFromCheckpoint(b *testing.B) {
	ctx := context.Background()
	dataDir := seedPersistentUsers(b, 10_000, true)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db, err := zenithdb.Open(ctx, benchmarkSchema(), zenithdb.Options{DataDir: dataDir})
		if err != nil {
			b.Fatalf("open db: %v", err)
		}
		if err := db.Close(); err != nil {
			b.Fatalf("close db: %v", err)
		}
	}
}

func seedUsers(b *testing.B, count int) *zenithdb.DB {
	b.Helper()
	ctx := context.Background()
	db, err := zenithdb.Open(ctx, benchmarkSchema(), zenithdb.Options{})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}

	for i := 0; i < count; i++ {
		id := "u" + strconv.Itoa(i)
		_, err := db.Create(ctx, "User", zenithdb.Record{
			"id":    id,
			"email": id + "@example.com",
			"name":  "User " + strconv.Itoa(i),
		})
		if err != nil {
			b.Fatalf("create user: %v", err)
		}
	}
	b.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func seedPosts(b *testing.B, count int) *zenithdb.DB {
	b.Helper()
	ctx := context.Background()
	db, err := zenithdb.Open(ctx, benchmarkSchema(), zenithdb.Options{})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}

	_, err = db.Create(ctx, "User", zenithdb.Record{"id": "u1", "email": "ada@example.com", "name": "Ada"})
	if err != nil {
		b.Fatalf("create user: %v", err)
	}
	for i := 0; i < count; i++ {
		_, err := db.Create(ctx, "Post", zenithdb.Record{
			"id":       "p" + strconv.Itoa(i),
			"authorId": "u1",
			"title":    "Post " + strconv.Itoa(i),
		})
		if err != nil {
			b.Fatalf("create post: %v", err)
		}
	}
	b.Cleanup(func() {
		_ = db.Close()
	})
	return db
}

func seedPersistentUsers(b *testing.B, count int, checkpoint bool) string {
	b.Helper()
	ctx := context.Background()
	dataDir := filepath.Join(b.TempDir(), ".zenithdb")
	db, err := zenithdb.Open(ctx, benchmarkSchema(), zenithdb.Options{DataDir: dataDir})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}
	for i := 0; i < count; i++ {
		id := "u" + strconv.Itoa(i)
		_, err := db.Create(ctx, "User", zenithdb.Record{
			"id":    id,
			"email": id + "@example.com",
			"name":  "User " + strconv.Itoa(i),
		})
		if err != nil {
			b.Fatalf("create user: %v", err)
		}
	}
	if checkpoint {
		if err := db.Checkpoint(ctx); err != nil {
			b.Fatalf("checkpoint: %v", err)
		}
	}
	if err := db.Close(); err != nil {
		b.Fatalf("close db: %v", err)
	}
	return dataDir
}

type benchmarkUser struct {
	ID    string
	Email string
	Name  string
}

type benchmarkUserClient struct {
	store *benchmarkUserStore
}

type benchmarkUserStore struct {
	byID map[string]benchmarkUser
}

type benchmarkUserWhereUniqueInput struct {
	ID string
}

type benchmarkUserFindUniqueArgs struct {
	Where benchmarkUserWhereUniqueInput
}

func (input benchmarkUserWhereUniqueInput) where() map[string]any {
	if input.ID != "" {
		return map[string]any{"id": input.ID}
	}
	return nil
}

func (c benchmarkUserClient) FindUnique(ctx context.Context, args benchmarkUserFindUniqueArgs) (benchmarkUser, bool, error) {
	if args.Where.ID != "" {
		record, ok := c.store.findByID(args.Where.ID)
		return record, ok, nil
	}
	return benchmarkUser{}, false, nil
}

func (c benchmarkUserClient) FindUniqueByID(ctx context.Context, id string) (benchmarkUser, bool, error) {
	record, ok := c.store.findByID(id)
	return record, ok, nil
}

func seedGeneratedUserClient(count int) benchmarkUserClient {
	store := &benchmarkUserStore{byID: make(map[string]benchmarkUser, count)}
	for i := 0; i < count; i++ {
		id := "u" + strconv.Itoa(i)
		store.put(benchmarkUser{
			ID:    id,
			Email: id + "@example.com",
			Name:  "User " + strconv.Itoa(i),
		})
	}
	return benchmarkUserClient{store: store}
}

func (s *benchmarkUserStore) put(record benchmarkUser) {
	s.byID[record.ID] = record
}

func (s *benchmarkUserStore) findByID(id string) (benchmarkUser, bool) {
	record, ok := s.byID[id]
	return record, ok
}

func recordToBenchmarkUser(record zenithdb.Record) benchmarkUser {
	return benchmarkUser{
		ID:    record["id"].(string),
		Email: record["email"].(string),
		Name:  record["name"].(string),
	}
}

func benchmarkSchema() zenithdb.Schema {
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
				Relations: []zenithdb.Relation{
					{Name: "posts", Model: "Post", Fields: []string{"id"}, References: []string{"authorId"}, Many: true},
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
				Relations: []zenithdb.Relation{
					{Name: "author", Model: "User", Fields: []string{"authorId"}, References: []string{"id"}},
				},
			},
		},
	}
}
