package benchmarks

import (
	"context"
	"strconv"
	"testing"

	"github.com/bypepe77/ZenithDB/pkg/zenithdb"
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
