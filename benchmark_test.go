package zenithdb

import (
	"context"
	"strconv"
	"testing"
)

func BenchmarkFindUniqueByPrimaryKey(b *testing.B) {
	ctx := context.Background()
	db, err := Open(ctx, testSchema(), Options{})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}

	for i := 0; i < 100_000; i++ {
		id := "u" + strconv.Itoa(i)
		_, err := db.Create(ctx, "User", Record{
			"id":    id,
			"email": id + "@example.com",
			"name":  "User " + strconv.Itoa(i),
		})
		if err != nil {
			b.Fatalf("create user: %v", err)
		}
	}

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

func BenchmarkFindManyBySecondaryIndex(b *testing.B) {
	ctx := context.Background()
	db, err := Open(ctx, testSchema(), Options{})
	if err != nil {
		b.Fatalf("open db: %v", err)
	}

	_, err = db.Create(ctx, "User", Record{"id": "u1", "email": "ada@example.com", "name": "Ada"})
	if err != nil {
		b.Fatalf("create user: %v", err)
	}
	for i := 0; i < 100_000; i++ {
		_, err := db.Create(ctx, "Post", Record{
			"id":       "p" + strconv.Itoa(i),
			"authorId": "u1",
			"title":    "Post " + strconv.Itoa(i),
		})
		if err != nil {
			b.Fatalf("create post: %v", err)
		}
	}

	query := Query{
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
