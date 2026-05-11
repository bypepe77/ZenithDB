package compiler

import (
	"strings"
	"testing"
)

func TestParseSchemaSupportsModelsIndexesAndRelations(t *testing.T) {
	schema, err := ParseSchema(`
model User {
  id    String @id
  email String @unique
  name  String
  posts Post[]
}

model Post {
  id       String @id
  authorId String
  title    String
  author   User @relation(fields: [authorId], references: [id])

  @@index([authorId])
}
`)
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}

	if len(schema.Models) != 2 {
		t.Fatalf("expected 2 models, got %d", len(schema.Models))
	}
	user := schema.Models[0]
	if user.Name != "User" {
		t.Fatalf("unexpected first model: %s", user.Name)
	}
	if len(user.PrimaryKey) != 1 || user.PrimaryKey[0] != "id" {
		t.Fatalf("unexpected user primary key: %v", user.PrimaryKey)
	}
	if len(user.Indexes) != 1 || !user.Indexes[0].Unique {
		t.Fatalf("expected user unique email index: %+v", user.Indexes)
	}
	if len(user.Relations) != 1 || user.Relations[0].Name != "posts" || !user.Relations[0].Many {
		t.Fatalf("unexpected user relations: %+v", user.Relations)
	}
	if user.Relations[0].Fields[0] != "id" || user.Relations[0].References[0] != "authorId" {
		t.Fatalf("expected inferred posts relation id -> authorId, got %+v", user.Relations[0])
	}

	post := schema.Models[1]
	if len(post.Indexes) != 1 || post.Indexes[0].Fields[0] != "authorId" {
		t.Fatalf("expected post author index: %+v", post.Indexes)
	}
	if len(post.Relations) != 1 || post.Relations[0].Fields[0] != "authorId" {
		t.Fatalf("unexpected post relation: %+v", post.Relations)
	}
}

func TestGenerateGoSchema(t *testing.T) {
	schema, err := ParseSchema(`
model User {
  id String @id
}
`)
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}

	code, err := GenerateGoSchema("generated", "AppSchema", schema)
	if err != nil {
		t.Fatalf("generate schema: %v", err)
	}
	if !strings.Contains(string(code), "var AppSchema = zenithdb.Schema") {
		t.Fatalf("generated code does not contain schema variable:\n%s", code)
	}
}

func TestGenerateGoClient(t *testing.T) {
	schema, err := ParseSchema(`
model User {
  id    String @id
  email String @unique
  name  String
  posts Post[]
}

model Post {
  id       String @id
  authorId String
  title    String
  author   User @relation(fields: [authorId], references: [id])

  @@index([authorId])
}
`)
	if err != nil {
		t.Fatalf("parse schema: %v", err)
	}

	code, err := GenerateGoClient("generated", schema)
	if err != nil {
		t.Fatalf("generate client: %v", err)
	}
	generated := string(code)
	for _, expected := range []string{
		"type Client struct",
		"remote bool",
		"options.WireURL",
		"remote.OpenWithOptions",
		"Schema.Hash()",
		"if c.client.remote",
		"type User struct",
		"type UserCreateInput struct",
		"func (c UserClient) FindUniqueByID",
		"func (c UserClient) FindUniqueByEmail",
		"type UserUpdateInput struct",
		"type UserUpdateArgs struct",
		"type UserDeleteArgs struct",
		"func (c UserClient) Update",
		"func (c UserClient) Delete",
		"type userStore struct",
		"func newUserStore() *userStore",
		"func (s *userStore) remove",
		"func (s *userStore) replace",
		"func (s *postStore) findManyByAuthorID",
		"func (c *Client) includeUser",
		"record.Posts = c.postStore.findManyByAuthorID(record.ID, 0)",
	} {
		if !strings.Contains(generated, expected) {
			t.Fatalf("generated client missing %q:\n%s", expected, generated)
		}
	}
}
