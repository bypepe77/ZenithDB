# ZenithDB

ZenithDB is an experimental application database engine written in Go.

The goal is a fast in-process engine with a Prisma-like developer experience,
schema-defined models, native relations, in-memory indexes, durable append-only
persistence, and schema compilation for low-overhead query paths.

## Repository Layout

```txt
cmd/zenith/                Developer CLI
examples/hexagonal/        Hexagonal architecture integration example
pkg/zenithdb/              Core public Go engine
pkg/zenithdb/compiler/     Prisma-like schema parser and Go schema generator
benchmarks/                Throughput and allocation benchmarks
```

## Current MVP

- Schema-first models with fields, primary keys, secondary indexes, unique indexes, and relation metadata.
- In-memory query engine with `Create`, `Update`, `Delete`, `FindUnique`, and `FindMany`.
- Indexed lookups for primary keys, unique indexes, and secondary indexes.
- Prisma-like relation expansion through `Include`.
- Data directory persistence with manifest, WAL, snapshots, and automatic recovery.
- Append-only WAL for durable mutations with configurable sync policy.
- Checkpoint snapshots for faster recovery.
- Prisma-like schema parser foundation.
- Go schema generator with Prisma-like `FindUnique` and `FindMany` argument types.
- Binary TCP wire protocol for remote data operations.
- Versioned wire handshake with schema hash compatibility checks and bounded frames.
- Pooled remote client connections for concurrent callers.
- HTTP control plane for health, schema metadata, and checkpoint operations.
- Developer CLI with `init`, `validate`, `generate`, `bench`, `repl`, `serve`, and remote schema commands.
- Dedicated benchmark package with raw map baseline comparison.
- Focused tests for indexed reads, relations, WAL replay, and snapshots.

## CLI

Create a starter schema:

```bash
go run ./cmd/zenith init
```

Validate the schema:

```bash
go run ./cmd/zenith validate
```

Generate a typed Go client from the schema:

```bash
go run ./cmd/zenith generate
```

The default output is `zenith/generated.go`. It contains typed structs and
model clients such as:

```go
client, err := zenith.Open(ctx, zenithdb.Options{
	DataDir: ".zenithdb",
})
user, ok, err := client.User.FindUniqueByID(ctx, "u1")
user, ok, err = client.User.FindUnique(ctx, zenith.UserFindUniqueArgs{
	Where: zenith.UserWhereUniqueInput{ID: "u1"},
	Include: &zenith.UserInclude{Posts: true},
})
posts, err := client.Post.FindManyByAuthorID(ctx, "u1", 50)
posts, err = client.Post.FindMany(ctx, zenith.PostFindManyArgs{
	Where: zenith.PostWhereInput{AuthorID: ptr("u1")},
	Take:  50,
})
```

Run a quick in-process read benchmark:

```bash
go run ./cmd/zenith bench -records 100000 -queries 1000000
```

Open a tiny REPL:

```bash
go run ./cmd/zenith repl -data .zenithdb
```

Example REPL commands:

```txt
create User id=u1 email=ada@example.com name=Ada
find User id=u1
list User
exit
```

Run ZenithDB as a remote server:

```bash
go run ./cmd/zenith serve -schema zenith.schema -addr 0.0.0.0:8787 -wire-addr 0.0.0.0:8788 -data .zenithdb -token dev-token
```

The HTTP address is the control plane. The `zenith://` URL should point to the
binary wire address, which is used for data operations and schema metadata.

From another machine, pull the server schema and generate a typed client from it:

```bash
go run ./cmd/zenith schema pull -url "zenith://db.example.com:8788?token=dev-token" -out zenith.schema
go run ./cmd/zenith generate -url "zenith://db.example.com:8788?token=dev-token" -out zenith/generated.go
```

Validate that a local schema matches the remote server:

```bash
go run ./cmd/zenith schema push -url "zenith://db.example.com:8788?token=dev-token" -schema zenith.schema
```

## Example

```go
import zenithdb "github.com/bypepe77/ZenithDB/pkg/zenithdb"

schema := zenithdb.Schema{
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

db, err := zenithdb.Open(ctx, schema, zenithdb.Options{
	DataDir: ".zenithdb",
})
if err != nil {
	panic(err)
}
defer db.Close()

_, err = db.Create(ctx, "User", zenithdb.Record{
	"id":    "u1",
	"email": "ada@example.com",
	"name":  "Ada",
})

err = db.Checkpoint(ctx)
```

## Persistence

When `DataDir` is set, ZenithDB persists data under a product-style directory:

```txt
.zenithdb/
  manifest.json
  wal/
    000001.wal
  snapshots/
    000001.snapshot.json
  locks/
    db.lock
```

Open recovery loads the latest checkpoint snapshot and replays WAL entries after
that snapshot sequence. Writes append to the WAL before mutating memory.

## Prisma-like Schema Compiler

```go
import "github.com/bypepe77/ZenithDB/pkg/zenithdb/compiler"

schema, err := compiler.ParseSchema(`
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
```

## Benchmarks

```bash
go test ./benchmarks -bench=. -benchmem
```

The benchmark suite includes a raw Go map baseline so the engine always has a
clear target for the generated hot path.

## Design Direction

The long-term direction is:

- Keep the hot path allocation-conscious and index-first.
- Compile schema/query shapes into specialized execution plans.
- Add sharded indexes and lock-light reads for high QPS.
- Use WAL, binary snapshots, and segment compaction instead of rewriting a large JSON file.
- Keep JSON for import/export, debugging, and small portable snapshots.
