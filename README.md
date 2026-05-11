# ZenithDB

ZenithDB is an experimental application database engine written in Go.

The goal is a fast in-process engine with a Prisma-like developer experience,
schema-defined models, native relations, in-memory indexes, durable append-only
persistence, and schema compilation for low-overhead query paths.

## Repository Layout

```txt
cmd/zenith/                Developer CLI
pkg/zenithdb/              Core public Go engine
pkg/zenithdb/compiler/     Prisma-like schema parser and Go schema generator
benchmarks/                Throughput and allocation benchmarks
```

## Current MVP

- Schema-first models with fields, primary keys, secondary indexes, unique indexes, and relation metadata.
- In-memory query engine with `Create`, `Update`, `Delete`, `FindUnique`, and `FindMany`.
- Indexed lookups for primary keys, unique indexes, and secondary indexes.
- Prisma-like relation expansion through `Include`.
- Append-only WAL for durable mutations.
- Snapshot save/load primitives.
- Prisma-like schema parser foundation.
- Go schema generator foundation.
- Developer CLI with `init`, `validate`, `bench`, and `repl`.
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

Run a quick in-process read benchmark:

```bash
go run ./cmd/zenith bench -records 100000 -queries 1000000
```

Open a tiny REPL:

```bash
go run ./cmd/zenith repl
```

Example REPL commands:

```txt
create User id=u1 email=ada@example.com name=Ada
find User id=u1
list User
exit
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
	WALPath: "data/zenith.wal",
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
```

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
