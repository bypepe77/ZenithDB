# ZenithDB

**ZenithDB is a schema-compiled application database for Go: Prisma-like schema,
generated typed clients, in-memory indexed execution, WAL persistence, and
embedded or remote access over a binary protocol.**

Status: **experimental, not production-ready**. ZenithDB is currently an engine
and architecture project. It is useful for exploring a modern database design,
benchmarking index-first execution paths, and building toward a serious
application-native database, but it should not be treated as a Postgres or Redis
replacement today.

Category: **schema-compiled embedded operational database**. More casually, it
is an application-native database engine.

The project is built around one central bet:

> If an application schema is known ahead of time, the database can compile that
> knowledge into its client, indexes, relation expansion, persistence contract,
> and remote protocol.

That makes ZenithDB closer to a schema-compiled application database than to a
traditional relational server. It borrows the developer experience of Prisma,
the low-latency shape of an embedded/in-memory engine, and the deployment model
of a remote database server.

## What This Is

ZenithDB is:

- A database engine with schema-defined models.
- An embedded-first in-memory store with indexes.
- A generated-client system inspired by Prisma.
- A durable append-only engine with WAL replay and checkpoints.
- A remote database server when opened through `zenith://`.
- A binary data-plane protocol with HTTP kept out of the hot path.

## What This Is Not

ZenithDB is not:

- A SQL database.
- A drop-in Postgres replacement.
- A Redis-compatible cache.
- An analytics engine.
- A mature distributed database.
- A production-ready transactional system.

The core tradeoff is explicit: ZenithDB gives up broad generality to make known
model operations cheaper and more predictable.

## What ZenithDB Is Optimizing For

ZenithDB is designed for workloads where data access is predictable:

- Model operations instead of arbitrary SQL strings.
- Primary-key, unique-index, and secondary-index reads as the common path.
- Generated Go clients that know the schema at compile time.
- Relation expansion through explicit metadata and indexes.
- In-memory tables and indexes for low-overhead reads.
- Append-only durability with WAL replay and checkpoints.
- Embedded mode when the application and database should share a process.
- Remote mode through a custom binary TCP protocol, not HTTP/JSON.

The goal is not to replace every database. The goal is to make a specific class
of application workloads simpler and faster by removing generic layers that are
not needed when the schema and query shapes are known.

## Why Not Just Postgres?

Postgres is the right default for many systems. It is mature, operationally
proven, SQL-native, transactional, extensible, and excellent for complex
relational workloads.

ZenithDB explores a different tradeoff.

Postgres must accept arbitrary SQL, plan queries dynamically, execute across a
disk-oriented storage engine, handle many isolation scenarios, and support broad
workload shapes. That generality is powerful, but it adds layers between the
application model and the actual data access path.

ZenithDB removes some of that generality:

- No SQL planner in the hot path.
- No runtime ORM mapping from rows into model objects.
- No requirement to cross a network boundary in embedded mode.
- No text-based data protocol for remote operations.
- No ad hoc relation discovery at query time.

In exchange, ZenithDB is narrower. It is strongest when queries are model-level,
index-first, and known ahead of time. Postgres remains the better choice for
complex joins, analytics, mature ACID semantics, replication, operational
tooling, and heterogeneous workloads.

## Why Not Just Redis?

Redis is extremely fast because it is an in-memory data structure server. It is
excellent for caching, counters, queues, pub/sub, ephemeral state, and shared
low-latency infrastructure.

ZenithDB is trying to sit higher in the stack:

- It has schema-defined models.
- It builds primary, unique, and secondary indexes from that schema.
- It generates typed model clients.
- It understands relation metadata.
- It persists database mutations through a WAL and checkpoints.
- It can run embedded or as a remote database server.

Redis gives you powerful primitives. ZenithDB aims to give you an application
database model with a Prisma-like API and a more explicit persistence story.

## Why Not Just Prisma?

Prisma is a client and schema layer on top of existing databases. ZenithDB uses a
Prisma-like schema as the database contract itself.

The schema drives:

- Engine validation.
- Primary, unique, and secondary index construction.
- Generated Go model structs.
- Typed create, update, where, include, and query arguments.
- Relation expansion metadata.
- Remote client compatibility through a schema hash handshake.

In other words, the schema is not only ORM metadata. It is part of the runtime
execution plan.

## Architecture

ZenithDB is split into a few focused layers:

```txt
Prisma-like schema
        |
        v
Schema compiler  --->  Generated Go client
        |                     |
        v                     v
 zenithdb.Schema       Model operations
        |                     |
        v                     v
 In-memory engine <--- Binary wire protocol
        |
        v
 WAL + checkpoints
```

- **Schema compiler**: parses the Prisma-like schema and generates typed Go
  clients.
- **In-memory engine**: stores model records and maintains primary, unique, and
  secondary indexes.
- **Query executor**: handles `FindUnique`, `FindMany`, filters, ordering,
  pagination, counts, relation includes, upserts, bulk mutations, and batches.
- **Durability layer**: appends mutations to the WAL and writes checkpoint
  snapshots for recovery.
- **Binary wire protocol**: serves remote data operations over TCP with protocol
  versioning, auth token support, bounded frames, schema hash checks, and pooled
  clients.
- **HTTP control plane**: exposes non-hot-path operations such as health checks,
  schema metadata, and checkpoints.

The data plane and the control plane are intentionally separate. Data operations
use the binary protocol; HTTP is not used for the performance-critical path.

## Performance Thesis

ZenithDB should be judged on narrow, reproducible workloads, not vague claims.
The performance thesis is:

- If a query shape is known, avoid dynamic SQL planning.
- If a field is hot, make it an explicit index.
- If the app and database can share a process, remove the network hop.
- If remote access is needed, use a compact binary protocol.
- If the schema is known, generate typed client code instead of runtime mapping.

The benchmark suite includes raw Go map baselines as a reality check. ZenithDB
does not claim to beat Postgres or Redis in general. It aims to be competitive on
known, index-first operational paths where a general SQL engine or generic cache
adds unnecessary layers.

Run benchmarks locally:

```bash
go test ./benchmarks -bench=. -benchmem -count=1
```

Current benchmark coverage includes primary-key reads, secondary-index reads,
generated-client shortcuts, binary-wire reads, WAL recovery, checkpoint recovery,
and raw map baselines.

## Execution Model

Records live in memory inside model-specific tables. Each table owns:

- A primary-key map.
- Unique index maps.
- Secondary index maps.
- Validation against the schema.

Common generated-client reads can become direct indexed lookups. More advanced
queries still execute against the engine, but they operate on known model
metadata instead of parsing SQL.

Mutations are designed around atomic publication. Batch operations, `Upsert`,
`CreateMany`, `UpdateMany`, and `DeleteMany` are applied to a cloned next state
first. If validation or uniqueness checks fail, the live state is not published.

## Relation Model

ZenithDB stores scalar foreign keys and treats relation fields as metadata.

```prisma
model User {
  id    String @id
  email String @unique
  posts Post[]
}

model Post {
  id       String @id
  authorId String
  author   User @relation(fields: [authorId], references: [id])

  @@index([authorId])
}
```

The stored `Post` record contains `authorId`. It does not contain an embedded
`User`. The `author` and `posts` fields tell ZenithDB how to expand related
records when a query asks for `Include`.

This design keeps relation reads explicit. A many-to-one include uses a primary
or unique lookup on the target model. A one-to-many include uses a secondary
index on the target foreign-key field.

Current relation support is focused on single-field relation pairs. Nested
writes, cascading actions, strict referential integrity, many-to-many helpers,
and relation filters are still roadmap items.

## Persistence Model

ZenithDB is not designed around rewriting one large JSON file on every mutation.
The durable path is append-first:

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

Writes are appended to the WAL before they are published in memory. Checkpoints
write snapshots so recovery can load a recent state and replay only newer WAL
entries.

JSON remains useful for readable snapshots, portability, import/export, and
debugging. The performance direction is binary WAL by default, checksums,
segment rotation, binary snapshots, and compaction.

## Current Capabilities

- Prisma-like schema parser.
- Typed Go client generation.
- Embedded and remote connection URLs.
- Primary-key, unique-index, and secondary-index lookups.
- Filters with equality, `in`, string `contains`, and range operators.
- Ordering, skip/take, cursor pagination, and count.
- Relation expansion with `Include`.
- `Create`, `CreateMany`, `Update`, `UpdateMany`, `Delete`, `DeleteMany`.
- Atomic `Batch` mutations.
- Atomic `Upsert`.
- WAL replay, snapshots, checkpoints, and data-directory recovery.
- Binary TCP data protocol with pooled remote clients.
- HTTP control plane for operational endpoints.
- Benchmarks with raw Go map baselines.

## Current Limits

ZenithDB is still experimental. Important production-grade areas remain open:

- Full transaction isolation.
- Referential integrity enforcement.
- Cascading relation actions.
- Replication and clustering.
- Online migrations.
- Backup and restore tooling.
- Observability and operational metrics.
- Complex query planning across multiple indexes or relation filters.
- WAL checksums, segment rotation, and corruption recovery hardening.

The project should be judged as an engine and architecture experiment, not as a
drop-in replacement for mature production databases.

## Technical Roadmap

The next serious engineering milestones are:

- Binary WAL as the default durable format.
- Checksums per WAL entry.
- WAL segment rotation and corruption recovery tests.
- Binary snapshot format.
- Referential integrity checks for relations.
- Cascading relation actions.
- Online schema migration strategy.
- Observability metrics for query latency, WAL latency, recovery time, and
  connection pool behavior.
- CI with tests and benchmarks on every pull request.

## When ZenithDB Is Interesting

ZenithDB is worth exploring when:

- The application owns a known schema.
- Hot queries are predictable and index-first.
- The team wants Prisma-like ergonomics with generated Go types.
- Embedded mode can remove a network hop.
- Remote mode is needed, but HTTP/JSON is not acceptable for data calls.
- Schema compatibility between client and server should be explicit.

Postgres is still the safer default for broad SQL, complex relational behavior,
analytics, mature transactions, and operational depth. Redis is still the better
fit for cache-first and ephemeral data-structure workloads.

## Documentation

Detailed guides live in [`docs/`](docs/README.md):

- [Schema overview](docs/prisma-schema/overview.md)
- [Data model](docs/prisma-schema/data-model.md)
- [Relations](docs/prisma-schema/relations.md)
- [Queries](docs/prisma-client/queries.md)
- [Mutations](docs/prisma-client/mutations.md)

## Repository Layout

```txt
cmd/zenith/                Developer CLI
docs/                      Product documentation
examples/hexagonal/        Hexagonal architecture integration example
pkg/zenithdb/              Core public Go engine
pkg/zenithdb/compiler/     Schema parser and Go client generator
pkg/zenithdb/wire/         Binary remote protocol
pkg/zenithdb/remote/       Remote client pool
benchmarks/                Throughput and allocation benchmarks
zenith/                    Generated example client
```

## Development Commands

```bash
go run ./cmd/zenith validate
go run ./cmd/zenith generate
go test ./...
go test ./benchmarks -bench=. -benchmem
```
