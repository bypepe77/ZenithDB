# Prisma Schema Overview

ZenithDB uses a Prisma-like schema language to define the logical shape of your
database. The schema is the source of truth for models, scalar fields, indexes,
unique constraints, and relation metadata.

The schema compiler turns that file into:

- `zenithdb.Schema` metadata used by the engine.
- Typed Go structs for every model.
- Typed create, update, where, include, and query argument types.
- Model clients such as `client.User` and `client.Post`.
- A schema hash used by remote clients during the binary wire handshake.

## Example

```prisma
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
```

This schema defines:

- `User.id` as the primary key.
- `User.email` as a unique lookup.
- `Post.authorId` as the stored foreign-key scalar.
- `Post.author` as a many-to-one relation from `Post` to `User`.
- `User.posts` as the inverse one-to-many relation.
- An index on `Post.authorId`, which makes `User.posts` expansion efficient.

## CLI Flow

Create a starter schema:

```bash
go run ./cmd/zenith init
```

Validate it:

```bash
go run ./cmd/zenith validate -schema zenith.schema
```

Generate the typed client:

```bash
go run ./cmd/zenith generate -schema zenith.schema -out zenith/generated.go -package zenith
```

For a remote server, pull the schema first:

```bash
go run ./cmd/zenith schema pull -url "zenith://db.example.com:8788?token=dev-token" -out zenith.schema
go run ./cmd/zenith generate -schema zenith.schema -out zenith/generated.go -package zenith
```

## Supported Schema Concepts

ZenithDB currently supports:

- `model` blocks.
- Scalar fields: `String`, `Int`, `BigInt`, `Boolean`, `Bool`, `Float`, `Decimal`, and `DateTime`.
- `@id` primary-key fields.
- `@unique` single-field unique indexes.
- `@@index([...])` secondary indexes.
- `@@unique([...])` compound unique metadata.
- Relation fields using `@relation(fields: [...], references: [...])`.
- Inverse relation fields such as `Post[]`.

## Design Rule

Relations do not store nested objects. The stored record contains scalar fields
only. Relation fields are metadata that tells the generated client and engine how
to expand related records when `Include` is requested.
