# ZenithDB Documentation

This documentation is organized like a product guide rather than a single README.
The goal is to make ZenithDB easy to learn for developers who already understand
Prisma-style schemas and generated clients.

## Prisma Schema

- [Overview](prisma-schema/overview.md)
- [Data Model](prisma-schema/data-model.md)
- [Relations](prisma-schema/relations.md)

## Prisma Client

- [Queries](prisma-client/queries.md)
- [Mutations](prisma-client/mutations.md)

## Current Scope

ZenithDB currently supports a focused subset of Prisma-like behavior:

- Schema-defined models.
- Scalar fields.
- Primary keys.
- Unique and secondary indexes.
- One-to-one, many-to-one, and one-to-many relation metadata.
- Generated Go clients.
- Relation expansion with `Include`.
- Embedded and remote binary-wire execution.

Anything not documented here should be treated as unsupported until it is
implemented and tested.
