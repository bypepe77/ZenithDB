# Benchmarks

Benchmarks are first-class in ZenithDB because the product constraint is high
query throughput.

Run the full benchmark suite:

```bash
go test ./benchmarks -bench=. -benchmem
```

Recommended workflow when changing the hot path:

```bash
go test ./... && go test ./benchmarks -bench=. -benchmem -count=5
```

The `RawMapBaseline` benchmark is intentionally included as a reality check.
ZenithDB's generic engine should move toward that baseline over time through
schema compilation, generated query functions, lower allocation counts, and
lock-light read paths.

Current benchmark groups:

- Generic engine reads through `FindUnique` and `FindMany`.
- Generated-client shaped reads through shortcut and Prisma-like methods.
- DataDir recovery from WAL and checkpoint snapshots.
- Raw Go map lookup baseline.
