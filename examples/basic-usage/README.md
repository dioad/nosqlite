# basic-usage

Demonstrates the core `nosqlite` workflow: opening a store, creating a table
for a document type, indexing a field, inserting documents, and querying
them back with a combined clause.

## Prerequisites

None beyond the module's own dependencies (fetched automatically via `go run`
or `go build`).

## Running

```bash
go run github.com/dioad/nosqlite/examples/basic-usage
```

Or build and run the binary directly:

```bash
cd examples/basic-usage
go build
./basic-usage
```

This creates a `basic-usage.db` SQLite file in the current directory. Delete
it to start from a clean slate on the next run (re-running without deleting
it just inserts duplicate rows, since `id` is indexed but not declared
unique).

## Expected output

```
Found: Alice (30)
```

Bob is excluded: he's indexed, inserted, and present in the table, but
`age >= 25` and `tags contains "go"` only match Alice.
