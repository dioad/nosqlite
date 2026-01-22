# nosqlite

A lightweight NoSQL-like document store for Go, backed by SQLite and its JSONB support.

`nosqlite` provides a type-safe, document-oriented interface on top of SQLite. It allows you to store Go structs as JSON
documents while still benefiting from SQLite's performance, reliability, and indexing capabilities.

## Features

- **Document-oriented**: Store and retrieve Go structs directly.
- **Type-safe**: Leverages Go generics for a clean, type-safe API.
- **SQLite-backed**: Single-file database, ACID compliance, and excellent performance.
- **Rich Querying**: Fluent API for complex queries (Equal, In, Between, Contains, etc.).
- **Indexing**: Easily create indexes on JSON fields for fast lookups.
- **Transactions**: Full support for ACID transactions.
- **Pagination**: Built-in support for `Limit` and `Offset`.

## Installation

```bash
go get github.com/dioad/nosqlite
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/dioad/nosqlite"
)

type User struct {
    ID    string `json:"id"`
    Name  string `json:"name"`
    Age   int    `json:"age"`
    Tags  []string `json:"tags"`
}

func main() {
    ctx := context.Background()

    // Create a new store
    store, err := nosqlite.NewStore("users.db")
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    // Initialize a table for User documents
    users, err := nosqlite.NewTable[User](ctx, store)
    if err != nil {
        log.Fatal(err)
    }

    // Create an index for faster queries
    _, err = users.CreateIndex(ctx, "id")
    if err != nil {
        log.Fatal(err)
    }

    // Insert a document
    newUser := User{
        ID:   "1",
        Name: "Alice",
        Age:  30,
        Tags: []string{"go", "sqlite"},
    }
    err = users.Insert(ctx, newUser)
    if err != nil {
        log.Fatal(err)
    }

    // Query documents
    clause := nosqlite.And(
        nosqlite.GreaterThanOrEqual("age", 25),
        nosqlite.Contains("tags", "go"),
    )
    
    foundUsers, err := users.QueryMany(ctx, clause)
    if err != nil {
        log.Fatal(err)
    }

    for _, u := range foundUsers {
        fmt.Printf("Found: %s (%d)\n", u.Name, u.Age)
    }
}
```

## Querying API

`nosqlite` provides a rich set of clauses for querying your data:

- `Equal(field, value)`
- `NotEqual(field, value)`
- `GreaterThan(field, value)`, `GreaterThanOrEqual(field, value)`
- `LessThan(field, value)`, `LessThanOrEqual(field, value)`
- `In(field, values...)`
- `Between(field, from, to)`
- `Like(field, pattern)`
- `True(field)`, `False(field)`
- `Contains(field, value)`
- `ContainsAll(field, values...)`
- `ContainsAny(field, values...)`

Combine them using `And(...)` and `Or(...)`:

```go
clause := nosqlite.Or(
    nosqlite.Equal("status", "active"),
    nosqlite.And(
        nosqlite.Equal("status", "pending"),
        nosqlite.GreaterThan("priority", 10),
    ),
)
```

## License

Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
