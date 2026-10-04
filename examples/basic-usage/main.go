// Command basic-usage demonstrates the core nosqlite workflow: opening a
// store, creating a table for a document type, indexing a field, inserting
// documents, and querying them back with a combined clause.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/dioad/nosqlite"
)

// User is the document type stored in this example. nosqlite serializes it
// to JSON and stores it in a generated table named after the type.
type User struct {
	ID   string   `json:"id"`
	Name string   `json:"name"`
	Age  int      `json:"age"`
	Tags []string `json:"tags"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// run holds main's logic in a function that returns its error instead of
// calling log.Fatal directly, so the deferred store.Close() below actually
// runs on every path - calling log.Fatal after registering a defer would
// skip it, since os.Exit terminates the process before deferred calls run.
func run() (err error) {
	ctx := context.Background()

	store, err := nosqlite.NewStore("basic-usage.db")
	if err != nil {
		return fmt.Errorf("failed to open store: %w", err)
	}
	defer func() {
		if cerr := store.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	users, err := nosqlite.NewTable[User](ctx, store)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if _, err := users.CreateIndex(ctx, "id"); err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	for _, u := range []User{
		{ID: "1", Name: "Alice", Age: 30, Tags: []string{"go", "sqlite"}},
		{ID: "2", Name: "Bob", Age: 22, Tags: []string{"python"}},
	} {
		if err := users.Insert(ctx, u); err != nil {
			return fmt.Errorf("failed to insert %s: %w", u.Name, err)
		}
	}

	clause := nosqlite.And(
		nosqlite.GreaterThanOrEqual("age", 25),
		nosqlite.Contains("tags", "go"),
	)

	found, err := users.QueryMany(ctx, clause)
	if err != nil {
		return fmt.Errorf("failed to query: %w", err)
	}

	for _, u := range found {
		fmt.Printf("Found: %s (%d)\n", u.Name, u.Age)
	}

	return nil
}
