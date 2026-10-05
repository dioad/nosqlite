package nosqlite_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/dioad/nosqlite"
)

// exampleUser is the document type used by Example.
type exampleUser struct {
	ID   string   `json:"id"`
	Name string   `json:"name"`
	Age  int      `json:"age"`
	Tags []string `json:"tags"`
}

// Example demonstrates the basic workflow: open a store, create a table for
// a document type, insert a document, and query it back with a combined
// clause. This mirrors the README's Quick Start - if this example's output
// stops matching, the README is out of date.
func Example() {
	if err := runExample(); err != nil {
		log.Fatal(err)
	}
	// Output:
	// Found: Alice (30)
}

// runExample holds Example's logic in a function that returns its error
// instead of calling log.Fatal directly, so every defer below actually runs
// on every path - calling log.Fatal after registering a defer would skip it,
// since os.Exit terminates the process before deferred calls run.
func runExample() (err error) {
	dir, mkdirErr := os.MkdirTemp("", "nosqlite-example")
	if mkdirErr != nil {
		return mkdirErr
	}
	defer func() {
		if rerr := os.RemoveAll(dir); rerr != nil && err == nil {
			err = rerr
		}
	}()

	ctx := context.Background()

	store, err := nosqlite.NewStore(filepath.Join(dir, "users.db"))
	if err != nil {
		return err
	}
	defer func() {
		if cerr := store.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	users, err := nosqlite.NewTable[exampleUser](ctx, store)
	if err != nil {
		return err
	}

	if _, err := users.CreateIndex(ctx, "id"); err != nil {
		return err
	}

	newUser := exampleUser{
		ID:   "1",
		Name: "Alice",
		Age:  30,
		Tags: []string{"go", "sqlite"},
	}
	if err := users.Insert(ctx, newUser); err != nil {
		return err
	}

	clause := nosqlite.And(
		nosqlite.GreaterThanOrEqual("age", 25),
		nosqlite.Contains("tags", "go"),
	)

	foundUsers, err := users.QueryMany(ctx, clause)
	if err != nil {
		return err
	}

	for _, u := range foundUsers {
		fmt.Printf("Found: %s (%d)\n", u.Name, u.Age)
	}

	return nil
}

// ExampleAnd demonstrates combining multiple clauses with And and Or, as
// described in the README's Querying API section.
func ExampleAnd() {
	clause := nosqlite.Or(
		nosqlite.Equal("status", "active"),
		nosqlite.And(
			nosqlite.Equal("status", "pending"),
			nosqlite.GreaterThan("priority", 10),
		),
	)

	fmt.Println(clause.Clause())
	// Output:
	// ((data->>'status' = ?) OR ((data->>'status' = ?) AND (data->>'priority' > ?)))
}
