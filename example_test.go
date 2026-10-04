package nosqlite_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/dioad/nosqlite"
)

// Example demonstrates the basic workflow: open a store, create a table for
// a document type, insert a document, and query it back with a combined
// clause. This mirrors the README's Quick Start - if this example's output
// stops matching, the README is out of date.
func Example() {
	dir, err := os.MkdirTemp("", "nosqlite-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()

	store, err := nosqlite.NewStore(filepath.Join(dir, "users.db"))
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	type User struct {
		ID   string   `json:"id"`
		Name string   `json:"name"`
		Age  int      `json:"age"`
		Tags []string `json:"tags"`
	}

	users, err := nosqlite.NewTable[User](ctx, store)
	if err != nil {
		log.Fatal(err)
	}

	if _, err := users.CreateIndex(ctx, "id"); err != nil {
		log.Fatal(err)
	}

	newUser := User{
		ID:   "1",
		Name: "Alice",
		Age:  30,
		Tags: []string{"go", "sqlite"},
	}
	if err := users.Insert(ctx, newUser); err != nil {
		log.Fatal(err)
	}

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
	// Output:
	// Found: Alice (30)
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
	// ((data->>? = ?) OR ((data->>? = ?) AND (data->>? > ?)))
}
