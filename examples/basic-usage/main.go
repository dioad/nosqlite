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
	ctx := context.Background()

	store, err := nosqlite.NewStore("basic-usage.db")
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer store.Close()

	users, err := nosqlite.NewTable[User](ctx, store)
	if err != nil {
		log.Fatalf("failed to create table: %v", err)
	}

	if _, err := users.CreateIndex(ctx, "id"); err != nil {
		log.Fatalf("failed to create index: %v", err)
	}

	for _, u := range []User{
		{ID: "1", Name: "Alice", Age: 30, Tags: []string{"go", "sqlite"}},
		{ID: "2", Name: "Bob", Age: 22, Tags: []string{"python"}},
	} {
		if err := users.Insert(ctx, u); err != nil {
			log.Fatalf("failed to insert %s: %v", u.Name, err)
		}
	}

	clause := nosqlite.And(
		nosqlite.GreaterThanOrEqual("age", 25),
		nosqlite.Contains("tags", "go"),
	)

	found, err := users.QueryMany(ctx, clause)
	if err != nil {
		log.Fatalf("failed to query: %v", err)
	}

	for _, u := range found {
		fmt.Printf("Found: %s (%d)\n", u.Name, u.Age)
	}
}
