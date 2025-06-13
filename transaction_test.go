package nosqlite

import (
	"context"
	"testing"
)

func TestTransaction_Commit(t *testing.T) {
	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Start a transaction
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Get a table with transaction
	tableTx := table.WithTransaction(tx)

	// Insert data within the transaction
	foo := Foo{
		Name: "transaction-commit",
		Bar: Bar{
			Name: "commit",
		},
	}

	err = tableTx.Insert(ctx, foo)
	if err != nil {
		t.Fatalf("Failed to insert data in transaction: %v", err)
	}

	// Verify data exists in transaction but not in main table yet
	txResult, err := tableTx.QueryOne(ctx, Equal("$.name", "transaction-commit"))
	if err != nil {
		t.Fatalf("Failed to query data in transaction: %v", err)
	}
	if txResult == nil {
		t.Fatal("Expected to find data in transaction, but got nil")
	}

	mainResult, err := table.QueryOne(ctx, Equal("$.name", "transaction-commit"))
	if err != nil {
		t.Fatalf("Failed to query data in main table: %v", err)
	}
	if mainResult != nil {
		t.Fatal("Expected not to find data in main table yet, but got data")
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify data now exists in main table
	mainResult, err = table.QueryOne(ctx, Equal("$.name", "transaction-commit"))
	if err != nil {
		t.Fatalf("Failed to query data in main table after commit: %v", err)
	}
	if mainResult == nil {
		t.Fatal("Expected to find data in main table after commit, but got nil")
	}
	if mainResult.Bar.Name != "commit" {
		t.Errorf("Expected Bar.Name to be 'commit', got '%s'", mainResult.Bar.Name)
	}
}

func TestTransaction_Rollback(t *testing.T) {
	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Start a transaction
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Get a table with transaction
	tableTx := table.WithTransaction(tx)

	// Insert data within the transaction
	foo := Foo{
		Name: "transaction-rollback",
		Bar: Bar{
			Name: "rollback",
		},
	}

	err = tableTx.Insert(ctx, foo)
	if err != nil {
		t.Fatalf("Failed to insert data in transaction: %v", err)
	}

	// Verify data exists in transaction
	txResult, err := tableTx.QueryOne(ctx, Equal("$.name", "transaction-rollback"))
	if err != nil {
		t.Fatalf("Failed to query data in transaction: %v", err)
	}
	if txResult == nil {
		t.Fatal("Expected to find data in transaction, but got nil")
	}

	// Rollback the transaction
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Verify data does not exist in main table
	mainResult, err := table.QueryOne(ctx, Equal("$.name", "transaction-rollback"))
	if err != nil {
		t.Fatalf("Failed to query data in main table after rollback: %v", err)
	}
	if mainResult != nil {
		t.Fatal("Expected not to find data in main table after rollback, but got data")
	}
}

func TestTableWithTx_CRUD(t *testing.T) {
	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Start a transaction
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Get a table with transaction
	tableTx := table.WithTransaction(tx)

	// Test Insert
	foo := Foo{
		Name: "tx-crud",
		Bar: Bar{
			Name: "original",
		},
	}

	err = tableTx.Insert(ctx, foo)
	if err != nil {
		t.Fatalf("Failed to insert data in transaction: %v", err)
	}

	// Test QueryOne
	result, err := tableTx.QueryOne(ctx, Equal("$.name", "tx-crud"))
	if err != nil {
		t.Fatalf("Failed to query data in transaction: %v", err)
	}
	if result == nil {
		t.Fatal("Expected to find data in transaction, but got nil")
	}
	if result.Bar.Name != "original" {
		t.Errorf("Expected Bar.Name to be 'original', got '%s'", result.Bar.Name)
	}

	// Test Update
	foo.Bar.Name = "updated"
	err = tableTx.Update(ctx, Equal("$.name", "tx-crud"), foo)
	if err != nil {
		t.Fatalf("Failed to update data in transaction: %v", err)
	}

	// Verify update
	result, err = tableTx.QueryOne(ctx, Equal("$.name", "tx-crud"))
	if err != nil {
		t.Fatalf("Failed to query data after update: %v", err)
	}
	if result == nil {
		t.Fatal("Expected to find data after update, but got nil")
	}
	if result.Bar.Name != "updated" {
		t.Errorf("Expected Bar.Name to be 'updated', got '%s'", result.Bar.Name)
	}

	// Test Count
	count, err := tableTx.Count(ctx)
	if err != nil {
		t.Fatalf("Failed to count data in transaction: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected count to be 1, got %d", count)
	}

	// Test Delete
	err = tableTx.Delete(ctx, Equal("$.name", "tx-crud"))
	if err != nil {
		t.Fatalf("Failed to delete data in transaction: %v", err)
	}

	// Verify delete
	result, err = tableTx.QueryOne(ctx, Equal("$.name", "tx-crud"))
	if err != nil {
		t.Fatalf("Failed to query data after delete: %v", err)
	}
	if result != nil {
		t.Fatal("Expected not to find data after delete, but got data")
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
}

func TestTransaction_Isolation(t *testing.T) {
	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Insert initial data
	initialFoo := Foo{
		Name: "isolation-test",
		Bar: Bar{
			Name: "initial",
		},
	}
	err := table.Insert(ctx, initialFoo)
	if err != nil {
		t.Fatalf("Failed to insert initial data: %v", err)
	}

	// Start a transaction
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Get a table with transaction
	tableTx := table.WithTransaction(tx)

	// Update data in transaction
	updatedFoo := Foo{
		Name: "isolation-test",
		Bar: Bar{
			Name: "updated-in-tx",
		},
	}
	err = tableTx.Update(ctx, Equal("$.name", "isolation-test"), updatedFoo)
	if err != nil {
		t.Fatalf("Failed to update data in transaction: %v", err)
	}

	// Verify data is updated in transaction
	txResult, err := tableTx.QueryOne(ctx, Equal("$.name", "isolation-test"))
	if err != nil {
		t.Fatalf("Failed to query data in transaction: %v", err)
	}
	if txResult == nil {
		t.Fatal("Expected to find data in transaction, but got nil")
	}
	if txResult.Bar.Name != "updated-in-tx" {
		t.Errorf("Expected Bar.Name to be 'updated-in-tx', got '%s'", txResult.Bar.Name)
	}

	// Verify data is not updated in main table
	mainResult, err := table.QueryOne(ctx, Equal("$.name", "isolation-test"))
	if err != nil {
		t.Fatalf("Failed to query data in main table: %v", err)
	}
	if mainResult == nil {
		t.Fatal("Expected to find data in main table, but got nil")
	}
	if mainResult.Bar.Name != "initial" {
		t.Errorf("Expected Bar.Name to be 'initial', got '%s'", mainResult.Bar.Name)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify data is now updated in main table
	mainResult, err = table.QueryOne(ctx, Equal("$.name", "isolation-test"))
	if err != nil {
		t.Fatalf("Failed to query data in main table after commit: %v", err)
	}
	if mainResult == nil {
		t.Fatal("Expected to find data in main table after commit, but got nil")
	}
	if mainResult.Bar.Name != "updated-in-tx" {
		t.Errorf("Expected Bar.Name to be 'updated-in-tx', got '%s'", mainResult.Bar.Name)
	}
}