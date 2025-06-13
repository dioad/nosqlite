package nosqlite

import (
	"context"
	"testing"
)

func TestCombined_TransactionAndPagination(t *testing.T) {
	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Insert some initial data in the main table
	for i := 1; i <= 5; i++ {
		foo := Foo{
			Id:   i,
			Name: "main-data",
			Bar: Bar{
				Name: "original",
			},
		}
		err := table.Insert(ctx, foo)
		if err != nil {
			t.Fatalf("Failed to insert initial data: %v", err)
		}
	}

	// Start a transaction
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Get a table with transaction
	tableTx := table.WithTransaction(tx)

	// Insert additional data in the transaction
	for i := 6; i <= 15; i++ {
		foo := Foo{
			Id:   i,
			Name: "tx-data",
			Bar: Bar{
				Name: "transaction",
			},
		}
		err := tableTx.Insert(ctx, foo)
		if err != nil {
			t.Fatalf("Failed to insert transaction data: %v", err)
		}
	}

	// Update some of the main data within the transaction
	for i := 1; i <= 3; i++ {
		foo := Foo{
			Id:   i,
			Name: "main-data",
			Bar: Bar{
				Name: "updated-in-tx",
			},
		}
		err := tableTx.Update(ctx, Equal("$.id", i), foo)
		if err != nil {
			t.Fatalf("Failed to update data in transaction: %v", err)
		}
	}

	// Test case 1: Pagination on transaction-only data
	t.Run("PaginationOnTransactionData", func(t *testing.T) {
		results, err := tableTx.QueryManyWithPagination(ctx, Equal("$.name", "tx-data"), 3, 2)
		if err != nil {
			t.Fatalf("Failed to query transaction data with pagination: %v", err)
		}
		
		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}
		
		// Verify we got items 8-10
		expectedIds := []int{8, 9, 10}
		for i, result := range results {
			if result.Id != expectedIds[i] {
				t.Errorf("Expected ID %d at position %d, got %d", expectedIds[i], i, result.Id)
			}
			if result.Bar.Name != "transaction" {
				t.Errorf("Expected Bar.Name to be 'transaction', got '%s'", result.Bar.Name)
			}
		}
	})

	// Test case 2: Pagination on updated data in transaction
	t.Run("PaginationOnUpdatedData", func(t *testing.T) {
		// Query updated items in transaction
		results, err := tableTx.QueryManyWithPagination(ctx, And(
			Equal("$.name", "main-data"),
			Equal("$.bar.name", "updated-in-tx"),
		), 2, 0)
		if err != nil {
			t.Fatalf("Failed to query updated data with pagination: %v", err)
		}
		
		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}
		
		// Verify we got items 1-2 with updated values
		expectedIds := []int{1, 2}
		for i, result := range results {
			if result.Id != expectedIds[i] {
				t.Errorf("Expected ID %d at position %d, got %d", expectedIds[i], i, result.Id)
			}
			if result.Bar.Name != "updated-in-tx" {
				t.Errorf("Expected Bar.Name to be 'updated-in-tx', got '%s'", result.Bar.Name)
			}
		}
		
		// Query same items in main table - should have original values
		mainResults, err := table.QueryManyWithPagination(ctx, And(
			Equal("$.name", "main-data"),
			In("$.id", 1, 2),
		), 0, 0)
		if err != nil {
			t.Fatalf("Failed to query main data with pagination: %v", err)
		}
		
		if len(mainResults) != 2 {
			t.Errorf("Expected 2 results from main table, got %d", len(mainResults))
		}
		
		for _, result := range mainResults {
			if result.Bar.Name != "original" {
				t.Errorf("Expected Bar.Name to be 'original' in main table, got '%s'", result.Bar.Name)
			}
		}
	})

	// Test case 3: Verify transaction data is not visible in main table
	t.Run("TransactionDataIsolation", func(t *testing.T) {
		// Query from main table should not see tx-data
		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "tx-data"), 0, 0)
		if err != nil {
			t.Fatalf("Failed to query with pagination from main table: %v", err)
		}
		
		if len(results) != 0 {
			t.Errorf("Expected 0 results from main table for tx-data, got %d", len(results))
		}
	})

	// Test case 4: All data visible in transaction
	t.Run("AllDataVisibleInTransaction", func(t *testing.T) {
		// All data should be visible in transaction
		results, err := tableTx.QueryManyWithPagination(ctx, All(), 0, 0)
		if err != nil {
			t.Fatalf("Failed to query all data in transaction: %v", err)
		}
		
		if len(results) != 15 {
			t.Errorf("Expected 15 total results in transaction, got %d", len(results))
		}
	})

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Test case 5: Verify all data is now visible in main table after commit
	t.Run("AllDataVisibleAfterCommit", func(t *testing.T) {
		// Query all data from main table after commit
		results, err := table.QueryManyWithPagination(ctx, All(), 0, 0)
		if err != nil {
			t.Fatalf("Failed to query all data after commit: %v", err)
		}
		
		if len(results) != 15 {
			t.Errorf("Expected 15 total results after commit, got %d", len(results))
		}
		
		// Verify tx-data is now visible
		txResults, err := table.QueryManyWithPagination(ctx, Equal("$.name", "tx-data"), 3, 2)
		if err != nil {
			t.Fatalf("Failed to query tx-data after commit: %v", err)
		}
		
		if len(txResults) != 3 {
			t.Errorf("Expected 3 tx-data results after commit, got %d", len(txResults))
		}
		
		// Verify updates are now visible
		updatedResults, err := table.QueryManyWithPagination(ctx, And(
			Equal("$.name", "main-data"),
			Equal("$.bar.name", "updated-in-tx"),
		), 0, 0)
		if err != nil {
			t.Fatalf("Failed to query updated data after commit: %v", err)
		}
		
		if len(updatedResults) != 3 {
			t.Errorf("Expected 3 updated results after commit, got %d", len(updatedResults))
		}
	})
}

func TestCombined_TransactionRollbackWithPagination(t *testing.T) {
	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Insert some initial data in the main table
	for i := 1; i <= 5; i++ {
		foo := Foo{
			Id:   i,
			Name: "rollback-test",
			Bar: Bar{
				Name: "original",
			},
		}
		err := table.Insert(ctx, foo)
		if err != nil {
			t.Fatalf("Failed to insert initial data: %v", err)
		}
	}

	// Start a transaction
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Get a table with transaction
	tableTx := table.WithTransaction(tx)

	// Update data in transaction
	for i := 1; i <= 5; i++ {
		foo := Foo{
			Id:   i,
			Name: "rollback-test",
			Bar: Bar{
				Name: "will-be-rolled-back",
			},
		}
		err := tableTx.Update(ctx, Equal("$.id", i), foo)
		if err != nil {
			t.Fatalf("Failed to update data in transaction: %v", err)
		}
	}

	// Insert additional data in transaction
	for i := 6; i <= 10; i++ {
		foo := Foo{
			Id:   i,
			Name: "rollback-test",
			Bar: Bar{
				Name: "will-be-rolled-back",
			},
		}
		err := tableTx.Insert(ctx, foo)
		if err != nil {
			t.Fatalf("Failed to insert data in transaction: %v", err)
		}
	}

	// Verify changes are visible in transaction with pagination
	results, err := tableTx.QueryManyWithPagination(ctx, Equal("$.bar.name", "will-be-rolled-back"), 3, 2)
	if err != nil {
		t.Fatalf("Failed to query data in transaction: %v", err)
	}
	
	if len(results) != 3 {
		t.Errorf("Expected 3 results in transaction, got %d", len(results))
	}

	// Rollback the transaction
	err = tx.Rollback()
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Verify changes are not visible in main table after rollback
	t.Run("DataNotVisibleAfterRollback", func(t *testing.T) {
		// Query for updated data - should not exist
		results, err := table.QueryManyWithPagination(ctx, Equal("$.bar.name", "will-be-rolled-back"), 0, 0)
		if err != nil {
			t.Fatalf("Failed to query data after rollback: %v", err)
		}
		
		if len(results) != 0 {
			t.Errorf("Expected 0 results after rollback, got %d", len(results))
		}
		
		// Verify original data is intact
		origResults, err := table.QueryManyWithPagination(ctx, Equal("$.bar.name", "original"), 0, 0)
		if err != nil {
			t.Fatalf("Failed to query original data after rollback: %v", err)
		}
		
		if len(origResults) != 5 {
			t.Errorf("Expected 5 original results after rollback, got %d", len(origResults))
		}
		
		// Verify total count is still 5
		count, err := table.Count(ctx)
		if err != nil {
			t.Fatalf("Failed to count data after rollback: %v", err)
		}
		
		if count != 5 {
			t.Errorf("Expected count of 5 after rollback, got %d", count)
		}
	})
}