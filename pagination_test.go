package nosqlite

import (
	"context"
	"testing"
)

func TestTable_QueryManyWithPagination(t *testing.T) {
	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Insert test data - 10 items with sequential IDs
	for i := 1; i <= 10; i++ {
		foo := Foo{
			Id:   i,
			Name: "pagination-test",
		}
		err := table.Insert(ctx, foo)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Test case 1: Limit only (limit=3, offset=0)
	t.Run("LimitOnly", func(t *testing.T) {
		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 3, 0)
		if err != nil {
			t.Fatalf("Failed to query with pagination: %v", err)
		}

		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}

		// Verify we got the first 3 items
		expectedIds := []int{1, 2, 3}
		for i, result := range results {
			if result.Id != expectedIds[i] {
				t.Errorf("Expected ID %d at position %d, got %d", expectedIds[i], i, result.Id)
			}
		}
	})

	// Test case 2: Offset only (limit=0, offset=5)
	t.Run("OffsetOnly", func(t *testing.T) {
		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 0, 5)
		if err != nil {
			t.Fatalf("Failed to query with pagination: %v", err)
		}

		if len(results) != 5 {
			t.Errorf("Expected 5 results, got %d", len(results))
		}

		// Verify we got items 6-10
		expectedIds := []int{6, 7, 8, 9, 10}
		for i, result := range results {
			if result.Id != expectedIds[i] {
				t.Errorf("Expected ID %d at position %d, got %d", expectedIds[i], i, result.Id)
			}
		}
	})

	// Test case 3: Both limit and offset (limit=3, offset=5)
	t.Run("LimitAndOffset", func(t *testing.T) {
		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 3, 5)
		if err != nil {
			t.Fatalf("Failed to query with pagination: %v", err)
		}

		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}

		// Verify we got items 6-8
		expectedIds := []int{6, 7, 8}
		for i, result := range results {
			if result.Id != expectedIds[i] {
				t.Errorf("Expected ID %d at position %d, got %d", expectedIds[i], i, result.Id)
			}
		}
	})

	// Test case 4: Zero limit and zero offset (should return all items)
	t.Run("ZeroLimitAndOffset", func(t *testing.T) {
		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 0, 0)
		if err != nil {
			t.Fatalf("Failed to query with pagination: %v", err)
		}

		if len(results) != 10 {
			t.Errorf("Expected 10 results, got %d", len(results))
		}
	})

	// Test case 5: Offset beyond available data
	t.Run("OffsetBeyondData", func(t *testing.T) {
		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 0, 15)
		if err != nil {
			t.Fatalf("Failed to query with pagination: %v", err)
		}

		if len(results) != 0 {
			t.Errorf("Expected 0 results, got %d", len(results))
		}
	})

	// Test case 6: Limit larger than available data
	t.Run("LargeLimitSmallData", func(t *testing.T) {
		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 20, 0)
		if err != nil {
			t.Fatalf("Failed to query with pagination: %v", err)
		}

		if len(results) != 10 {
			t.Errorf("Expected 10 results, got %d", len(results))
		}
	})
}

func TestTableWithTx_QueryManyWithPagination(t *testing.T) {
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

	// Insert test data within transaction - 10 items with sequential IDs
	for i := 1; i <= 10; i++ {
		foo := Foo{
			Id:   i,
			Name: "tx-pagination-test",
		}
		err := tableTx.Insert(ctx, foo)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Test case 1: Basic pagination in transaction
	t.Run("BasicPaginationInTx", func(t *testing.T) {
		results, err := tableTx.QueryManyWithPagination(ctx, Equal("$.name", "tx-pagination-test"), 3, 2)
		if err != nil {
			t.Fatalf("Failed to query with pagination in transaction: %v", err)
		}

		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}

		// Verify we got items 3-5
		expectedIds := []int{3, 4, 5}
		for i, result := range results {
			if result.Id != expectedIds[i] {
				t.Errorf("Expected ID %d at position %d, got %d", expectedIds[i], i, result.Id)
			}
		}
	})

	// Test case 2: Verify data is not visible outside transaction
	t.Run("DataIsolationWithPagination", func(t *testing.T) {
		// Query from main table should return no results
		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "tx-pagination-test"), 0, 0)
		if err != nil {
			t.Fatalf("Failed to query with pagination from main table: %v", err)
		}

		if len(results) != 0 {
			t.Errorf("Expected 0 results from main table, got %d", len(results))
		}
	})

	// Test case 3: Verify QueryMany calls QueryManyWithPagination
	t.Run("QueryManyCallsPagination", func(t *testing.T) {
		// QueryMany should call QueryManyWithPagination with limit=0, offset=0
		results, err := tableTx.QueryMany(ctx, Equal("$.name", "tx-pagination-test"))
		if err != nil {
			t.Fatalf("Failed to query with QueryMany in transaction: %v", err)
		}

		if len(results) != 10 {
			t.Errorf("Expected 10 results, got %d", len(results))
		}
	})

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Test case 4: Verify data is now visible in main table after commit
	t.Run("PaginationAfterCommit", func(t *testing.T) {
		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "tx-pagination-test"), 3, 2)
		if err != nil {
			t.Fatalf("Failed to query with pagination from main table after commit: %v", err)
		}

		if len(results) != 3 {
			t.Errorf("Expected 3 results from main table after commit, got %d", len(results))
		}

		// Verify we got items 3-5
		expectedIds := []int{3, 4, 5}
		for i, result := range results {
			if result.Id != expectedIds[i] {
				t.Errorf("Expected ID %d at position %d, got %d", expectedIds[i], i, result.Id)
			}
		}
	})
}

func TestPagination_WithComplexQuery(t *testing.T) {
	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Insert test data with different categories
	categories := []string{"category1", "category2", "category3"}
	id := 1
	for _, category := range categories {
		for i := 1; i <= 5; i++ {
			foo := Foo{
				Id:   id,
				Name: category,
				Bar: Bar{
					Name: "item",
				},
			}
			err := table.Insert(ctx, foo)
			if err != nil {
				t.Fatalf("Failed to insert test data: %v", err)
			}
			id++
		}
	}

	// Test pagination with complex query (AND condition)
	t.Run("PaginationWithComplexQuery", func(t *testing.T) {
		// Query items from category2 with pagination
		clause := And(
			Equal("$.name", "category2"),
			GreaterThan("$.id", 5),
		)

		results, err := table.QueryManyWithPagination(ctx, clause, 2, 1)
		if err != nil {
			t.Fatalf("Failed to query with complex condition and pagination: %v", err)
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}

		// Should get items with IDs 7 and 8 (skipping 6 due to offset=1)
		expectedIds := []int{7, 8}
		for i, result := range results {
			if result.Id != expectedIds[i] {
				t.Errorf("Expected ID %d at position %d, got %d", expectedIds[i], i, result.Id)
			}
			if result.Name != "category2" {
				t.Errorf("Expected Name 'category2', got '%s'", result.Name)
			}
		}
	})
}
