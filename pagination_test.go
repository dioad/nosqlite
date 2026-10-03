package nosqlite

import (
	"context"
	"testing"
)

// TestTable_QueryManyWithPagination_StableUnderIndex exercises pagination in
// a realistic scenario where a result-narrowing index exists and rowid
// order disagrees with the indexed field's value order (an index on $.id,
// with rows inserted in descending $.id order). The actual ordering
// guarantee - ORDER BY rowid is always present in the generated SQL - is
// pinned directly by TestPaginationQuery against paginationQuery's output;
// this test only confirms the documented stable order is what callers
// observe in a case designed to tempt the planner into a different one.
func TestTable_QueryManyWithPagination_StableUnderIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := helperOpenStore(t)
	t.Cleanup(func() { helperCloseStore(t, store) })

	table := helperTable[Foo](ctx, t, store)

	// Insert rowids 1..5 with descending $.id values, so rowid order and
	// $.id order disagree.
	insertOrderIDs := []int{5, 4, 3, 2, 1}
	for _, id := range insertOrderIDs {
		err := table.Insert(ctx, Foo{ID: id, Name: "stable-order"})
		if err != nil {
			t.Fatalf("failed to insert test data: %v", err)
		}
	}

	_, err := table.CreateIndex(ctx, "$.id")
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}

	results, err := table.QueryManyWithPagination(ctx, GreaterThan("$.id", 0), 0, 0)
	if err != nil {
		t.Fatalf("failed to query with pagination: %v", err)
	}

	if len(results) != len(insertOrderIDs) {
		t.Fatalf("expected %d results, got %d", len(insertOrderIDs), len(results))
	}

	for i, result := range results {
		if result.ID != insertOrderIDs[i] {
			t.Errorf("expected rowid (insertion) order %v at position %d, got ID %d", insertOrderIDs, i, result.ID)
		}
	}
}

// TestPaginationQuery pins the SQL text paginationQuery generates, which is
// the actual mechanism behind the ORDER BY / LIMIT guarantees documented on
// QueryManyWithPagination (see Table and TableWithTx's implementations,
// which both call it).
func TestPaginationQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		limit, offset uint64
		want          string
	}{
		{"NoLimitNoOffset", 0, 0, "SELECT data FROM `t` WHERE 1 ORDER BY rowid LIMIT -1"},
		{"LimitOnly", 3, 0, "SELECT data FROM `t` WHERE 1 ORDER BY rowid LIMIT 3"},
		// LIMIT must be present whenever OFFSET is: SQLite rejects a bare
		// OFFSET with no preceding LIMIT ("near \"OFFSET\": syntax error").
		{"OffsetOnly", 0, 5, "SELECT data FROM `t` WHERE 1 ORDER BY rowid LIMIT -1 OFFSET 5"},
		{"LimitAndOffset", 3, 5, "SELECT data FROM `t` WHERE 1 ORDER BY rowid LIMIT 3 OFFSET 5"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			if got := paginationQuery("t", "1", test.limit, test.offset); got != test.want {
				t.Errorf("got = %q, want %q", got, test.want)
			}
		})
	}
}

// TestTableWithTx_QueryManyWithPagination_OffsetOnly is a regression test:
// TableWithTx.QueryManyWithPagination used to omit LIMIT entirely when
// limit was 0, so an offset-only call (limit=0, offset>0) produced a bare
// "... OFFSET n" with no preceding LIMIT, which SQLite rejects outright
// ("SQL logic error: near \"OFFSET\": syntax error") - unlike Table's
// version, which already appended "LIMIT -1" for this case. Both now share
// paginationQuery, which always includes a LIMIT.
func TestTableWithTx_QueryManyWithPagination_OffsetOnly(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := helperOpenStore(t)
	t.Cleanup(func() { helperCloseStore(t, store) })

	table := helperTable[Foo](ctx, t, store)
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			t.Errorf("failed to rollback transaction: %v", err)
		}
	}()

	tableTx := table.WithTransaction(tx)

	for i := 1; i <= 5; i++ {
		if err := tableTx.Insert(ctx, Foo{ID: i, Name: "offset-only"}); err != nil {
			t.Fatalf("failed to insert test data: %v", err)
		}
	}

	results, err := tableTx.QueryManyWithPagination(ctx, Equal("$.name", "offset-only"), 0, 2)
	if err != nil {
		t.Fatalf("offset-only pagination failed: %v", err)
	}

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	expectedIDs := []int{3, 4, 5}
	for i, result := range results {
		if result.ID != expectedIDs[i] {
			t.Errorf("expected ID %d at position %d, got %d", expectedIDs[i], i, result.ID)
		}
	}
}

func TestTable_QueryManyWithPagination(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := helperOpenStore(t)
	t.Cleanup(func() { helperCloseStore(t, store) })

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Insert test data - 10 items with sequential IDs
	for i := 1; i <= 10; i++ {
		foo := Foo{
			ID:   i,
			Name: "pagination-test",
		}
		err := table.Insert(ctx, foo)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Test case 1: Limit only (limit=3, offset=0)
	t.Run("LimitOnly", func(t *testing.T) {
		t.Parallel()

		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 3, 0)
		if err != nil {
			t.Fatalf("Failed to query with pagination: %v", err)
		}

		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}

		// Verify we got the first 3 items
		expectedIDs := []int{1, 2, 3}
		for i, result := range results {
			if result.ID != expectedIDs[i] {
				t.Errorf("Expected ID %d at position %d, got %d", expectedIDs[i], i, result.ID)
			}
		}
	})

	// Test case 2: Offset only (limit=0, offset=5)
	t.Run("OffsetOnly", func(t *testing.T) {
		t.Parallel()

		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 0, 5)
		if err != nil {
			t.Fatalf("Failed to query with pagination: %v", err)
		}

		if len(results) != 5 {
			t.Errorf("Expected 5 results, got %d", len(results))
		}

		// Verify we got items 6-10
		expectedIDs := []int{6, 7, 8, 9, 10}
		for i, result := range results {
			if result.ID != expectedIDs[i] {
				t.Errorf("Expected ID %d at position %d, got %d", expectedIDs[i], i, result.ID)
			}
		}
	})

	// Test case 3: Both limit and offset (limit=3, offset=5)
	t.Run("LimitAndOffset", func(t *testing.T) {
		t.Parallel()

		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 3, 5)
		if err != nil {
			t.Fatalf("Failed to query with pagination: %v", err)
		}

		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}

		// Verify we got items 6-8
		expectedIDs := []int{6, 7, 8}
		for i, result := range results {
			if result.ID != expectedIDs[i] {
				t.Errorf("Expected ID %d at position %d, got %d", expectedIDs[i], i, result.ID)
			}
		}
	})

	// Test case 4: Zero limit and zero offset (should return all items)
	t.Run("ZeroLimitAndOffset", func(t *testing.T) {
		t.Parallel()

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
		t.Parallel()

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
		t.Parallel()

		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 20, 0)
		if err != nil {
			t.Fatalf("Failed to query with pagination: %v", err)
		}

		if len(results) != 10 {
			t.Errorf("Expected 10 results, got %d", len(results))
		}
	})
}

// TestTableWithTx_QueryManyWithPagination is not t.Parallel(): its subtests
// observe transaction state interleaved with tx.Commit() in this function
// body (see TestCombined_TransactionAndPagination in combined_test.go), so
// they cannot be made parallel themselves, and a parallel parent with
// non-parallel subtests is a lint violation (tparallel).
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
			ID:   i,
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
		expectedIDs := []int{3, 4, 5}
		for i, result := range results {
			if result.ID != expectedIDs[i] {
				t.Errorf("Expected ID %d at position %d, got %d", expectedIDs[i], i, result.ID)
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
		expectedIDs := []int{3, 4, 5}
		for i, result := range results {
			if result.ID != expectedIDs[i] {
				t.Errorf("Expected ID %d at position %d, got %d", expectedIDs[i], i, result.ID)
			}
		}
	})
}

func TestPagination_WithComplexQuery(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := helperOpenStore(t)
	t.Cleanup(func() { helperCloseStore(t, store) })

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Insert test data with different categories
	categories := []string{"category1", "category2", "category3"}
	id := 1
	for _, category := range categories {
		for i := 1; i <= 5; i++ {
			foo := Foo{
				ID:   id,
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
		t.Parallel()

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
		expectedIDs := []int{7, 8}
		for i, result := range results {
			if result.ID != expectedIDs[i] {
				t.Errorf("Expected ID %d at position %d, got %d", expectedIDs[i], i, result.ID)
			}
			if result.Name != "category2" {
				t.Errorf("Expected Name 'category2', got '%s'", result.Name)
			}
		}
	})
}
