package nosqlite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fooIDs extracts the ID field from a slice of Foo, for comparing query
// results against an expected ID sequence in one assert.Equal call (which
// checks both length and order, unlike a manual element-by-element loop).
func fooIDs(results []Foo) []int {
	ids := make([]int, len(results))
	for i, result := range results {
		ids[i] = result.ID
	}

	return ids
}

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
		require.NoError(t, err)
	}

	_, err := table.CreateIndex(ctx, "$.id")
	require.NoError(t, err)

	results, err := table.QueryManyWithPagination(ctx, GreaterThan("$.id", 0), 0, 0)
	require.NoError(t, err)
	assert.Equal(t, insertOrderIDs, fooIDs(results))
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

			assert.Equal(t, test.want, paginationQuery("t", "1", test.limit, test.offset))
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
	require.NoError(t, err)
	defer func() {
		assert.NoError(t, tx.Rollback())
	}()

	tableTx := table.WithTransaction(tx)

	for i := 1; i <= 5; i++ {
		err := tableTx.Insert(ctx, Foo{ID: i, Name: "offset-only"})
		require.NoError(t, err)
	}

	results, err := tableTx.QueryManyWithPagination(ctx, Equal("$.name", "offset-only"), 0, 2)
	require.NoError(t, err)
	assert.Equal(t, []int{3, 4, 5}, fooIDs(results))
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
		require.NoError(t, err)
	}

	// Test case 1: Limit only (limit=3, offset=0)
	t.Run("LimitOnly", func(t *testing.T) {
		t.Parallel()

		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 3, 0)
		require.NoError(t, err)
		assert.Equal(t, []int{1, 2, 3}, fooIDs(results))
	})

	// Test case 2: Offset only (limit=0, offset=5)
	t.Run("OffsetOnly", func(t *testing.T) {
		t.Parallel()

		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 0, 5)
		require.NoError(t, err)
		assert.Equal(t, []int{6, 7, 8, 9, 10}, fooIDs(results))
	})

	// Test case 3: Both limit and offset (limit=3, offset=5)
	t.Run("LimitAndOffset", func(t *testing.T) {
		t.Parallel()

		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 3, 5)
		require.NoError(t, err)
		assert.Equal(t, []int{6, 7, 8}, fooIDs(results))
	})

	// Test case 4: Zero limit and zero offset (should return all items)
	t.Run("ZeroLimitAndOffset", func(t *testing.T) {
		t.Parallel()

		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 0, 0)
		require.NoError(t, err)
		assert.Len(t, results, 10)
	})

	// Test case 5: Offset beyond available data
	t.Run("OffsetBeyondData", func(t *testing.T) {
		t.Parallel()

		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 0, 15)
		require.NoError(t, err)
		assert.Empty(t, results)
	})

	// Test case 6: Limit larger than available data
	t.Run("LargeLimitSmallData", func(t *testing.T) {
		t.Parallel()

		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "pagination-test"), 20, 0)
		require.NoError(t, err)
		assert.Len(t, results, 10)
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
	require.NoError(t, err)

	// Get a table with transaction
	tableTx := table.WithTransaction(tx)

	// Insert test data within transaction - 10 items with sequential IDs
	for i := 1; i <= 10; i++ {
		foo := Foo{
			ID:   i,
			Name: "tx-pagination-test",
		}
		err := tableTx.Insert(ctx, foo)
		require.NoError(t, err)
	}

	// Test case 1: Basic pagination in transaction
	t.Run("BasicPaginationInTx", func(t *testing.T) {
		results, err := tableTx.QueryManyWithPagination(ctx, Equal("$.name", "tx-pagination-test"), 3, 2)
		require.NoError(t, err)
		assert.Equal(t, []int{3, 4, 5}, fooIDs(results))
	})

	// Test case 2: Verify data is not visible outside transaction
	t.Run("DataIsolationWithPagination", func(t *testing.T) {
		// Query from main table should return no results
		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "tx-pagination-test"), 0, 0)
		require.NoError(t, err)
		assert.Empty(t, results)
	})

	// Test case 3: Verify QueryMany calls QueryManyWithPagination
	t.Run("QueryManyCallsPagination", func(t *testing.T) {
		// QueryMany should call QueryManyWithPagination with limit=0, offset=0
		results, err := tableTx.QueryMany(ctx, Equal("$.name", "tx-pagination-test"))
		require.NoError(t, err)
		assert.Len(t, results, 10)
	})

	// Commit the transaction
	err = tx.Commit()
	require.NoError(t, err)

	// Test case 4: Verify data is now visible in main table after commit
	t.Run("PaginationAfterCommit", func(t *testing.T) {
		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "tx-pagination-test"), 3, 2)
		require.NoError(t, err)
		assert.Equal(t, []int{3, 4, 5}, fooIDs(results))
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
			require.NoError(t, err)
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
		require.NoError(t, err)

		// Should get items with IDs 7 and 8 (skipping 6 due to offset=1)
		assert.Equal(t, []int{7, 8}, fooIDs(results))
		for _, result := range results {
			assert.Equal(t, "category2", result.Name)
		}
	})
}
