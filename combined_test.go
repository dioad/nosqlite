package nosqlite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCombined_TransactionAndPagination is not t.Parallel(): its subtests
// observe transaction state (pre- and post-commit) interleaved with
// tx.Commit() in this function body, so they cannot be made parallel
// themselves, and a parallel parent with non-parallel subtests is a lint
// violation (tparallel).
func TestCombined_TransactionAndPagination(t *testing.T) {
	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Insert some initial data in the main table
	for i := 1; i <= 5; i++ {
		foo := Foo{
			ID:   i,
			Name: "main-data",
			Bar: Bar{
				Name: "original",
			},
		}
		err := table.Insert(ctx, foo)
		require.NoError(t, err, "failed to insert initial data")
	}

	// Start a transaction
	tx, err := store.Begin(ctx)
	require.NoError(t, err, "failed to begin transaction")

	// Get a table with transaction
	tableTx := table.WithTransaction(tx)

	// Insert additional data in the transaction
	for i := 6; i <= 15; i++ {
		foo := Foo{
			ID:   i,
			Name: "tx-data",
			Bar: Bar{
				Name: "transaction",
			},
		}
		err := tableTx.Insert(ctx, foo)
		require.NoError(t, err, "failed to insert transaction data")
	}

	// Update some of the main data within the transaction
	for i := 1; i <= 3; i++ {
		foo := Foo{
			ID:   i,
			Name: "main-data",
			Bar: Bar{
				Name: "updated-in-tx",
			},
		}
		err := tableTx.Update(ctx, Equal("$.id", i), foo)
		require.NoError(t, err, "failed to update data in transaction")
	}

	// Test case 1: Pagination on transaction-only data
	t.Run("PaginationOnTransactionData", func(t *testing.T) {
		results, err := tableTx.QueryManyWithPagination(ctx, Equal("$.name", "tx-data"), 3, 2)
		require.NoError(t, err, "failed to query transaction data with pagination")

		assert.Equal(t, []int{8, 9, 10}, fooIDs(results))
		for _, result := range results {
			assert.Equal(t, "transaction", result.Bar.Name)
		}
	})

	// Test case 2: Pagination on updated data in transaction
	t.Run("PaginationOnUpdatedData", func(t *testing.T) {
		// Query updated items in transaction
		results, err := tableTx.QueryManyWithPagination(ctx, And(
			Equal("$.name", "main-data"),
			Equal("$.bar.name", "updated-in-tx"),
		), 2, 0)
		require.NoError(t, err, "failed to query updated data with pagination")

		assert.Equal(t, []int{1, 2}, fooIDs(results))
		for _, result := range results {
			assert.Equal(t, "updated-in-tx", result.Bar.Name)
		}

		// Query same items in main table - should have original values
		mainResults, err := table.QueryManyWithPagination(ctx, And(
			Equal("$.name", "main-data"),
			In("$.id", 1, 2),
		), 0, 0)
		require.NoError(t, err, "failed to query main data with pagination")

		assert.Len(t, mainResults, 2)
		for _, result := range mainResults {
			assert.Equal(t, "original", result.Bar.Name, "main table value")
		}
	})

	// Test case 3: Verify transaction data is not visible in main table
	t.Run("TransactionDataIsolation", func(t *testing.T) {
		// Query from main table should not see tx-data
		results, err := table.QueryManyWithPagination(ctx, Equal("$.name", "tx-data"), 0, 0)
		require.NoError(t, err, "failed to query with pagination from main table")
		assert.Empty(t, results, "main table should not see tx-data")
	})

	// Test case 4: All data visible in transaction
	t.Run("AllDataVisibleInTransaction", func(t *testing.T) {
		// All data should be visible in transaction
		results, err := tableTx.QueryManyWithPagination(ctx, All(), 0, 0)
		require.NoError(t, err, "failed to query all data in transaction")
		assert.Len(t, results, 15)
	})

	// Commit the transaction
	err = tx.Commit()
	require.NoError(t, err, "failed to commit transaction")

	// Test case 5: Verify all data is now visible in main table after commit
	t.Run("AllDataVisibleAfterCommit", func(t *testing.T) {
		// Query all data from main table after commit
		results, err := table.QueryManyWithPagination(ctx, All(), 0, 0)
		require.NoError(t, err, "failed to query all data after commit")
		assert.Len(t, results, 15)

		// Verify tx-data is now visible
		txResults, err := table.QueryManyWithPagination(ctx, Equal("$.name", "tx-data"), 3, 2)
		require.NoError(t, err, "failed to query tx-data after commit")
		assert.Len(t, txResults, 3)

		// Verify updates are now visible
		updatedResults, err := table.QueryManyWithPagination(ctx, And(
			Equal("$.name", "main-data"),
			Equal("$.bar.name", "updated-in-tx"),
		), 0, 0)
		require.NoError(t, err, "failed to query updated data after commit")
		assert.Len(t, updatedResults, 3)
	})
}

// TestCombined_TransactionRollbackWithPagination is not t.Parallel(): see
// TestCombined_TransactionAndPagination.
func TestCombined_TransactionRollbackWithPagination(t *testing.T) {
	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Insert some initial data in the main table
	for i := 1; i <= 5; i++ {
		foo := Foo{
			ID:   i,
			Name: "rollback-test",
			Bar: Bar{
				Name: "original",
			},
		}
		err := table.Insert(ctx, foo)
		require.NoError(t, err, "failed to insert initial data")
	}

	// Start a transaction
	tx, err := store.Begin(ctx)
	require.NoError(t, err, "failed to begin transaction")

	// Get a table with transaction
	tableTx := table.WithTransaction(tx)

	// Update data in transaction
	for i := 1; i <= 5; i++ {
		foo := Foo{
			ID:   i,
			Name: "rollback-test",
			Bar: Bar{
				Name: "will-be-rolled-back",
			},
		}
		err := tableTx.Update(ctx, Equal("$.id", i), foo)
		require.NoError(t, err, "failed to update data in transaction")
	}

	// Insert additional data in transaction
	for i := 6; i <= 10; i++ {
		foo := Foo{
			ID:   i,
			Name: "rollback-test",
			Bar: Bar{
				Name: "will-be-rolled-back",
			},
		}
		err := tableTx.Insert(ctx, foo)
		require.NoError(t, err, "failed to insert data in transaction")
	}

	// Verify changes are visible in transaction with pagination
	results, err := tableTx.QueryManyWithPagination(ctx, Equal("$.bar.name", "will-be-rolled-back"), 3, 2)
	require.NoError(t, err, "failed to query data in transaction")
	assert.Len(t, results, 3)

	// Rollback the transaction
	err = tx.Rollback()
	require.NoError(t, err, "failed to rollback transaction")

	// Verify changes are not visible in main table after rollback
	t.Run("DataNotVisibleAfterRollback", func(t *testing.T) {
		// Query for updated data - should not exist
		results, err := table.QueryManyWithPagination(ctx, Equal("$.bar.name", "will-be-rolled-back"), 0, 0)
		require.NoError(t, err, "failed to query data after rollback")
		assert.Empty(t, results)

		// Verify original data is intact
		origResults, err := table.QueryManyWithPagination(ctx, Equal("$.bar.name", "original"), 0, 0)
		require.NoError(t, err, "failed to query original data after rollback")
		assert.Len(t, origResults, 5)

		// Verify total count is still 5
		count, err := table.Count(ctx)
		require.NoError(t, err, "failed to count data after rollback")
		assert.EqualValues(t, 5, count)
	})
}
