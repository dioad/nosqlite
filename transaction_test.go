package nosqlite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransaction_Commit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Start a transaction
	tx, err := store.Begin(ctx)
	require.NoError(t, err, "failed to begin transaction")

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
	require.NoError(t, err, "failed to insert data in transaction")

	// Verify data exists in transaction but not in main table yet
	txResult, err := tableTx.QueryOne(ctx, Equal("$.name", "transaction-commit"))
	require.NoError(t, err, "failed to query data in transaction")
	require.NotNil(t, txResult, "expected to find data in transaction")

	mainResult, err := table.QueryOne(ctx, Equal("$.name", "transaction-commit"))
	require.NoError(t, err, "failed to query data in main table")
	require.Nil(t, mainResult, "expected not to find data in main table yet")

	// Commit the transaction
	err = tx.Commit()
	require.NoError(t, err, "failed to commit transaction")

	// Verify data now exists in main table
	mainResult, err = table.QueryOne(ctx, Equal("$.name", "transaction-commit"))
	require.NoError(t, err, "failed to query data in main table after commit")
	require.NotNil(t, mainResult, "expected to find data in main table after commit")
	assert.Equal(t, "commit", mainResult.Bar.Name)
}

func TestTransaction_Rollback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Start a transaction
	tx, err := store.Begin(ctx)
	require.NoError(t, err, "failed to begin transaction")

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
	require.NoError(t, err, "failed to insert data in transaction")

	// Verify data exists in transaction
	txResult, err := tableTx.QueryOne(ctx, Equal("$.name", "transaction-rollback"))
	require.NoError(t, err, "failed to query data in transaction")
	require.NotNil(t, txResult, "expected to find data in transaction")

	// Rollback the transaction
	err = tx.Rollback()
	require.NoError(t, err, "failed to rollback transaction")

	// Verify data does not exist in main table
	mainResult, err := table.QueryOne(ctx, Equal("$.name", "transaction-rollback"))
	require.NoError(t, err, "failed to query data in main table after rollback")
	assert.Nil(t, mainResult, "expected not to find data in main table after rollback")
}

func TestTableWithTx_CRUD(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	// Create a table
	table := helperTable[Foo](ctx, t, store)

	// Start a transaction
	tx, err := store.Begin(ctx)
	require.NoError(t, err, "failed to begin transaction")

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
	require.NoError(t, err, "failed to insert data in transaction")

	// Test QueryOne
	result, err := tableTx.QueryOne(ctx, Equal("$.name", "tx-crud"))
	require.NoError(t, err, "failed to query data in transaction")
	require.NotNil(t, result, "expected to find data in transaction")
	assert.Equal(t, "original", result.Bar.Name)

	// Test Update
	foo.Bar.Name = "updated"
	err = tableTx.Update(ctx, Equal("$.name", "tx-crud"), foo)
	require.NoError(t, err, "failed to update data in transaction")

	// Verify update
	result, err = tableTx.QueryOne(ctx, Equal("$.name", "tx-crud"))
	require.NoError(t, err, "failed to query data after update")
	require.NotNil(t, result, "expected to find data after update")
	assert.Equal(t, "updated", result.Bar.Name)

	// Test Count
	count, err := tableTx.Count(ctx)
	require.NoError(t, err, "failed to count data in transaction")
	assert.EqualValues(t, 1, count)

	// Test Delete
	_, err = tableTx.Delete(ctx, Equal("$.name", "tx-crud"))
	require.NoError(t, err, "failed to delete data in transaction")

	// Verify delete
	result, err = tableTx.QueryOne(ctx, Equal("$.name", "tx-crud"))
	require.NoError(t, err, "failed to query data after delete")
	assert.Nil(t, result, "expected not to find data after delete")

	// Commit the transaction
	err = tx.Commit()
	require.NoError(t, err, "failed to commit transaction")
}

func TestTransaction_Isolation(t *testing.T) {
	t.Parallel()

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
	require.NoError(t, err, "failed to insert initial data")

	// Start a transaction
	tx, err := store.Begin(ctx)
	require.NoError(t, err, "failed to begin transaction")

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
	require.NoError(t, err, "failed to update data in transaction")

	// Verify data is updated in transaction
	txResult, err := tableTx.QueryOne(ctx, Equal("$.name", "isolation-test"))
	require.NoError(t, err, "failed to query data in transaction")
	require.NotNil(t, txResult, "expected to find data in transaction")
	assert.Equal(t, "updated-in-tx", txResult.Bar.Name)

	// Verify data is not updated in main table
	mainResult, err := table.QueryOne(ctx, Equal("$.name", "isolation-test"))
	require.NoError(t, err, "failed to query data in main table")
	require.NotNil(t, mainResult, "expected to find data in main table")
	assert.Equal(t, "initial", mainResult.Bar.Name)

	// Commit the transaction
	err = tx.Commit()
	require.NoError(t, err, "failed to commit transaction")

	// Verify data is now updated in main table
	mainResult, err = table.QueryOne(ctx, Equal("$.name", "isolation-test"))
	require.NoError(t, err, "failed to query data in main table after commit")
	require.NotNil(t, mainResult, "expected to find data in main table after commit")
	assert.Equal(t, "updated-in-tx", mainResult.Bar.Name)
}
