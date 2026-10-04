package nosqlite

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewStore(t *testing.T) {
	t.Parallel()

	fileName := helperTempFile(t)

	store, err := NewStore(fileName)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, store.Close())
	}()

	assert.NoError(t, store.Ping())
}

func TestNewStore_Defaults(t *testing.T) {
	t.Parallel()

	store, err := NewStore(helperTempFile(t))
	require.NoError(t, err)
	defer helperCloseStore(t, store)

	assertPragma(t, store, "busy_timeout", "5000")
	assertPragma(t, store, "synchronous", "1") // SQLite reports synchronous as its integer level; 1 = NORMAL
}

func TestNewStore_Options(t *testing.T) {
	t.Parallel()

	store, err := NewStore(helperTempFile(t),
		WithBusyTimeout(250*time.Millisecond),
		WithSynchronous(SynchronousFull),
	)
	require.NoError(t, err)
	defer helperCloseStore(t, store)

	assertPragma(t, store, "busy_timeout", "250")
	assertPragma(t, store, "synchronous", "2") // 2 = FULL
}

func TestNewStoreWithDB_Options(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", helperTempFile(t))
	require.NoError(t, err)

	store, err := NewStoreWithDB(db, WithBusyTimeout(250*time.Millisecond))
	require.NoError(t, err)
	defer helperCloseStore(t, store)

	assertPragma(t, store, "busy_timeout", "250")
}

func assertPragma(t *testing.T, store *Store, pragma, want string) {
	t.Helper()

	var got string
	row := store.db.QueryRowContext(context.Background(), "PRAGMA "+pragma)
	require.NoError(t, row.Scan(&got), "failed to read pragma %s", pragma)
	assert.Equal(t, want, got, "pragma %s", pragma)
}

func TestStore_Begin(t *testing.T) {
	t.Parallel()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	ctx := context.Background()

	tx, err := store.Begin(ctx)
	require.NoError(t, err, "Begin failed")

	assert.NoError(t, tx.Rollback(), "Rollback failed")
}
