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

	store, err := NewStore(context.Background(), fileName)
	require.NoError(t, err)

	defer func() {
		require.NoError(t, store.Close())
	}()

	assert.NoError(t, store.Ping(context.Background()))
}

func TestNewStore_Defaults(t *testing.T) {
	t.Parallel()

	store, err := NewStore(context.Background(), helperTempFile(t))
	require.NoError(t, err)
	defer helperCloseStore(t, store)

	assertPragma(t, store, "busy_timeout", "5000")
	assertPragma(t, store, "synchronous", "1") // SQLite reports synchronous as its integer level; 1 = NORMAL
}

func TestNewStore_Options(t *testing.T) {
	t.Parallel()

	store, err := NewStore(context.Background(), helperTempFile(t),
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

	store, err := NewStoreWithDB(context.Background(), db, WithBusyTimeout(250*time.Millisecond))
	require.NoError(t, err)
	defer helperCloseStore(t, store)

	assertPragma(t, store, "busy_timeout", "250")
}

func TestNewStore_WALAutoCheckpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		opts []Option
		want string
	}{
		{name: "disabled", opts: []Option{WithWALAutoCheckpoint(0)}, want: "0"},
		{name: "custom", opts: []Option{WithWALAutoCheckpoint(250)}, want: "250"},
		{name: "default", want: "1000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store, err := NewStore(context.Background(), helperTempFile(t), tt.opts...)
			require.NoError(t, err)
			defer helperCloseStore(t, store)

			assertPragma(t, store, "wal_autocheckpoint", tt.want)
		})
	}
}

func TestNewStoreWithDB_WALAutoCheckpoint(t *testing.T) {
	t.Parallel()

	db, err := sql.Open("sqlite3", helperTempFile(t))
	require.NoError(t, err)

	store, err := NewStoreWithDB(context.Background(), db, WithWALAutoCheckpoint(0))
	require.NoError(t, err)
	defer helperCloseStore(t, store)

	assertPragma(t, store, "wal_autocheckpoint", "0")
}

func TestStore_Ping_CancelledContext(t *testing.T) {
	t.Parallel()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.Ping(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestStore_Ping_DeadlineWhileConnectionBusy(t *testing.T) {
	t.Parallel()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	// With a single pooled connection held by an open transaction, Ping has
	// to wait for a connection and must give up when its deadline passes.
	store.db.SetMaxOpenConns(1)

	tx, err := store.Begin(context.Background())
	require.NoError(t, err)

	defer func() {
		require.NoError(t, tx.Rollback())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err = store.Ping(ctx)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 2*time.Second)
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

func TestTransaction_Exec_CancelledContext(t *testing.T) {
	t.Parallel()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	tx, err := store.Begin(context.Background())
	require.NoError(t, err)

	defer func() {
		require.NoError(t, tx.Rollback())
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = tx.Exec(ctx, "SELECT 1")
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestNewStore_CancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store, err := NewStore(ctx, helperTempFile(t))
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, store)
}
