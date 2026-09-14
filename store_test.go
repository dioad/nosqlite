package nosqlite

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestNewStore(t *testing.T) {
	fileName := helperTempFile(t)

	store, err := NewStore(fileName)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err := store.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	err = store.Ping()
	if err != nil {
		t.Errorf("Ping failed: %v", err)
	}
}

func TestNewStore_Defaults(t *testing.T) {
	store, err := NewStore(helperTempFile(t))
	if err != nil {
		t.Fatal(err)
	}
	defer helperCloseStore(t, store)

	assertPragma(t, store, "busy_timeout", "5000")
	assertPragma(t, store, "synchronous", "1") // SQLite reports synchronous as its integer level; 1 = NORMAL
}

func TestNewStore_Options(t *testing.T) {
	store, err := NewStore(helperTempFile(t),
		WithBusyTimeout(250*time.Millisecond),
		WithSynchronous(SynchronousFull),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer helperCloseStore(t, store)

	assertPragma(t, store, "busy_timeout", "250")
	assertPragma(t, store, "synchronous", "2") // 2 = FULL
}

func TestNewStoreWithDB_Options(t *testing.T) {
	db, err := sql.Open("sqlite3", helperTempFile(t))
	if err != nil {
		t.Fatal(err)
	}

	store, err := NewStoreWithDB(db, WithBusyTimeout(250*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer helperCloseStore(t, store)

	assertPragma(t, store, "busy_timeout", "250")
}

func assertPragma(t *testing.T, store *Store, pragma, want string) {
	t.Helper()

	var got string
	row := store.db.QueryRow("PRAGMA " + pragma)
	if err := row.Scan(&got); err != nil {
		t.Fatalf("failed to read pragma %s: %v", pragma, err)
	}
	if got != want {
		t.Errorf("pragma %s = %q, want %q", pragma, got, want)
	}
}

func TestStore_Begin(t *testing.T) {
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	err = tx.Rollback()
	if err != nil {
		t.Errorf("Rollback failed: %v", err)
	}
}
