package nosqlite

import (
	"context"
	"testing"
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
