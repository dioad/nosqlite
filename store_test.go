package nosqlite

import "testing"

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
}
