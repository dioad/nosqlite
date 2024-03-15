package nosqlite

import (
	"database/sql"
	"errors"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type Bar struct {
	Name string `json:"name"`
}

type Foo struct {
	Name string `json:"name"`
	Bar  Bar    `json:"bar"`
}

func helperTempFile(t *testing.T) string {
	tmpDir := os.TempDir()
	f, err := os.CreateTemp(tmpDir, "test-nosqlite.db")
	if err != nil {
		panic(err)
	}
	return f.Name()
}

func helperOpenStore(t *testing.T) *Store {
	t.Helper()

	fileName := helperTempFile(t)

	store, err := NewStore(fileName)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func helperCloseStore(t *testing.T, store *Store) {
	t.Helper()

	err := store.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func helperTable[T any](t *testing.T, store *Store) *Table[T] {
	t.Helper()

	table, err := NewTable[T](store)
	if err != nil {
		t.Fatal(err)
	}

	return table
}

func TestEscapeFieldName(t *testing.T) {
	tests := []struct {
		field    string
		expected string
	}{
		{"$.name", "name"},
		{"$.name.first", "name__first"},
		{"$.name.first.last", "name__first__last"},
	}

	for _, test := range tests {
		result := escapeFieldName(test.field)
		if result != test.expected {
			t.Errorf("expected %s got %s", test.expected, result)
		}
	}
}

func TestJoinEscapedFieldNames(t *testing.T) {
	tests := []struct {
		fields   []string
		expected string
	}{
		{[]string{"$.name", "$.country"}, "name_country"},
		{[]string{"$.name.first", "$.country"}, "name__first_country"},
		{[]string{"$.name.first.last", "$.country"}, "name__first__last_country"},
		{[]string{"$.name.first_last", "$.country"}, "name__first_last_country"},
	}

	for _, test := range tests {
		result := joinEscapedFieldNames(test.fields...)
		if result != test.expected {
			t.Errorf("expected %s got %s", test.expected, result)
		}
	}
}

func TestTableInsert(t *testing.T) {
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](t, store)

	tag := Foo{
		Name: "test",
		Bar: Bar{
			Name: "insert",
		},
	}

	err := table.Insert(tag)
	if err != nil {
		t.Fatal(err)
	}

	val, err := table.QueryOne("$.name", "test")
	if err != nil {
		t.Fatal(err)
	}

	if val.Bar.Name != "insert" {
		t.Errorf("expected japan got %s", val.Bar.Name)
	}
}

func TestTableUpdate(t *testing.T) {
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](t, store)

	foo1 := Foo{
		Name: "test-one",
		Bar: Bar{
			Name: "update-one",
		},
	}

	foo2 := Foo{
		Name: "test-two",
		Bar: Bar{
			Name: "update-two",
		},
	}

	err := table.Insert(foo1)
	if err != nil {
		t.Fatal(err)
	}

	err = table.Update("$.name", "test-one", foo2)
	if err != nil {
		t.Fatal(err)
	}

	_, err = table.QueryOne("$.name", "test-one")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}

	val, err := table.QueryOne("$.name", "test-two")
	if err != nil {
		t.Fatal(err)
	}

	if val.Bar.Name != "update-two" {
		t.Errorf("expected update-two got %s", val.Bar.Name)
	}
}

func TestTableCreateIndex(t *testing.T) {
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](t, store)

	name, err := table.CreateIndex("$.name", "$.bar.name")
	if err != nil {
		t.Fatal(err)
	}

	_, err = table.hasIndex(name)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTableCount(t *testing.T) {
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](t, store)

	foos := []Foo{
		{
			Name: "count-one",
			Bar: Bar{
				Name: "one",
			},
		}, {
			Name: "count-two",
			Bar: Bar{
				Name: "two",
			},
		},
	}

	for _, tag := range foos {
		err := table.Insert(tag)
		if err != nil {
			t.Fatal(err)
		}
	}

	count, err := table.Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("expected 2 got %d", count)
	}
}

func TestTableSelectNoResults(t *testing.T) {
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](t, store)

	_, err := table.QueryOne("$.name", "nothing")
	if err == nil {
		t.Fatal("expected error got nil")
	}

	if errors.Is(err, sql.ErrNoRows) {
		return
	}

	t.Fatal(err)
}

func TestTableSelectMany(t *testing.T) {
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](t, store)

	foos := []Foo{{
		Name: "select-many",
		Bar: Bar{
			Name: "one",
		},
	}, {
		Name: "select-many",
		Bar: Bar{
			Name: "two",
		},
	}}

	for _, tag := range foos {
		err := table.Insert(tag)
		if err != nil {
			t.Fatal(err)
		}
	}

	vals, err := table.QueryMany("$.name", "select-many")
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 2 {
		t.Errorf("expected 2 got %d", len(vals))
	}
	/*
		var version string
		err = db.QueryRow("SELECT sqlite_version()").Scan(&version)
		// t.Fatal(version)

		var country string
		err = db.QueryRow("select tag->'$.country.name' from foo where tag->>? = 'mattn'", "name").Scan(&country)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(country)

		var tag Tag
		err = db.QueryRow("select tag from foo where tag->>? = 'mattn'", "name").Scan(&tag)
		if err != nil {
			t.Fatal(err)
		}

		fmt.Println(tag.Name)

		tag.Country.Name = "日本"
		_, err = db.Exec(`update foo set tag = ? where tag->>? == 'mattn'`, &tag, "name")
		if err != nil {
			t.Fatal(err)
		}

		err = db.QueryRow("select tag->'$.country.name' from foo where tag->>'name' = 'mattn'").Scan(&country)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(country)

	*/
}
