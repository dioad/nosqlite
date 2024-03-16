package nosqlite

import (
	"database/sql"
	"errors"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

type Bar struct {
	Name string `json:"name,omitempty"`
}

type Foo struct {
	Id   int    `json:"id,omitempty"`
	Name string `json:"name,omitempty"`
	Bar  Bar    `json:"bar,omitempty"`
}

func helperTempFile(t *testing.T) string {
	tmpDir := os.TempDir()
	f, err := os.CreateTemp(tmpDir, "test-nosqlite.db")
	if err != nil {
		panic(err)
	}
	return f.Name()
}

func helperOpenStoreWithFile(t *testing.T, fileName string) *Store {
	t.Helper()

	store, err := NewStore(fileName)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func helperOpenStore(t *testing.T) *Store {
	t.Helper()

	fileName := helperTempFile(t)

	return helperOpenStoreWithFile(t, fileName)
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

	c := Equal("$.name", "test")

	val, err := table.QueryOne(c)
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

	c1 := Equal("$.name", "test-one")

	_, err = table.QueryOne(c1)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}

	c2 := Equal("$.name", "test-two")

	val, err := table.QueryOne(c2)
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

	c := Equal("$.name", "nothing")

	_, err := table.QueryOne(c)
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

	c := Equal("$.name", "select-many")

	vals, err := table.QueryMany(c)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 2 {
		t.Errorf("expected 2 got %d", len(vals))
	}
}

func TestTableInjectValue(t *testing.T) {
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](t, store)

	foo := Foo{
		Name: "injection",
		Bar: Bar{
			Name: "one",
		},
	}

	err := table.Insert(foo)
	if err != nil {
		t.Fatal(err)
	}

	_, err = table.QueryOne(Equal("$.name", "injection' OR 1=1 --"))
	if err == nil {
		t.Fatal("expected error got nil")
	}

}

func TestTableInjectField(t *testing.T) {
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](t, store)

	foo := Foo{
		Name: "injection",
		Bar: Bar{
			Name: "one",
		},
	}

	err := table.Insert(foo)
	if err != nil {
		t.Fatal(err)
	}

	_, err = table.QueryOne(Equal("$.name' OR 1=1 --", "injection"))
	if err == nil {
		t.Fatal("expected error got nil")
	}

}

func TestTableDelete(t *testing.T) {
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](t, store)

	foo := Foo{
		Name: "delete",
		Bar: Bar{
			Name: "one",
		},
	}

	err := table.Insert(foo)
	if err != nil {
		t.Fatal(err)
	}

	c := Equal("$.name", "delete")

	err = table.Delete(c)
	if err != nil {
		t.Fatal(err)
	}

	_, err = table.QueryOne(c)
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
}

func TestTableSelectIn(t *testing.T) {
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](t, store)

	foos := []Foo{
		{
			Id:   1,
			Name: "select-one",
		},
		{
			Id:   2,
			Name: "select-two",
		},
		{
			Id:   7,
			Name: "select-seven",
		},
		{
			Id:   8,
			Name: "select-eight",
		},
	}

	for _, f := range foos {
		err := table.Insert(f)
		if err != nil {
			t.Fatal(err)
		}
	}

	condition := In("$.id", 1, 2, 3)

	//_, err := table.QueryMany(condition)
	vals, err := table.QueryMany(condition)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 2 {
		t.Errorf("expected 2 got %d", len(vals))
	}
}
