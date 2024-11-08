package nosqlite

import (
	"context"
	"os"
	"testing"

	_ "github.com/glebarez/go-sqlite/compat"
)

type Bar struct {
	Name string `json:"name,omitempty"`
}

type Foo struct {
	Id   int      `json:"id,omitempty"`
	Name string   `json:"name,omitempty"`
	Bar  Bar      `json:"bar,omitempty"`
	List []string `json:"list,omitempty"`
}

type ID struct {
	ID string `json:"id,omitempty"`
}

type IDOne ID
type IDTwo ID

func helperTempFile(t *testing.T) string {
	t.Helper()

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

func helperTable[T any](ctx context.Context, t *testing.T, store *Store) *Table[T] {
	t.Helper()

	table, err := NewTable[T](ctx, store)
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

func TestTableName(t *testing.T) {
	result := tableName[Foo]()
	if result != "nosqlite_foo" {
		t.Errorf("expected nosqlite_foo got %s", result)
	}
}

func TestTableNameWithPointer(t *testing.T) {
	result := tableName[*Foo]()
	if result != "nosqlite_foo" {
		t.Errorf("expected nosqlite_foo got %s", result)
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

func TestTable_Insert(t *testing.T) {
	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	tag := Foo{
		Name: "test",
		Bar: Bar{
			Name: "insert",
		},
	}

	err := table.Insert(ctx, tag)
	if err != nil {
		t.Fatal(err)
	}

	c := Equal("$.name", "test")

	val, err := table.QueryOne(ctx, c)
	if err != nil {
		t.Fatal(err)
	}

	if val.Bar.Name != "insert" {
		t.Errorf("expected japan got %s", val.Bar.Name)
	}
}

func TestTable_Update(t *testing.T) {

	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

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

	err := table.Insert(ctx, foo1)
	if err != nil {
		t.Fatal(err)
	}

	updateClause := Equal("$.name", "test-one")

	err = table.Update(ctx, updateClause, foo2)
	if err != nil {
		t.Fatal(err)
	}

	c1 := Equal("$.name", "test-one")

	_, err = table.QueryOne(ctx, c1)
	if err != nil {
		t.Fatal(err)
	}

	c2 := Equal("$.name", "test-two")

	val, err := table.QueryOne(ctx, c2)
	if err != nil {
		t.Fatal(err)
	}

	if val.Bar.Name != "update-two" {
		t.Errorf("expected update-two got %s", val.Bar.Name)
	}
}

func TestTable_CreateIndex(t *testing.T) {
	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[*Foo](ctx, t, store)

	name, err := table.CreateIndex(ctx, "$.name", "$.bar.name")
	if err != nil {
		t.Fatal(err)
	}

	if name != "idx_nosqlite_foo_name_bar__name" {
		t.Errorf("expected idx_foo_name_bar__name got %s", name)

	}

	_, err = table.hasIndex(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTable_Count(t *testing.T) {
	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

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
		err := table.Insert(ctx, tag)
		if err != nil {
			t.Fatal(err)
		}
	}

	count, err := table.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("expected 2 got %d", count)
	}
}

func TestTable_QueryOneNoResults(t *testing.T) {
	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	c := Equal("$.name", "nothing")

	res, err := table.QueryOne(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if res != nil {
		t.Fatal("expected nil result")
	}
}

func TestTable_QueryMany(t *testing.T) {

	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

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
		err := table.Insert(ctx, tag)
		if err != nil {
			t.Fatal(err)
		}
	}

	c := Equal("$.name", "select-many")

	vals, err := table.QueryMany(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 2 {
		t.Errorf("expected 2 got %d", len(vals))
	}
}

func TestTable_All(t *testing.T) {

	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

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
		err := table.Insert(ctx, tag)
		if err != nil {
			t.Fatal(err)
		}
	}

	vals, err := table.All(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 2 {
		t.Errorf("expected 2 got %d", len(vals))
	}
}

func TestTable_QueryOneInjectInValue(t *testing.T) {
	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	foo := Foo{
		Name: "injection",
		Bar: Bar{
			Name: "one",
		},
	}

	err := table.Insert(ctx, foo)
	if err != nil {
		t.Fatal(err)
	}

	res, err := table.QueryOne(ctx, Equal("$.name", "injection' OR 1=1 --"))
	if err != nil {
		t.Fatal("expected error got nil")
	}

	if res != nil {
		t.Fatal("expected nil result")
	}
}

func TestTable_QueryOneInjectInField(t *testing.T) {
	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	foo := Foo{
		Name: "injection",
		Bar: Bar{
			Name: "one",
		},
	}

	err := table.Insert(ctx, foo)
	if err != nil {
		t.Fatal(err)
	}

	_, err = table.QueryOne(ctx, Equal("$.name' OR 1=1 --", "injection"))
	if err == nil {
		t.Fatal("expected error got nil")
	}
}

func TestTable_Delete(t *testing.T) {
	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	foo := Foo{
		Name: "delete",
		Bar: Bar{
			Name: "one",
		},
	}

	err := table.Insert(ctx, foo)
	if err != nil {
		t.Fatal(err)
	}

	c := Equal("$.name", "delete")

	err = table.Delete(ctx, c)
	if err != nil {
		t.Fatal(err)
	}

	res, err := table.QueryOne(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if res != nil {
		t.Fatal("expected nil result")
	}
}

func TestTable_QueryManyIn(t *testing.T) {
	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

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
		err := table.Insert(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
	}

	condition := In("$.id", 1, 2, 3)

	vals, err := table.QueryMany(ctx, condition)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 2 {
		t.Errorf("expected 2 got %d", len(vals))
	}
}

func TestTable_QueryManyContainsAll(t *testing.T) {
	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	foos := []Foo{
		{
			Name: "contains-one",
			List: []string{"one", "two", "three"},
		},
		{
			Name: "contains-two",
			List: []string{"three", "four", "five"},
		},
		{
			Name: "contains-three",
			List: []string{"two", "three", "four"},
		},
	}

	for _, f := range foos {
		err := table.Insert(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
	}

	// condition := ContainsAll("$.list", "two", "three")
	condition := ContainsAll("$.list", "two")

	vals, err := table.QueryMany(ctx, condition)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 2 {
		t.Errorf("expected 2 got %d", len(vals))
	}
}

func TestTable_QueryManyContainsAny(t *testing.T) {
	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	foos := []Foo{
		{
			Name: "contains-one",
			List: []string{"one", "two", "three"},
		},
		{
			Name: "contains-two",
			List: []string{"three", "four", "five"},
		},
		{
			Name: "contains-three",
			List: []string{"two", "three", "four"},
		},
	}

	for _, f := range foos {
		err := table.Insert(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
	}

	condition := ContainsAny("$.list", "one", "two", "three")

	vals, err := table.QueryMany(ctx, condition)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 3 {
		t.Errorf("expected 3 got %d", len(vals))
	}
}

func TestTable_QueryManyContains(t *testing.T) {
	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	foos := []Foo{
		{
			Name: "contains-one",
			List: []string{"one", "two", "three"},
		},
		{
			Name: "contains-two",
			List: []string{"three", "four", "five"},
		},
		{
			Name: "contains-three",
			List: []string{"two", "three", "four"},
		},
	}

	for _, f := range foos {
		err := table.Insert(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
	}

	condition := Contains("$.list", "one")

	vals, err := table.QueryMany(ctx, condition)
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != 1 {
		t.Errorf("expected 1 got %d", len(vals))
	}
}

func TestDeleteFromTables(t *testing.T) {
	var err error

	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	tableOne := helperTable[IDOne](ctx, t, store)
	tableTwo := helperTable[IDTwo](ctx, t, store)

	id := "some-id"

	itemOne := IDOne{ID: id}
	itemTwo := IDTwo{ID: id}

	err = tableOne.Insert(ctx, itemOne)
	if err != nil {
		t.Fatal(err)
	}
	err = tableTwo.Insert(ctx, itemTwo)
	if err != nil {
		t.Fatal(err)
	}

	tableOneItems, err := tableOne.All(ctx)
	if len(tableOneItems) != 1 {
		t.Fatalf("expected 1 got %d", len(tableOneItems))
	}

	tableTwoItems, err := tableTwo.All(ctx)
	if len(tableTwoItems) != 1 {
		t.Fatalf("expected 1 got %d", len(tableTwoItems))
	}

	err = tableTwo.Delete(ctx, Equal("$.id", id))
	if err != nil {
		t.Fatal(err)
	}

	tableOneItems, err = tableOne.All(ctx)
	if len(tableOneItems) != 1 {
		t.Fatalf("expected 1 got %d", len(tableOneItems))
	}

	tableTwoItems, err = tableTwo.All(ctx)
	if len(tableTwoItems) != 0 {
		t.Fatalf("expected 0 got %d", len(tableTwoItems))
	}
}
