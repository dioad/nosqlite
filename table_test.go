package nosqlite

import (
	"context"
	"os"
	"testing"

	_ "github.com/glebarez/go-sqlite/compat"
)

type Bar struct {
	Name string `json:"name,omitzero"`
}

type Foo struct {
	ID   int      `json:"id,omitzero"`
	Name string   `json:"name,omitzero"`
	Bar  Bar      `json:"bar"`
	List []string `json:"list,omitzero"`
	Bool bool     `json:"bool,omitzero"`
}

type ID struct {
	ID string `json:"id,omitzero"`
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
	t.Parallel()

	tests := []struct {
		field    string
		expected string
	}{
		{"$.name", "name"},
		{"$.name.first", "name__first"},
		{"$.name.first.last", "name__first__last"},
		// Bare field names (no "$." prefix) are also valid - jsonFieldExpr
		// treats them as shorthand for "$.<field>" - and must not collapse to "".
		{"time", "time"},
		{"type", "type"},
	}

	for _, test := range tests {
		result := escapeFieldName(test.field)
		if result != test.expected {
			t.Errorf("expected %s got %s", test.expected, result)
		}
	}
}

// TestTable_CreateIndexes_BareFieldNamesDoNotCollide is a regression test:
// escapeFieldName used to reduce any field with no "." (e.g. plain "time" or
// "type", as opposed to "$.time"/"$.type") to "", so distinct single-field
// indexes on a table ended up requesting the same index name. The second
// CREATE INDEX IF NOT EXISTS then silently no-op'd instead of creating the
// index it was asked for.
func TestTable_CreateIndexes_BareFieldNamesDoNotCollide(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	names, err := table.CreateIndexes(ctx, []string{"name"}, []string{"id"})
	if err != nil {
		t.Fatal(err)
	}

	if names[0] == names[1] {
		t.Fatalf("expected distinct index names for distinct fields, got %q for both", names[0])
	}

	// Query sqlite_master directly rather than via hasIndex, which reports
	// true regardless of whether a matching row was actually found.
	for _, name := range names {
		var got string
		err := store.db.QueryRowContext(ctx,
			"SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? AND name=?",
			table.Name, name,
		).Scan(&got)
		if err != nil {
			t.Errorf("expected index %q to exist: %v", name, err)
		}
	}
}

func TestTableName(t *testing.T) {
	t.Parallel()

	result := tableName[Foo]()
	if result != "nosqlite_foo" {
		t.Errorf("expected nosqlite_foo got %s", result)
	}
}

func TestTableNameWithPointer(t *testing.T) {
	t.Parallel()

	result := tableName[*Foo]()
	if result != "nosqlite_foo" {
		t.Errorf("expected nosqlite_foo got %s", result)
	}
}

func TestJoinEscapedFieldNames(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

// TestTable_CreateIndex_RejectsInvalidField is a regression test: CreateIndex
// builds a CREATE INDEX statement with the field interpolated directly into
// the SQL text (SQLite does not permit bound parameters in index
// expressions - "SQL logic error: parameters prohibited in index
// expressions"), so the field must be validated before being embedded.
func TestTable_CreateIndex_RejectsInvalidField(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	_, err := table.CreateIndex(ctx, "$.name' ) -- ")
	if err == nil {
		t.Fatal("expected error for a field containing characters outside a JSON path, got nil")
	}
}

func TestTable_Count(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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

func TestTable_QueryBool(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	foo := Foo{
		Name: "bool",
		Bar: Bar{
			Name: "one",
		},
		Bool: true,
	}

	err := table.Insert(ctx, foo)
	if err != nil {
		t.Fatal(err)
	}

	res, err := table.QueryOne(ctx, True("$.bool"))
	if err != nil {
		t.Fatalf("expected  nil error, got %v", err)
	}

	if res == nil {
		t.Fatal("expected non nil result")
	}

	res, err = table.QueryOne(ctx, False("$.bool"))
	if err != nil {
		t.Fatalf("expected  nil error, got %v", err)
	}

	if res != nil {
		t.Fatal("expected  nil result")
	}

	res, err = table.QueryOne(ctx, Equal("$.bool", true))
	if err != nil {
		t.Fatalf("expected  nil error, got %v", err)
	}

	if res == nil {
		t.Fatal("expected non nil result")
	}
}

// TestTable_QueryOneInjectInField is a regression test for the field path
// previously being interpolated directly into SQL text. With the field path
// now passed as a bound parameter (see jsonFieldExpr in clause.go), even a
// quote-balanced payload that would have altered the query's structure is
// inert: SQLite treats it as a literal, non-matching JSON path rather than
// as SQL syntax, so it neither errors nor widens the match.
func TestTable_QueryOneInjectInField(t *testing.T) {
	t.Parallel()

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

	res, err := table.QueryOne(ctx, Equal("$.name' = 'injection' OR '1'='1", "injection"))
	if err != nil {
		t.Fatalf("expected no error for a bound (non-executable) field path, got: %v", err)
	}
	if res != nil {
		t.Fatal("expected nil result: a malicious field path must not widen the match")
	}
}

func TestTable_Delete(t *testing.T) {
	t.Parallel()

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

	rowsAffected, err := table.Delete(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if rowsAffected != 1 {
		t.Fatalf("expected 1 row affected, got %d", rowsAffected)
	}

	res, err := table.QueryOne(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if res != nil {
		t.Fatal("expected nil result")
	}

	// Verify count is 0 when no rows match
	rowsAffected, err = table.Delete(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	if rowsAffected != 0 {
		t.Fatalf("expected 0 rows affected, got %d", rowsAffected)
	}
}

func TestTable_QueryManyIn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	foos := []Foo{
		{
			ID:   1,
			Name: "select-one",
		},
		{
			ID:   2,
			Name: "select-two",
		},
		{
			ID:   7,
			Name: "select-seven",
		},
		{
			ID:   8,
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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

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
	if err != nil {
		t.Fatal(err)
	}
	if len(tableOneItems) != 1 {
		t.Fatalf("expected 1 got %d", len(tableOneItems))
	}

	tableTwoItems, err := tableTwo.All(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tableTwoItems) != 1 {
		t.Fatalf("expected 1 got %d", len(tableTwoItems))
	}

	_, err = tableTwo.Delete(ctx, Equal("$.id", id))
	if err != nil {
		t.Fatal(err)
	}

	tableOneItems, err = tableOne.All(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tableOneItems) != 1 {
		t.Fatalf("expected 1 got %d", len(tableOneItems))
	}

	tableTwoItems, err = tableTwo.All(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(tableTwoItems) != 0 {
		t.Fatalf("expected 0 got %d", len(tableTwoItems))
	}
}

type ParentStruct struct {
	ID    string
	Child struct {
		Value int
	}
}

func TestUpdateWithEmbeddedStruct(t *testing.T) {
	t.Parallel()

	var err error

	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[ParentStruct](ctx, t, store)

	obj := ParentStruct{
		ID: "some-id",
	}

	err = table.Insert(ctx, obj)
	if err != nil {
		t.Fatal(err)
	}

	obj.Child.Value = 13

	err = table.Update(ctx, Equal("$.ID", "some-id"), obj)
	if err != nil {
		t.Fatal(err)
	}

	res, err := table.QueryOne(ctx, Equal("$.ID", "some-id"))
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		t.Fatal("expected result got nil")
	}

	if res.Child.Value != 13 {
		t.Fatalf("expected 13 got %d", res.Child.Value)
	}
}

func TestTable_CreateIndexes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	names, err := table.CreateIndexes(ctx, []string{"$.name"}, []string{"$.id"})
	if err != nil {
		t.Fatal(err)
	}

	if len(names) != 2 {
		t.Fatalf("expected 2 index names, got %d", len(names))
	}
}
