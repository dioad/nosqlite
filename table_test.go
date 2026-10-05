package nosqlite

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	_ "github.com/glebarez/go-sqlite/compat"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)

	return f.Name()
}

func helperOpenStoreWithFile(t *testing.T, fileName string) *Store {
	t.Helper()

	store, err := NewStore(fileName)
	require.NoError(t, err)

	return store
}

func helperOpenStore(t *testing.T) *Store {
	t.Helper()

	fileName := helperTempFile(t)

	return helperOpenStoreWithFile(t, fileName)
}

func helperCloseStore(t *testing.T, store *Store) {
	t.Helper()

	require.NoError(t, store.Close())
}

func helperTable[T any](ctx context.Context, t *testing.T, store *Store) *Table[T] {
	t.Helper()

	table, err := NewTable[T](ctx, store)
	require.NoError(t, err)

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
		assert.Equal(t, test.expected, result)
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
	require.NoError(t, err)
	require.NotEqual(t, names[0], names[1], "expected distinct index names for distinct fields")

	// Query sqlite_master directly rather than via hasIndex, which reports
	// true regardless of whether a matching row was actually found.
	for _, name := range names {
		var got string
		err := store.db.QueryRowContext(ctx,
			"SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? AND name=?",
			table.Name, name,
		).Scan(&got)
		assert.NoError(t, err, "expected index %q to exist", name)
	}
}

func TestTableName(t *testing.T) {
	t.Parallel()

	result := tableName[Foo]()
	assert.Equal(t, "nosqlite_foo", result)
}

func TestTableNameWithPointer(t *testing.T) {
	t.Parallel()

	result := tableName[*Foo]()
	assert.Equal(t, "nosqlite_foo", result)
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
		assert.Equal(t, test.expected, result)
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
	require.NoError(t, err)

	c := Equal("$.name", "test")

	val, err := table.QueryOne(ctx, c)
	require.NoError(t, err)
	require.NotNil(t, val)
	assert.Equal(t, "insert", val.Bar.Name)
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
	require.NoError(t, err)

	updateClause := Equal("$.name", "test-one")

	err = table.Update(ctx, updateClause, foo2)
	require.NoError(t, err)

	c1 := Equal("$.name", "test-one")

	_, err = table.QueryOne(ctx, c1)
	require.NoError(t, err)

	c2 := Equal("$.name", "test-two")

	val, err := table.QueryOne(ctx, c2)
	require.NoError(t, err)
	require.NotNil(t, val)
	assert.Equal(t, "update-two", val.Bar.Name)
}

func TestTable_CreateIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[*Foo](ctx, t, store)

	name, err := table.CreateIndex(ctx, "$.name", "$.bar.name")
	require.NoError(t, err)
	assert.Equal(t, "idx_nosqlite_foo_name_bar__name", name)

	var got string
	err = store.db.QueryRowContext(ctx,
		"SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? AND name=?",
		table.Name, name,
	).Scan(&got)
	require.NoError(t, err, "expected index %q to exist", name)
}

// TestTable_CreateIndex_QueryUsesIndex is a regression test for jsonField:
// a query clause built from a field that satisfies validIndexField must
// interpolate that field literally, using the exact expression text
// CreateIndex emits, so SQLite's planner recognizes the index rather than
// falling back to a full table scan. A clause that instead bound the field
// as a parameter (data->>?) would be semantically equivalent but would
// never match the index, since SQLite matches expression indexes by
// literal expression text.
func TestTable_CreateIndex_QueryUsesIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	_, err := table.CreateIndex(ctx, "$.name")
	require.NoError(t, err)

	for i := range 50 {
		require.NoError(t, table.Insert(ctx, Foo{Name: fmt.Sprintf("name-%d", i)}))
	}

	clause := Equal("$.name", "name-7")
	queryPlan := fmt.Sprintf("EXPLAIN QUERY PLAN SELECT data FROM `%s` WHERE %s", table.Name, clause.Clause()) // #nosec G201 -- table.Name is derived from the Go type name via tableName[T](); clause.Clause() embeds only "?" placeholders bound via clause.Values(), except for a field path that passes validIndexField, which is interpolated as a validated literal (see jsonField) rather than unsanitized caller data

	rows, err := store.db.QueryContext(ctx, queryPlan, clause.Values()...)
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var plan strings.Builder
	for rows.Next() {
		var id, parent, notused int
		var detail string
		require.NoError(t, rows.Scan(&id, &parent, &notused, &detail))
		plan.WriteString(detail)
		plan.WriteString("\n")
	}
	require.NoError(t, rows.Err())

	assert.Contains(t, plan.String(), "USING INDEX", "expected the $.name index to be used, got plan:\n"+plan.String())
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
	require.Error(t, err, "expected error for a field containing characters outside a JSON path")
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
		require.NoError(t, err)
	}

	count, err := table.Count(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 2, count)
}

func TestTable_QueryOneNoResults(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	c := Equal("$.name", "nothing")

	res, err := table.QueryOne(ctx, c)
	require.NoError(t, err)
	assert.Nil(t, res)
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
		require.NoError(t, err)
	}

	c := Equal("$.name", "select-many")

	vals, err := table.QueryMany(ctx, c)
	require.NoError(t, err)
	assert.Len(t, vals, 2)
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
		require.NoError(t, err)
	}

	vals, err := table.All(ctx)
	require.NoError(t, err)
	assert.Len(t, vals, 2)
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
	require.NoError(t, err)

	res, err := table.QueryOne(ctx, Equal("$.name", "injection' OR 1=1 --"))
	require.NoError(t, err)
	assert.Nil(t, res)
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
	require.NoError(t, err)

	res, err := table.QueryOne(ctx, True("$.bool"))
	require.NoError(t, err)
	assert.NotNil(t, res)

	res, err = table.QueryOne(ctx, False("$.bool"))
	require.NoError(t, err)
	assert.Nil(t, res)

	res, err = table.QueryOne(ctx, Equal("$.bool", true))
	require.NoError(t, err)
	assert.NotNil(t, res)
}

// numericIDFoo exercises numeric types other than the literal int/float64
// that condition[T].Values() used to special-case.
type numericIDFoo struct {
	ID int64 `json:"id,omitzero"`
}

// TestTable_QueryOneNonLiteralNumericType is a regression test:
// condition[T].Values() used to type-switch on the comparison value and
// stringify anything that wasn't a literal int/float64/bool/string, so a
// field typed as int64 (or uint, float32, etc.) silently never matched -
// SQLite considers an INTEGER column value and a bound TEXT value unequal
// regardless of their textual content. Values() now passes c.Value through
// unconverted, so database/sql binds it with its real type.
func TestTable_QueryOneNonLiteralNumericType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[numericIDFoo](ctx, t, store)

	err := table.Insert(ctx, numericIDFoo{ID: 7})
	require.NoError(t, err)

	res, err := table.QueryOne(ctx, Equal[int64]("$.id", int64(7)))
	require.NoError(t, err)
	require.NotNil(t, res, "expected to find the row by its int64 id")
	assert.EqualValues(t, 7, res.ID)
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
	require.NoError(t, err)

	res, err := table.QueryOne(ctx, Equal("$.name' = 'injection' OR '1'='1", "injection"))
	require.NoError(t, err, "expected no error for a bound (non-executable) field path")
	assert.Nil(t, res, "a malicious field path must not widen the match")
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
	require.NoError(t, err)

	c := Equal("$.name", "delete")

	rowsAffected, err := table.Delete(ctx, c)
	require.NoError(t, err)
	require.EqualValues(t, 1, rowsAffected)

	res, err := table.QueryOne(ctx, c)
	require.NoError(t, err)
	assert.Nil(t, res)

	// Verify count is 0 when no rows match
	rowsAffected, err = table.Delete(ctx, c)
	require.NoError(t, err)
	assert.EqualValues(t, 0, rowsAffected)
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
		require.NoError(t, err)
	}

	condition := In("$.id", 1, 2, 3)

	vals, err := table.QueryMany(ctx, condition)
	require.NoError(t, err)
	assert.Len(t, vals, 2)
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
		require.NoError(t, err)
	}

	// condition := ContainsAll("$.list", "two", "three")
	condition := ContainsAll("$.list", "two")

	vals, err := table.QueryMany(ctx, condition)
	require.NoError(t, err)
	assert.Len(t, vals, 2)
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
		require.NoError(t, err)
	}

	condition := ContainsAny("$.list", "one", "two", "three")

	vals, err := table.QueryMany(ctx, condition)
	require.NoError(t, err)
	assert.Len(t, vals, 3)
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
		require.NoError(t, err)
	}

	condition := Contains("$.list", "one")

	vals, err := table.QueryMany(ctx, condition)
	require.NoError(t, err)
	assert.Len(t, vals, 1)
}

func TestDeleteFromTables(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	tableOne := helperTable[IDOne](ctx, t, store)
	tableTwo := helperTable[IDTwo](ctx, t, store)

	id := "some-id"

	itemOne := IDOne{ID: id}
	itemTwo := IDTwo{ID: id}

	err := tableOne.Insert(ctx, itemOne)
	require.NoError(t, err)
	err = tableTwo.Insert(ctx, itemTwo)
	require.NoError(t, err)

	tableOneItems, err := tableOne.All(ctx)
	require.NoError(t, err)
	require.Len(t, tableOneItems, 1)

	tableTwoItems, err := tableTwo.All(ctx)
	require.NoError(t, err)
	require.Len(t, tableTwoItems, 1)

	_, err = tableTwo.Delete(ctx, Equal("$.id", id))
	require.NoError(t, err)

	tableOneItems, err = tableOne.All(ctx)
	require.NoError(t, err)
	assert.Len(t, tableOneItems, 1)

	tableTwoItems, err = tableTwo.All(ctx)
	require.NoError(t, err)
	assert.Empty(t, tableTwoItems)
}

type ParentStruct struct {
	ID    string
	Child struct {
		Value int
	}
}

func TestUpdateWithEmbeddedStruct(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[ParentStruct](ctx, t, store)

	obj := ParentStruct{
		ID: "some-id",
	}

	err := table.Insert(ctx, obj)
	require.NoError(t, err)

	obj.Child.Value = 13

	err = table.Update(ctx, Equal("$.ID", "some-id"), obj)
	require.NoError(t, err)

	res, err := table.QueryOne(ctx, Equal("$.ID", "some-id"))
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, 13, res.Child.Value)
}

func TestTable_CreateIndexes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	names, err := table.CreateIndexes(ctx, []string{"$.name"}, []string{"$.id"})
	require.NoError(t, err)
	assert.Len(t, names, 2)
}

// TestTable_UpdateWithCount exercises UpdateWithCount's rows-affected
// return, including the case Update's own signature can't distinguish: a
// clause that matches nothing.
func TestTable_UpdateWithCount(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store := helperOpenStore(t)
	defer helperCloseStore(t, store)

	table := helperTable[Foo](ctx, t, store)

	err := table.Insert(ctx, Foo{Name: "update-with-count", Bar: Bar{Name: "original"}})
	require.NoError(t, err)

	count, err := table.UpdateWithCount(ctx, Equal("$.name", "update-with-count"), Foo{Name: "update-with-count", Bar: Bar{Name: "updated"}})
	require.NoError(t, err)
	assert.EqualValues(t, 1, count)

	count, err = table.UpdateWithCount(ctx, Equal("$.name", "no-such-row"), Foo{})
	require.NoError(t, err)
	assert.EqualValues(t, 0, count)
}
