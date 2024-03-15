package nosqlite

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

// Table represents a table in the database
type Table[T any] struct {
	store *Store
}

func (n *Table[T]) getTableName() string {
	a := *new(T)
	t := reflect.TypeOf(a)
	return strings.ToLower(t.Name())
}

// NewTable creates a new table with the given type T
func NewTable[T any](store *Store) (*Table[T], error) {
	table := &Table[T]{store: store}

	err := table.CreateTable()
	if err != nil {
		return nil, err
	}
	return table, nil
}

func escapeFieldName(field string) string {
	_, after, _ := strings.Cut(field, ".")

	a := strings.ReplaceAll(after, ".", "__")
	a = strings.ReplaceAll(a, " ", "_")
	return a
}

func joinEscapedFieldNames(fields ...string) string {
	parts := make([]string, len(fields))
	for i, field := range fields {
		parts[i] = escapeFieldName(field)
	}

	return strings.Join(parts, "_")
}

func constructIndexName(tableName string, fields ...string) string {
	joinedParts := joinEscapedFieldNames(fields...)

	return fmt.Sprintf("idx_%s_%s", tableName, joinedParts)
}

func (n *Table[T]) indexName(fields ...string) string {
	return constructIndexName(n.getTableName(), fields...)
}

// CreateTable creates the table if it does not exist
func (n *Table[T]) CreateTable() error {
	return n.createTableWithName(n.getTableName())
}

func (n *Table[T]) createTableWithName(tableName string) error {
	createStatement := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (data jsonb)", tableName)
	_, err := n.store.db.Exec(createStatement, tableName)
	return err
}

// Count returns the number of rows in the table
func (n *Table[T]) Count() (uint64, error) {
	var c uint64
	tableName := n.getTableName()
	count := n.store.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) AS count FROM `%s`", tableName))
	err := count.Scan(&c)
	return c, err
}

// CreateIndex creates an index on the given fields
func (n *Table[T]) CreateIndex(fields ...string) (string, error) {
	tableName := n.getTableName()
	indexName := n.indexName(fields...)

	indexFields := make([]string, len(fields))
	for i, field := range fields {
		indexFields[i] = fmt.Sprintf("data->>'%s'", field)
	}

	indexes := strings.Join(indexFields, ", ")

	createIndexStatement := fmt.Sprintf("CREATE INDEX IF NOT EXISTS `%s` ON `%s` (%s)", indexName, tableName, indexes)
	_, err := n.store.db.Exec(createIndexStatement)
	return indexName, err
}

// hasIndex returns true if the index exists
func (n *Table[T]) hasIndex(indexName string) (bool, error) {
	tableName := n.getTableName()
	_, err := n.store.db.Exec("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name=? AND name=?", tableName, indexName)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Delete removes one or more items from the table
func (n *Table[T]) Delete(field, value string) error {
	tableName := n.getTableName()
	deleteStatement := fmt.Sprintf("DELETE FROM `%s` WHERE data->>? = ?", tableName)
	_, err := n.store.db.Exec(deleteStatement, field, value)
	return err
}

// Insert adds a new item to the table
func (n *Table[T]) Insert(data T) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	tableName := n.getTableName()
	insertStatement := fmt.Sprintf("INSERT INTO `%s` (data) VALUES (?)", tableName)
	_, err = n.store.db.Exec(insertStatement, string(b))
	return err
}

// QueryOne returns a single item from the table
func (n *Table[T]) QueryOne(field, value string) (*T, error) {
	var data string
	//tag->'$.country.name'
	tableName := n.getTableName()
	queryStatement := fmt.Sprintf("SELECT data FROM `%s` WHERE data->>? = ?", tableName)
	row := n.store.db.QueryRow(queryStatement, field, value)
	err := row.Scan(&data)
	if err != nil {
		return nil, err
	}
	var result T
	err = json.Unmarshal([]byte(data), &result)
	return &result, err
}

// QueryMany returns multiple items from the table
// can we use http://doug-martin.github.io/goqu/ for this?
func (n *Table[T]) QueryMany(field, value string) ([]T, error) {
	var data string
	var results []T
	tableName := n.getTableName()
	queryStatement := fmt.Sprintf("SELECT data FROM %s WHERE data->>? = ?", tableName)
	rows, err := n.store.db.Query(queryStatement, field, value)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&data)
		if err != nil {
			return nil, err
		}
		var result T
		err = json.Unmarshal([]byte(data), &result)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, nil
}

// Update changes one or more items in the table
func (n *Table[T]) Update(field, value string, newVal T) error {
	b, err := json.Marshal(newVal)
	if err != nil {
		return err
	}
	tableName := n.getTableName()
	updateStatement := fmt.Sprintf("UPDATE %s SET data = ? WHERE data->>? = ?", tableName)
	_, err = n.store.db.Exec(updateStatement, string(b), field, value)
	return err
}
