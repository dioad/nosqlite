package nosqlite

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/glebarez/go-sqlite/compat"
)

// Store represents a document store backed by SQLite.
// It manages the database connection and provides methods for starting transactions and managing tables.
type Store struct {
	db *sql.DB
}

// Transaction represents an active database transaction.
// It provides methods for executing queries and managing transactions.
type Transaction struct {
	tx *sql.Tx
}

// NewStore creates a new Store with a connection to a SQLite database at the given file path.
// It also sets some recommended PRAGMAs for performance and concurrency (busy_timeout, synchronous=NORMAL, journal_mode=WAL).
func NewStore(filePath string) (*Store, error) {
	db, err := sql.Open("sqlite3", filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return NewStoreWithDB(db)
}

// NewStoreWithDB creates a new Store using an existing *sql.DB connection.
// It also sets some recommended PRAGMAs for performance and concurrency.
func NewStoreWithDB(db *sql.DB) (*Store, error) {
	// PRAGMA busy_timeout = 5000;
	_, err := db.Exec("PRAGMA busy_timeout = 5000")
	if err != nil {
		return nil, fmt.Errorf("failed to set busy_timeout: %w", err)
	}

	// PRAGMA synchronous = NORMAL;
	_, err = db.Exec("PRAGMA synchronous = NORMAL")
	if err != nil {
		return nil, fmt.Errorf("failed to set synchronous mode: %w", err)
	}

	// PRAGMA journal_mode = WAL;
	_, err = db.Exec("PRAGMA journal_mode = WAL")
	if err != nil {
		return nil, fmt.Errorf("failed to set journal mode: %w", err)
	}

	return &Store{db: db}, nil
}

// Ping verifies the connection to the database is still alive.
func (s *Store) Ping() error {
	return s.db.Ping()
}

// Close closes the underlying database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// BeginTx starts a new transaction with the provided context and options.
func (s *Store) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Transaction, error) {
	tx, err := s.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	return &Transaction{tx: tx}, nil
}

// Begin starts a new transaction with default options.
func (s *Store) Begin(ctx context.Context) (*Transaction, error) {
	return s.BeginTx(ctx, nil)
}

// Commit commits the current transaction.
func (tx *Transaction) Commit() error {
	if err := tx.tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

// Rollback aborts the current transaction.
func (tx *Transaction) Rollback() error {
	if err := tx.tx.Rollback(); err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}
	return nil
}

// Exec executes a query that doesn't return rows.
func (tx *Transaction) Exec(query string, args ...interface{}) (sql.Result, error) {
	return tx.tx.Exec(query, args...)
}

// Query executes a query that returns multiple rows.
func (tx *Transaction) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return tx.tx.Query(query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
func (tx *Transaction) QueryRow(query string, args ...interface{}) *sql.Row {
	return tx.tx.QueryRow(query, args...)
}

// ExecContext executes a query that doesn't return rows, with context support.
func (tx *Transaction) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return tx.tx.ExecContext(ctx, query, args...)
}

// QueryContext executes a query that returns multiple rows, with context support.
func (tx *Transaction) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return tx.tx.QueryContext(ctx, query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row, with context support.
func (tx *Transaction) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return tx.tx.QueryRowContext(ctx, query, args...)
}
