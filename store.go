package nosqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

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

// SynchronousMode controls SQLite's synchronous pragma, which trades commit
// durability for write throughput. See
// https://www.sqlite.org/pragma.html#pragma_synchronous.
type SynchronousMode string

const (
	SynchronousOff    SynchronousMode = "OFF"
	SynchronousNormal SynchronousMode = "NORMAL"
	SynchronousFull   SynchronousMode = "FULL"
	SynchronousExtra  SynchronousMode = "EXTRA"
)

const (
	defaultBusyTimeout = 5 * time.Second
	defaultSynchronous = SynchronousNormal
)

// storeConfig holds the tunable pragma values a Store is opened with. Zero
// value is meaningless; always build one from newStoreConfig so the defaults
// are applied.
type storeConfig struct {
	busyTimeout time.Duration
	synchronous SynchronousMode
}

func newStoreConfig(opts []Option) storeConfig {
	cfg := storeConfig{
		busyTimeout: defaultBusyTimeout,
		synchronous: defaultSynchronous,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// Option configures tunable pragma values for a Store created by NewStore or
// NewStoreWithDB. Only busy_timeout and synchronous are configurable -
// journal_mode and the transaction locking mode are fixed (see
// fixedDSNParams) because they are load-bearing for correctness under
// concurrent access, not a performance preference.
type Option func(*storeConfig)

// WithBusyTimeout overrides the busy_timeout pragma applied to every
// connection the Store opens: how long a connection waits for a lock held by
// another writer before giving up with SQLITE_BUSY. Defaults to 5 seconds.
func WithBusyTimeout(d time.Duration) Option {
	return func(c *storeConfig) { c.busyTimeout = d }
}

// WithSynchronous overrides the synchronous pragma applied to every
// connection the Store opens. Defaults to SynchronousNormal, which is safe
// (durable across an application crash) and, combined with the fixed
// journal_mode=WAL, does not fsync on every commit the way FULL does.
func WithSynchronous(mode SynchronousMode) Option {
	return func(c *storeConfig) { c.synchronous = mode }
}

// fixedDSNParams are applied via DSN query parameters (rather than a PRAGMA
// executed after Open) because SQLite pragmas are per-connection state:
// database/sql opens additional physical connections under concurrent load,
// and a PRAGMA run once via db.Exec only lands on whichever single pooled
// connection happened to run it. The glebarez/go-sqlite driver applies
// _pragma DSN parameters to every connection it opens, so this is the only
// way to guarantee these are in effect on all of them.
//
// These two are fixed rather than exposed as Options because they are
// correctness-critical, not tunable:
//
// journal_mode=WAL is required for the concurrency this store is designed
// for (readers do not block writers).
//
// _txlock=immediate switches every transaction from SQLite's default
// deferred mode to BEGIN IMMEDIATE, which acquires the write lock up front
// instead of at the first write statement. Callers here commonly run a
// check-then-write transaction (read a row, then update/delete it) - under
// a deferred transaction, several such transactions can all start from the
// same read snapshot and then race to upgrade to a writer; the losers hit
// SQLITE_BUSY from a stale-snapshot conflict that busy_timeout cannot
// resolve by waiting, because retrying the same doomed transaction never
// succeeds. BEGIN IMMEDIATE instead serialises transactions at the start
// (queuing on the write lock, which busy_timeout legitimately waits out),
// so each one always reads a fresh snapshot before writing. Allowing a
// caller to override this back to deferred would silently reintroduce that
// bug, so it is not an Option.
const fixedDSNParams = "_pragma=journal_mode(WAL)&_txlock=immediate"

// pragmaDSN renders cfg's tunable pragmas and the fixed correctness-critical
// settings as DSN query parameters.
func pragmaDSN(cfg storeConfig) string {
	return fmt.Sprintf(
		"_pragma=busy_timeout(%d)&_pragma=synchronous(%s)&%s",
		cfg.busyTimeout.Milliseconds(), cfg.synchronous, fixedDSNParams,
	)
}

// NewStore creates a new Store with a connection to a SQLite database at the given file path.
// It also sets some recommended PRAGMAs for performance and concurrency (busy_timeout, synchronous=NORMAL, journal_mode=WAL);
// use WithBusyTimeout and WithSynchronous to override the tunable ones.
func NewStore(filePath string, opts ...Option) (*Store, error) {
	cfg := newStoreConfig(opts)

	dsn := filePath
	if strings.Contains(dsn, "?") {
		dsn += "&" + pragmaDSN(cfg)
	} else {
		dsn += "?" + pragmaDSN(cfg)
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return newStoreWithDB(db, cfg)
}

// NewStoreWithDB creates a new Store using an existing *sql.DB connection.
// It also sets some recommended PRAGMAs for performance and concurrency;
// use WithBusyTimeout and WithSynchronous to override the tunable ones.
//
// These are applied via db.Exec, which - unlike the DSN parameters NewStore
// uses - only guarantees the pragma is in effect on the single connection
// that happens to run this Exec. Callers that open db themselves and expect
// these pragmas under concurrent access should set them via DSN parameters
// instead (see pragmaDSN), or use NewStore. This also cannot restore
// _txlock=immediate, since that is a connection-level DSN setting rather
// than a PRAGMA and so has no db.Exec equivalent; a db passed in here
// without it is exposed to the stale-snapshot SQLITE_BUSY races described
// on fixedDSNParams under concurrent check-then-write transactions.
func NewStoreWithDB(db *sql.DB, opts ...Option) (*Store, error) {
	return newStoreWithDB(db, newStoreConfig(opts))
}

func newStoreWithDB(db *sql.DB, cfg storeConfig) (*Store, error) {
	// PRAGMA busy_timeout = ...;
	_, err := db.Exec(fmt.Sprintf("PRAGMA busy_timeout = %d", cfg.busyTimeout.Milliseconds()))
	if err != nil {
		return nil, fmt.Errorf("failed to set busy_timeout: %w", err)
	}

	// PRAGMA synchronous = ...;
	_, err = db.Exec(fmt.Sprintf("PRAGMA synchronous = %s", cfg.synchronous))
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
func (tx *Transaction) Exec(query string, args ...any) (sql.Result, error) {
	return tx.tx.Exec(query, args...)
}

// Query executes a query that returns multiple rows.
func (tx *Transaction) Query(query string, args ...any) (*sql.Rows, error) {
	return tx.tx.Query(query, args...)
}

// QueryRow executes a query that is expected to return at most one row.
func (tx *Transaction) QueryRow(query string, args ...any) *sql.Row {
	return tx.tx.QueryRow(query, args...)
}

// ExecContext executes a query that doesn't return rows, with context support.
func (tx *Transaction) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.tx.ExecContext(ctx, query, args...)
}

// QueryContext executes a query that returns multiple rows, with context support.
func (tx *Transaction) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.tx.QueryContext(ctx, query, args...)
}

// QueryRowContext executes a query that is expected to return at most one row, with context support.
func (tx *Transaction) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.tx.QueryRowContext(ctx, query, args...)
}
