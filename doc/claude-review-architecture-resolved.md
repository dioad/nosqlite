# Architecture Review: Resolved Findings — `github.com/dioad/nosqlite`

---

### 1. Clause field names are interpolated into SQL unsanitized — contradicts the `#nosec` justification already in the code ✅ Resolved

- **File(s):** `clause.go` (`jsonField`, `condition.Clause`), `table.go:318` (`CreateIndex`)
- **Dimension(s):** Security, Correctness
- **Priority:** High
- **Resolved in:** e109f61
- **Description:** `jsonField` builds `fmt.Sprintf("data->>'%s'", field)` directly from the caller-supplied field string, with no escaping. Every clause constructor (`Equal`, `GreaterThan`, `In`, `Between`, `Contains`, ...) routes through it, so a field value containing a `'` breaks out of the string literal. `table.go`'s `Delete`/`QueryOne`/`QueryMany`/`Update` carry a `#nosec G201` comment claiming `"clause.Clause() interpolates only escapeFieldName-sanitized identifiers"` — this is false. `escapeFieldName` is used *only* to build index names (`constructIndexName`); it is never called from `jsonField` or `condition.Clause()`. The same unescaped interpolation existed independently in `CreateIndex` (table.go:317-319): `escapeFieldName` there sanitized only the generated *index name*, not the `data->>'%s'` column expression built from the same raw `field` just three lines above.
- **Outcome:** Query clauses now pass the JSON field path as a bound parameter (`data->>?`) via `Clause()`/`Values()`, the same mechanism already used for comparison values — verified live that SQLite accepts the path operand as a bound parameter and that a quote-balanced injection payload (`"$.name' = 'injection' OR '1'='1"`) becomes inert (no error, no widened match) rather than altering the query. `CreateIndex` builds DDL, where SQLite rejects bound parameters in index expressions (verified: `"SQL logic error: parameters prohibited in index expressions"`), so its fields are instead validated against an allow-listed JSON-path character set (`validateIndexFields`, extracted to a separate function to keep `CreateIndex` itself from gaining complexity) before being interpolated. Corrected the four stale `#nosec G201` comments in `table.go` to describe the actual mechanism. Replaced `TestTable_QueryOneInjectInField`'s single-payload assertion (which only proved one malformed string broke SQL syntax) with a quote-balanced payload proving the field cannot widen a match, and added `TestTable_CreateIndex_RejectsInvalidField` for the DDL validation path. Complexity delta: `CreateIndex` 1→2 (a single `if err != nil { return }` guard — the minimal irreducible cost of the validation check); the extracted `validateIndexFields` helper carries the loop's complexity (3) as new code rather than a regression of an existing function. `go build`, `go vet`, and `go test -race ./...` all pass.

### 2. Comparison clauses silently corrupt every numeric type except `int`, `float64`, and `bool` ✅ Resolved

- **File(s):** `clause.go` (`condition.Values`)
- **Dimension(s):** Correctness
- **Priority:** High
- **Resolved in:** 94c40d9
- **Description:** `condition[T].Values()` type-switched on `any(c.Value)` and only recognized the literal types `string`, `int`, `float64`, `bool`; every other type permitted by the `number` constraint (`int8`/`16`/`32`/`64`, `uint`/`uint8`/`16`/`32`/`64`, `float32`) fell through to `default: return []any{fmt.Sprintf("%v", v)}`, converting the value to a string before it was bound as a SQL parameter. Verified end-to-end against a real store before the fix: inserting `numericIDFoo{ID: int64(7)}` and querying `Equal[int64]("$.id", int64(7))` returned **no match** for the row that existed, because SQLite's type-affinity comparison rules never consider an INTEGER column value equal to a bound TEXT value. `betweenCondition.Values()` already returned `c.From`/`c.To` directly with no such switch and was unaffected, confirming the switch was the defect rather than an intentional normalization step.
- **Outcome:** `condition[T].Values()` now returns `[]any{c.Field, c.Value}` directly, exactly as `betweenCondition.Values()` already did — `database/sql` binds every type the `number` constraint allows correctly without help. Added `TestTable_QueryOneNonLiteralNumericType`, which inserts a row with an `int64` id and confirms `Equal[int64]` now finds it (it did not, before this commit). Complexity delta: `condition.Values()` 1→0 (the type switch was removed entirely). `go build`, `go vet`, and `go test -race ./...` all pass.

### 3. Paginated queries have no `ORDER BY` — page contents and ordering are not actually guaranteed ✅ Resolved

- **File(s):** `table.go` (`Table.QueryManyWithPagination`, `TableWithTx.QueryManyWithPagination`, new `paginationQuery` helper)
- **Dimension(s):** Correctness
- **Priority:** High
- **Resolved in:** (this commit)
- **Description:** Both `QueryManyWithPagination` implementations built `SELECT data FROM ... WHERE ... LIMIT n OFFSET m` with no `ORDER BY` clause. SQL does not guarantee row order without one; the only reason `pagination_test.go`'s exact-sequence assertions passed was that SQLite happened to return rows from a plain table scan in rowid insertion order — an implementation detail of the query plan, not a documented guarantee.
- **Outcome:** Extracted the query-building logic duplicated across both `QueryManyWithPagination` methods into a single `paginationQuery(tableName, clauseSQL string, limit, offset uint64) string` helper that always appends `ORDER BY rowid` before `LIMIT`/`OFFSET`. `TestPaginationQuery` pins the exact generated SQL for all four limit/offset combinations. While unifying the two call sites, found and fixed a second, previously untested bug the extraction exposed: `TableWithTx.QueryManyWithPagination` omitted `LIMIT` entirely when `limit == 0`, so an offset-only call produced a bare `... OFFSET n` with no preceding `LIMIT`, which SQLite rejects outright (verified live: `"SQL logic error: near \"OFFSET\": syntax error"`) — `Table`'s version already worked around this with `LIMIT -1` for the same case, so the shared helper now does this unconditionally for both. Added `TestTableWithTx_QueryManyWithPagination_OffsetOnly` (failed before this fix) and `TestTable_QueryManyWithPagination_StableUnderIndex` (a realistic scenario with rows inserted out of rowid/id order and a competing index on the field used in the query, confirming callers observe the documented stable order). Complexity delta: `(*Table).QueryManyWithPagination` 15→12, `(*TableWithTx).QueryManyWithPagination` 14→12 (both dropped, from de-duplicating the query-building logic into the new `paginationQuery` helper, which carries complexity 3 as new code). `go build`, `go vet`, and `go test -race ./...` all pass.

### 4. `rows.Close()` error can never reach the caller — unnamed return values make the deferred capture a no-op ✅ Resolved

- **File(s):** `table.go` (`Table.QueryManyWithPagination`, `TableWithTx.QueryManyWithPagination`)
- **Dimension(s):** Correctness
- **Priority:** Medium
- **Resolved in:** (this commit)
- **Description:** Both methods deferred a closure that assigned `rows.Close()`'s error into the local `err` variable "so it isn't lost" — but the enclosing function signature used unnamed return values (`([]T, error)`). `return results, nil` evaluates and binds the return values *before* the deferred closure runs, so mutating the local `err` afterward had no effect on what was already returned. Duplicated verbatim at both call sites.
- **Outcome:** Both methods now use named return values (`(results []T, err error)`), so the deferred `rows.Close()` assignment actually reaches the caller when a close fails after a successful, fully-drained iteration. Simplified the deferred closure's nested `if closeErr != nil { if err == nil { ... } }` into a single `if closeErr != nil && err == nil`. A genuine `rows.Close()` failure is very difficult to force deterministically against the real SQLite driver without introducing a mock abstraction this small library doesn't otherwise have (and which would be disproportionate to add solely for this), so this was verified via the well-established Go named-return/defer semantics plus the full existing test suite continuing to pass unmodified, rather than a new forced-failure test. Complexity delta: both methods 12→10 (the nested defer condition flattened). `go build`, `go vet`, and `go test -race ./...` all pass.

### 5. `hasIndex` always returns `true` and has no caller that uses its result — delete it ✅ Resolved

- **File(s):** `table.go`
- **Dimension(s):** Correctness
- **Priority:** Medium
- **Resolved in:** (this commit)
- **Description:** `hasIndex` ran a `SELECT ... FROM sqlite_master` via `db.ExecContext`, which discards any result rows, then unconditionally returned `true` unless the database itself errored — it reported an index exists regardless of whether a matching row was found. Its only caller, `TestTable_CreateIndex`, discarded the boolean.
- **Outcome:** Deleted `hasIndex` entirely. `TestTable_CreateIndex`'s call to it (which only checked for an error that could never meaningfully occur) was replaced with a direct `sqlite_master` query that actually asserts the index row exists — the same pattern `TestTable_CreateIndexes_BareFieldNamesDoNotCollide` already used elsewhere in the same file. Complexity delta: n/a (function removed). `go build`, `go vet`, and `go test -race ./...` all pass.

### 6. No executable `Example` functions and no `examples/` directory, despite the project's own documentation rules requiring both ✅ Resolved

- **File(s):** README.md; `example_test.go` (new), `examples/basic-usage/` (new)
- **Dimension(s):** Documentation
- **Priority:** Medium
- **Resolved in:** (this commit)
- **Description:** The project's own `.claude/rules/go-testing.md` requires all code examples to be executable `func Example...` functions; `AGENTS.md` separately requires standalone runnable programs under `examples/`. Neither existed: the README's Quick Start was a plain, unverified code block, and there were no `func Example...` functions anywhere in the repo.
- **Outcome:** Added `example_test.go` with `Example()` (mirroring the README's Quick Start exactly — opens a store, creates a table, indexes a field, inserts a document, queries it with a combined `And`/`GreaterThanOrEqual`/`Contains` clause, and verifies the printed output) and `ExampleAnd()` (the README's Querying API combinator snippet, asserting the exact generated `Clause()` string). Both run and are output-checked by `go test`. Added `examples/basic-usage/` with a complete, runnable `package main` program and its own `README.md` (prerequisites, run instructions, expected output), per `AGENTS.md`'s convention — built and run directly to confirm its documented output (`Found: Alice (30)`) matches reality. `go build ./...`, `go vet ./...`, and `go test -race ./...` all pass with the new files included.
