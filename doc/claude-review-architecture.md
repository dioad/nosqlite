# Architecture Review: `github.com/dioad/nosqlite`

_Reviewed: 2026-10-03 — branch `main` (f5b7682)_

---

## Executive Summary

`nosqlite` is a small, focused library (5 source files, ~1,800 lines) with a clean public API and generally idiomatic Go. The codebase builds and tests cleanly, and `store.go`'s PRAGMA/locking choices show real engineering care (the `fixedDSNParams` and `WithSynchronous` doc comments explain *why*, not just *what*). The most critical theme is that the query layer — the part of the API every caller touches — has three independent, verified correctness defects: field names are interpolated into SQL unsanitized despite a `#nosec` comment that claims otherwise, equality/comparison clauses silently corrupt any numeric type other than literal `int`/`float64` (confirmed: `Equal[int64]` returns zero rows for an existing match), and paginated queries have no `ORDER BY`, so the stable page ordering the test suite assumes is not actually guaranteed by SQLite. Recommended focus for Phase 2: fix all three query-layer defects first (they are the ones a consumer can hit silently, with no error), then clean up the smaller correctness/documentation gaps.

---

## Findings

### 9. Redundant `ctx.Err()` guards on write paths, absent on read paths — remove rather than extend

- **File(s):** `table.go` (`Insert`, `Update`, `Delete` on both `Table` and `TableWithTx`)
- **Dimension(s):** Good Engineering Practices
- **Priority:** Low
- **Status:** Open
- **Description:** `Insert`, `Update`, and `Delete` each open with `if ctx.Err() != nil { return ..., fmt.Errorf("context error before %s: %w", ..., ctx.Err()) }`, but `QueryOne`, `QueryMany`, `QueryManyWithPagination`, and `Count` have no equivalent check. `database/sql`'s `*Context` methods (`ExecContext`, `QueryContext`, `QueryRowContext`) already check context cancellation internally and return an error derived from `ctx.Err()` — the explicit guard does not add correctness, it only changes the wrapping text of an error that would already occur. The inconsistency (present on three methods, absent on four) suggests the guard was added defensively rather than to fix an observed gap.
- **Recommended fix:** Remove the four `ctx.Err()` guards from `Insert`/`Update`/`Delete` on both `Table` and `TableWithTx`, rather than adding matching guards to the read paths — `ExecContext`/`QueryContext` already surface context cancellation correctly without them. This reduces code and cyclomatic complexity with no behavior change.

---

## Priority Table

| # | Priority | Status | Finding | File(s) |
|---|----------|--------|---------|---------|
| 9 | Low      | Open   | Redundant `ctx.Err()` guards present on writes, absent on reads | table.go |
