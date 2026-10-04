# Architecture Review: `github.com/dioad/nosqlite`

_Reviewed: 2026-10-03 — branch `main` (f5b7682)_

---

## Executive Summary

`nosqlite` is a small, focused library (5 source files, ~1,800 lines) with a clean public API and generally idiomatic Go. The codebase builds and tests cleanly, and `store.go`'s PRAGMA/locking choices show real engineering care (the `fixedDSNParams` and `WithSynchronous` doc comments explain *why*, not just *what*). The most critical theme is that the query layer — the part of the API every caller touches — has three independent, verified correctness defects: field names are interpolated into SQL unsanitized despite a `#nosec` comment that claims otherwise, equality/comparison clauses silently corrupt any numeric type other than literal `int`/`float64` (confirmed: `Equal[int64]` returns zero rows for an existing match), and paginated queries have no `ORDER BY`, so the stable page ordering the test suite assumes is not actually guaranteed by SQLite. Recommended focus for Phase 2: fix all three query-layer defects first (they are the ones a consumer can hit silently, with no error), then clean up the smaller correctness/documentation gaps.

---

## Findings

### 7. Existing tests use bare `t.Fatal`/`t.Errorf` throughout, contradicting the project's own testify mandate

- **File(s):** `store_test.go`, `table_test.go`, `clause_test.go`, `pagination_test.go`, `transaction_test.go`, `combined_test.go`
- **Dimension(s):** Good Engineering Practices
- **Priority:** Medium
- **Status:** Open
- **Description:** `.claude/rules/go-testing.md` is explicit: "Use `github.com/stretchr/testify` for all test assertions. Do not use bare `t.Error`, `t.Fatal`, or manual comparisons when testify covers the case." Every test in the repo uses manual `if err != nil { t.Fatal(...) }` / `if got != want { t.Errorf(...) }` comparisons instead. This is a repo-wide convention mismatch, not an isolated lapse — it is the only pattern used anywhere in the test suite.
- **Recommended fix:** This is a larger change than the rest of this review combined (five files, no `testify` dependency currently in `go.mod`) and adds a new dependency — flag it to the user for sign-off before converting existing tests wholesale. A reasonable split: add `testify` and require new tests to use `assert`/`require` going forward, then convert the existing suite in a dedicated, separately-scoped pass (or batch of commits, one file each) rather than folding it into this review's other fixes.

### 8. `Table.Update` discards `RowsAffected`, unlike `Table.Delete` — but fixing it is a breaking API change on a released module

- **File(s):** `table.go` (`Table.Update:473-506`, `TableWithTx.Update:138-170`)
- **Dimension(s):** Good Engineering Practices
- **Priority:** Low
- **Status:** Open
- **Description:** `Delete` returns `(int64, error)` so callers can tell whether anything matched. `Update` computes `rowsAffected` internally (to decide whether to log/return early) and then discards it, returning only `error` — a caller cannot distinguish "updated one row" from "clause matched nothing." This is an existing, deliberate API asymmetry: `QueryOne`'s own `//nolint:nilnil` comment already documents that this package avoids breaking its public signatures for exactly this kind of ergonomics improvement ("a sentinel error would be a breaking API change"). `go.mod`'s `release.yml` tags real versions, so this module has external consumers pinned to its current signature.
- **Recommended fix:** Do not change `Update`'s signature in place. Either add an additive `UpdateWithCount(ctx, clause, newVal) (int64, error)` alongside the existing `Update`, or defer the signature change to a deliberate major version bump (user-triggered per `cross-repo-workflow.md`'s publishing rule, which applies equally to this module's own releases).

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
| 7 | Medium   | Open   | Tests don't use testify, contradicting project's own rule | *_test.go (all) |
| 8 | Low      | Open   | `Update` discards `RowsAffected`, unlike `Delete` (breaking change if fixed) | table.go |
| 9 | Low      | Open   | Redundant `ctx.Err()` guards present on writes, absent on reads | table.go |
