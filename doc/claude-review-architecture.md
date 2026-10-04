# Architecture Review: `github.com/dioad/nosqlite`

_Reviewed: 2026-10-03 — branch `main` (f5b7682)_

---

## Executive Summary

`nosqlite` is a small, focused library (5 source files, ~1,800 lines) with a clean public API and generally idiomatic Go. The codebase builds and tests cleanly, and `store.go`'s PRAGMA/locking choices show real engineering care (the `fixedDSNParams` and `WithSynchronous` doc comments explain *why*, not just *what*). The most critical theme was that the query layer — the part of the API every caller touches — had three independent, verified correctness defects: field names were interpolated into SQL unsanitized despite a `#nosec` comment that claimed otherwise, equality/comparison clauses silently corrupted any numeric type other than literal `int`/`float64` (confirmed: `Equal[int64]` returned zero rows for an existing match), and paginated queries had no `ORDER BY`, so the stable page ordering the test suite assumed was not actually guaranteed by SQLite.

**All 9 findings from this review have been resolved** — see `claude-review-architecture-resolved.md` for the full list, outcomes, and commit SHAs.

---

## Findings

None open.

---

## Priority Table

| # | Priority | Status | Finding | File(s) |
|---|----------|--------|---------|---------|

All findings resolved; see `claude-review-architecture-resolved.md`.
