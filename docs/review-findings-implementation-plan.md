# Review Findings Implementation Plan

Date: 2026-03-28

## Current Conclusion

The repository is publish-ready:
- `lein test` passed: 100+ tests, 550+ assertions, 0 failures, 0 errors.
- RDF4J compliance passed: 40 of 40.
- The SPARQL `/metrics` and `/health` endpoints are present and tested.
- The `compute-commit-ops` ordering bug has been fixed and regression-tested.

## Resolved Finding

### 1. `compute-commit-ops` reorders transaction operations and can commit the wrong final state

Status: **resolved** (2026-03-28)

Files changed:
- `src/rama_sail/sail/adapter.clj` — rewrote `compute-commit-ops`
- `test/rama_sail/sail/adapter_test.clj` — added unit tests for `compute-commit-ops`
- `test/rama_sail/query/transaction_test.clj` — added post-commit persistence regression tests

Problem (was):
- `compute-commit-ops` grouped all `:add` ops, then all `:del` ops, then all `:clear-context` ops.
- `commitInternal` assigned per-op timestamps using that reordered sequence.
- A `clear(ctx)` → `add(quad)` transaction committed as `add` then `clear`, deleting the new quad.

Fix:
- Rewrote `compute-commit-ops` to emit `:clear-context` ops in-place during a left-to-right reduce.
- Ops before a clear for the same context are discarded (subsumed by the clear, and invisible within the same microbatch anyway).
- Ops after a clear survive and follow it in the output, receiving higher tx-times.
- This ensures `commitInternal` tx-time assignment reflects original transaction intent.

Additional constraint discovered during implementation:
- Within a single Rama microbatch, `:clear-context` cannot see PState writes from `:add` ops in the same batch (they process on different partitions concurrently).
- Therefore, pre-clear adds must be discarded (not flushed before the clear), since the indexer's clear handler would not see them even with correct tx-time ordering.

Tests added:
- Unit tests: 7 cases covering clear ordering, last-write-wins, multi-context isolation, double-clear
- Integration tests: `test-clear-then-add-persists`, `test-add-clear-add-persists`, `test-add-then-clear-removes`
- All 58+ tests across 6 test suites pass with 0 failures

## What Was Obsolete And Has Been Removed

The previous draft of this document contained conclusions that are no longer accurate for the current tree. Those items should not be used for release decisions:

- obsolete: "Public OSS preview: ready"
- obsolete: broad release blocker claims based on missing `/metrics` and `/health`
- obsolete: older tombstone/soft-delete findings as the primary current blocker
- obsolete: verification notes that assumed IPC-backed tests had not been rerun in an unrestricted environment

Those were superseded by the latest review:
- full `lein test` passed in an unrestricted run
- compliance is 40/40
- the commit-order correctness issue has been fixed

## Release Recommendation

Current recommendation:
- **ready to publish**

All criteria met:
- `compute-commit-ops` no longer reorders clear-sensitive transaction semantics
- regression tests have been added and pass
- full test suite remains green after the fix
