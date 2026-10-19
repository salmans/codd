# codd: improvement points

Design-level items suggested during the 2026-10 review of the weighted-batch rewrite. Correctness findings and
their status live in `core/REVIEW.md`; this file is the forward-looking list.

## Engine

- **Keys as data (arrangements).** `Join`, `Product` and `Project` take closures, so the engine cannot see which
  columns a join uses. Declare joins on column positions and keep closures only for the output shape, then build
  an index per (relation, key columns) once and share it across every view that joins on it. This is the
  prerequisite for sharing work between views, for worst-case-optimal joins, and for fixing REVIEW #7/#11/#13.
- **Tick-based updates.** Replace the "at most one non-empty `recent` at a time" stabilization protocol with a
  clock: apply all pending changes of all inputs as one tick and let every operator compute its delta from all
  operand deltas with the full bilinear formula. Easier to reason about, lets a client apply a batch of inserts
  and retractions across relations as one step, and is the shape parallel evaluation needs.
- **Materialized operands for non-linear operators.** `Difference` and `Distinct` look up counts in their
  operands; an operand that is a nested join is recomputed in full per delta. Materialize such operands
  automatically (or reject them at `store_view`).
- **Cursors instead of copies.** `evaluate` consolidates every stable batch into a fresh `Tuples`. Offer a cursor
  or iterator over the stable spine (k-way merge, positive weights only) so "is this view empty / first tuple"
  is O(1) and large results are not copied.
- **Worst-case-optimal join** (leapfrog / generic join) over per-column-order indexes, once keys are data;
  benchmark on cyclic queries before making it the default anywhere.
- **`Send` + `Sync`.** `Rc` → `Arc` for batches and expressions, closures `Fn + Send + Sync`, so a database can
  move between threads (branch-parallel search in clients).

## API shape

- `View<T, E>` carries the expression type only to downcast; a type-erased `View<T>` would remove the `Mono`
  requirement from client code.
- `Mono<T>` only supports same-type projections; with keys as data most of the reason for it disappears.
- Expose derivation counts (`evaluate_weighted`) for clients that need multiplicities.

## Testing

- Harness compares results as sets; also compare derivation counts against a weighted reference.
- Generate type-changing projections, non-equality selections, larger domains, and error paths.
- Property: a tick-based engine must agree with the current one on every program (keep the old protocol as the
  oracle during the transition).

## Positioning

- Pitch: "datafrog with views, retractions and forks". Add one worked example that is not Razor (a speculative
  search or an undo/what-if tool) before publishing 0.2.
