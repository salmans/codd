# Correctness review log

Running log of correctness findings in `codd`, how each was detected, and its status.
The primary detection tool is `tests/differential.rs`: random programs (inserts, stored
views, ad hoc queries, database clones) are executed against both `codd` and a naive
`BTreeSet`-based reference model, and every evaluation must agree. Targeted regressions
live in `tests/regressions.rs`.

## Findings

| # | Finding | Found by | Status |
|---|---------|----------|--------|
| 1 | A failing `store_view` (missing dependency, wrong tuple type) registered the new `ViewRef` as a dependent of its relations before initialization. The next stabilization of such a relation hit `self.views.get(r).unwrap()` and panicked. | code reading | fixed: dependencies are checked and the view is initialized before anything is registered |
| 2 | The `stabilizing` re-entrancy flag was never cleared when a stabilization returned early with `?`, so that instance silently skipped all future stabilization. | code reading | fixed: RAII `StabilizingGuard` |
| 3 | `validate_view_expression` only rejected `Difference`; `Full` passed validation and failed later from inside initialization. | code reading | fixed: `Full` is rejected at `store_view` with `UnsupportedExpression` |
| 4 | A failed `Any` downcast (relation or view handle with the wrong tuple type) was reported as `InstanceNotFound`. | code reading | fixed: new `Error::TypeMismatch` |
| 5 | `diff_helper` rebound its loop variable instead of advancing the right-hand slices, so the gallop restarted from the beginning for every left tuple. Results were correct; cost was quadratic. | code reading | fixed |
| 6 | `IncrementalCollector::collect_difference` is not a delta: it recomputes `stable(L) − stable(R)` and ignores growth on the right. It is only correct because views over `Difference` are rejected and `Evaluator` empties every `recent` before evaluating. | code reading | fixed: `Difference` is an antijoin with an exact delta on both sides and can be stored as a view |
| 7 | Join, product, intersect and difference re-key and re-sort every stable batch of both children on every incremental step, so a delta step costs O(stable) rather than O(delta). `benches/incremental.rs`: one insert into a join view costs ~0.3 ms at 1k–10k stable tuples and ~1.2 ms at 100k. | code reading | open; addressed by arrangements (plan A4) |
| 8 | `Database::clone` shares the `Rc<RefCell<dyn FnMut>>` closures of expressions between clones. Stateless closures (the only kind `codd` has a reason to support) are unaffected. | code reading | open; closures should become `Fn` |
| 9 | First version of the weighted rewrite consolidated pending changes before applying set semantics, so `insert` of a present tuple followed by `retract` summed to no change and the tuple stayed. Pending changes are now folded in order against the running count of each tuple. | differential harness (shrunk to a 23-step program) | fixed before merge |

## Harness coverage

`generator_is_not_degenerate` reports, for 500 fixed-seed programs: thousands of inserts and
retractions, ~1.8k stored views (depth ≤ 3, views over views, self-joins, `Difference` and
`Distinct` inside views), ~3.5k ad hoc checks, ~900 clones mutated independently afterwards,
result sizes up to a few hundred rows. A 30 000-case `proptest` sweep passes.

## Clones

Batches are immutable and reference counted, so `Database::clone` copies only the spine of each
instance: ~0.5 µs regardless of size (1k, 10k and 100k tuples measured). A clone and its original
diverge independently, including by retraction; the differential harness exercises this.
