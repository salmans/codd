# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`codd` is a minimal in-memory relational database with incrementally maintained views, written to back
`rusty-razor`'s chase but kept a pure database: no chase or logic concepts belong here. The crate is the
workspace member `core/`. The design and its history are in `README.md` and `core/REVIEW.md` (the correctness
findings log; add to it when a bug is found or an issue is deliberately deferred).

## Commands

```sh
cargo test                                   # unit, doc, regression, and differential tests
cargo test --all-features                    # also the `unstable` query macros
PROPTEST_CASES=30000 cargo test --test differential codd_agrees   # long randomized sweep
cargo clippy --all-targets --all-features -- -D warnings           # CI runs this; keep it clean
cargo fmt --all -- --check
cargo bench --bench incremental              # per-step and clone cost vs stable size
```

`core/tests/differential.rs` is the main safety net: random programs (inserts, retractions, stored views, ad hoc
queries, clones mutated independently) run against codd and a naive `BTreeSet` reference and must agree.
Extend its generator whenever an operator or an operation is added; `generator_is_not_degenerate` reports what it
exercises. `core/tests/differential.proptest-regressions` is proptest's persisted seed file; commit it.

## Architecture

- `database/instance.rs`: storage. An `Instance<T>` is a spine of immutable, `Rc`-shared `Batch<T>` (sorted,
  consolidated weighted tuples), plus `recent` (the latest change) and `to_add` (pending changes). `changed()`
  folds pending changes in order against each tuple's running count (relations clamp to set semantics, views
  keep derivation counts), exposes the result as `recent`, and merges the previous `recent` into the spine with
  datafrog's geometric policy. `Rc::try_unwrap` makes merging in place when a batch is unshared and copy when a
  clone still holds it, which is what makes `Database::clone` cheap and clones independent.
- `database/evaluate.rs`: one `IncrementalCollector` implementing both `RecentCollector` (the delta of an
  expression from its operands' `recent`) and `StableCollector` (its full content from their `stable`). Linear
  operators pass deltas through, bilinear ones use `ΔL⋈R + L⋈ΔR + ΔL⋈ΔR`, and the non-linear ones (`Difference`
  as whole-tuple antijoin, `Distinct`) recompute the affected tuples from current counts. The invariant everything
  relies on: outside a stabilization loop every `recent` is empty, and inside one at most one instance's `recent`
  is non-empty, so no pair is counted twice.
- `database.rs`: `Database` holds relations and views with their dependency graph; `stabilize_relation` /
  `stabilize_view` drive the `while changed() { dependents.stabilize() }` protocol; `store_view` validates
  (`Full` is rejected) and initializes before registering dependencies so a failure leaves the database untouched;
  `evaluate` stabilizes, then returns the positive-weight tuples of the stable batches.
- `expression/`: the operator types (`Relation`, `Select`, `Project`, `Union`, `Intersect`, `Difference`,
  `Distinct`, `Product`, `Join`, `View`, `Singleton`, `Empty`, `Full`), the `Visitor`, the `Builder`, and `Mono`,
  the single-tuple-type recursive expression used by clients. Adding an operator means: a type here, a `Mono`
  variant, `Visitor`/walk hooks, both collector traits in `expression_ext.rs` and `evaluate.rs`, the harness.

## Code shape

Design the type before the code. Every change, review, and agent brief is held to these:

- Invariants live in types, not call order. A function that is valid only after another ran is a bug in the
  shape: return the value, or make the phase a type (the normal-form chain `NNF → PNF → SNF → CNF → GNF` is the
  model: each phase is its own type and the next transform only accepts it).
- A parameter whose valid values are a strict subset of its type's values is a missing type. If the caller
  filters before calling (a `Var` that must be a universal, a `Symbol` that must be a function, a `Vec` that must
  be sorted, non-empty or pairwise distinct, an `isize` that must be non-zero), the check lives in whoever calls,
  so it lives nowhere: make the newtype or enum, construct it only where the fact is established, and take it.
- A cluster of functions over one field set is a type with methods. Phased construction is a builder. A role is
  an enum, never an `Option`/`bool` pair; shared behaviour across roles is a trait. A bare boolean argument that
  reads `f(x, true)` at the call site is boolean blindness: use an enum or two constructors.
- Methods and traits over free functions. A function whose first argument is `&Self` is a method; a family of
  functions threading the same context wants a struct or newtype hosting them; two or more cohesive functions
  over a foreign type want an extension trait. Free functions only for genuine cross-type orchestration.
- Names carry the concept, the receiver and module carry the rest: short verb or noun, no `get_`/`lookup_`/
  `register_` prefixes, no `_for_`/`_by_`/`_from_` chains. One concept has one name across crates.
- A crate's surface is a small named set in `lib.rs`. Modules are private, helpers are `pub(crate)`, and no
  crate re-exports another crate's type.
- A rule has one authority. Before adding a type, table, or walk, find the existing one that decides the same
  thing and extend it; a new struct threaded as a parameter through several functions is the sign of a parallel
  mechanism. Reuse before you add, adjust before you fork, and migrate the old callers when you generalize.
- Problems over recursive data (formulae, terms, expressions, batches of views) are solved recursively, not by
  enumerating the cases you remembered. Before writing a `match`, ask whether the property belongs to the shape
  of the node or to every node in the tree; if the latter, recurse, and make every walker learn a new arm.
- A function reads like a paragraph; if it needs a comment to explain its steps, split it. Comments explain a
  non-obvious *why* (a hidden constraint, an invariant, a workaround), never *what*. Doc comments stay on public
  items because these are library crates with doctests, and they state the contract, not the implementation.
- No dead, experimental, or temporary code: no unused items, no scaffolding for later, no placeholder
  implementations. If a non-test build or a usage scan shows it unused, delete it.
- Tests are code: a fixture is a builder, a value helper is a noun, a test name is a short claim. Pin a fixed bug
  with a test that exercises the nested or combined case, not only the surface one.
- Every review includes a shape pass against this list, not only a bug pass, and asks explicitly whether anything
  new duplicates an existing authority.
