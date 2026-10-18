//! Regression tests for bugs found during the correctness review (see REVIEW.md).

use codd::{
    expression::{Full, Join, Mono, Relation, Select, Union},
    Database, Error,
};

type Row = Vec<u32>;

/// A `store_view` that fails must not leave the database in a state where a
/// later insert and evaluation panic.
#[test]
fn failed_store_view_leaves_database_usable() {
    let mut db = Database::new();
    let r = db.add_relation::<Row>("r").unwrap();

    // `missing` does not exist, so initializing the view fails.
    let bad: Mono<Row> = Join::new(
        Mono::from(r.clone()),
        Mono::from(Relation::<Row>::new("missing")),
        |t: &Row| t.clone(),
        |t: &Row| t.clone(),
        |_: &Row, a: &Row, b: &Row| a.iter().chain(b.iter()).copied().collect(),
    )
    .into();
    assert!(matches!(
        db.store_view(bad),
        Err(Error::InstanceNotFound { .. })
    ));

    // The relation must still accept inserts and evaluate without panicking.
    db.insert(&r, vec![vec![1], vec![2]].into()).unwrap();
    assert_eq!(
        vec![vec![1], vec![2]],
        db.evaluate(&r).unwrap().into_tuples()
    );

    // And storing a good view afterwards must work and see the data.
    let good: Mono<Row> = Select::new(Mono::from(r.clone()), |t: &Row| t[0] > 1).into();
    let view = db.store_view(good).unwrap();
    assert_eq!(vec![vec![2]], db.evaluate(&view).unwrap().into_tuples());
    db.insert(&r, vec![vec![3]].into()).unwrap();
    assert_eq!(
        vec![vec![2], vec![3]],
        db.evaluate(&view).unwrap().into_tuples()
    );
}

/// A view whose expression has the wrong tuple type for a relation must be
/// rejected with a type error, and must not corrupt the relation's bookkeeping.
#[test]
fn store_view_with_wrong_tuple_type_is_rejected() {
    let mut db = Database::new();
    let r = db.add_relation::<Row>("r").unwrap();

    let wrong: Mono<u32> =
        Select::new(Mono::from(Relation::<u32>::new("r")), |_: &u32| true).into();
    let err = db.store_view(wrong).unwrap_err();
    assert!(
        matches!(err, Error::TypeMismatch { .. }),
        "expected a type mismatch error, got {:?}",
        err
    );

    db.insert(&r, vec![vec![1]].into()).unwrap();
    assert_eq!(vec![vec![1]], db.evaluate(&r).unwrap().into_tuples());
}

/// `Full` cannot be materialized; `store_view` must say so up front instead of
/// failing from inside initialization (or, nested, from inside a later stabilize).
#[test]
fn store_view_rejects_full_anywhere_in_the_expression() {
    let mut db = Database::new();
    let r = db.add_relation::<Row>("r").unwrap();

    let top: Mono<Row> = Full::new().into();
    assert!(matches!(
        db.store_view(top),
        Err(Error::UnsupportedExpression { .. })
    ));

    let nested: Mono<Row> =
        Union::new(Mono::from(r.clone()), Mono::from(Full::<Row>::new())).into();
    assert!(matches!(
        db.store_view(nested),
        Err(Error::UnsupportedExpression { .. })
    ));

    db.insert(&r, vec![vec![1]].into()).unwrap();
    assert_eq!(vec![vec![1]], db.evaluate(&r).unwrap().into_tuples());
}

/// Evaluating a relation handle of the wrong type reports a type mismatch, not
/// a missing instance.
#[test]
fn evaluate_with_wrong_tuple_type_reports_type_mismatch() {
    let mut db = Database::new();
    db.add_relation::<Row>("r").unwrap();
    let err = db.evaluate(&Relation::<u32>::new("r")).unwrap_err();
    assert!(matches!(err, Error::TypeMismatch { .. }), "got {:?}", err);
}

/// Two clones of a database must not share mutable state through views:
/// a view stored in one clone must be independent of the other.
#[test]
fn clones_are_independent_including_views_stored_after_cloning() {
    let mut db = Database::new();
    let r = db.add_relation::<Row>("r").unwrap();
    db.insert(&r, vec![vec![1]].into()).unwrap();
    let v: Mono<Row> = Select::new(Mono::from(r.clone()), |t: &Row| t[0] % 2 == 1).into();
    let odd = db.store_view(v).unwrap();

    let mut other = db.clone();

    db.insert(&r, vec![vec![3]].into()).unwrap();
    other.insert(&r, vec![vec![5]].into()).unwrap();

    assert_eq!(
        vec![vec![1], vec![3]],
        db.evaluate(&odd).unwrap().into_tuples()
    );
    assert_eq!(
        vec![vec![1], vec![5]],
        other.evaluate(&odd).unwrap().into_tuples()
    );

    // A view stored only in `other` after the clone:
    let w: Mono<Row> = Select::new(Mono::from(r.clone()), |t: &Row| t[0] > 1).into();
    let big = other.store_view(w).unwrap();
    other.insert(&r, vec![vec![7]].into()).unwrap();
    assert_eq!(
        vec![vec![5], vec![7]],
        other.evaluate(&big).unwrap().into_tuples()
    );
    // `db` never stored that view, so its handle is unknown there.
    assert!(db.evaluate(&big).is_err());
}
