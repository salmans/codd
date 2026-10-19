//! Regression tests for bugs found during the correctness review.

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

/// A clone gets its own view references: a view stored in one clone after the fork is
/// unknown to the other instead of resolving to the other's same-numbered view, while a
/// view stored before the fork stays valid in both.
#[test]
fn view_handles_identify_the_database_that_stored_them() {
    let mut original = Database::new();
    let r = original.add_relation::<u32>("r").unwrap();
    original.insert(&r, vec![1, 2, 3, 4].into()).unwrap();
    let all = original.store_view(r.clone()).unwrap();

    let mut clone = original.clone();
    let even = clone
        .store_view(Select::new(r.clone(), |t: &u32| t.is_multiple_of(2)))
        .unwrap();
    let odd = original
        .store_view(Select::new(r.clone(), |t: &u32| t % 2 == 1))
        .unwrap();

    assert_eq!(vec![2, 4], clone.evaluate(&even).unwrap().into_tuples());
    assert_eq!(vec![1, 3], original.evaluate(&odd).unwrap().into_tuples());
    assert!(matches!(
        original.evaluate(&even),
        Err(Error::InstanceNotFound { .. })
    ));
    assert!(matches!(
        clone.evaluate(&odd),
        Err(Error::InstanceNotFound { .. })
    ));
    assert_eq!(
        vec![1, 2, 3, 4],
        original.evaluate(&all).unwrap().into_tuples()
    );
    assert_eq!(
        vec![1, 2, 3, 4],
        clone.evaluate(&all).unwrap().into_tuples()
    );
}

/// Expression closures are `Fn` and shared by clones; with only immutable captures a
/// clone and its original evaluate the same stored view independently.
#[test]
fn clones_evaluate_shared_closures_independently() {
    let threshold: u32 = 2;
    let mut original = Database::new();
    let r = original.add_relation::<Row>("r").unwrap();
    let above: Mono<Row> =
        Select::new(Mono::from(r.clone()), move |t: &Row| t[0] > threshold).into();
    let view = original.store_view(above).unwrap();
    original.insert(&r, vec![vec![1], vec![3]].into()).unwrap();

    let clone = original.clone();
    original.insert(&r, vec![vec![4]].into()).unwrap();
    clone.insert(&r, vec![vec![5]].into()).unwrap();
    clone.retract(&r, vec![vec![3]].into()).unwrap();

    assert_eq!(
        vec![vec![3], vec![4]],
        original.evaluate(&view).unwrap().into_tuples()
    );
    assert_eq!(vec![vec![5]], clone.evaluate(&view).unwrap().into_tuples());
}
