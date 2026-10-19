//! Cost of one incremental step and of cloning, as a function of the stable size.
use codd::{
    expression::{Join, Relation, View},
    Database,
};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};

type Row = (u32, u32);
type JoinView = View<Row, Join<u32, Row, Row, Relation<Row>, Relation<Row>, Row>>;

/// A database with a stored join view, the relation the benchmark inserts into, and the view.
struct Fixture {
    db: Database,
    r: Relation<Row>,
    view: JoinView,
}

/// A database with `n` tuples in two relations and a stored join view over them.
fn setup(n: u32) -> Fixture {
    let mut db = Database::new();
    let r = db.add_relation::<Row>("r").unwrap();
    let s = db.add_relation::<Row>("s").unwrap();
    let join = Join::new(
        r.clone(),
        s.clone(),
        |t: &Row| t.0,
        |t: &Row| t.0,
        |_: &u32, l: &Row, r: &Row| (l.1, r.1),
    );
    let view = db.store_view(join).unwrap();
    db.insert(&r, (0..n).map(|i| (i % 1024, i)).into()).unwrap();
    db.insert(&s, (0..n).map(|i| (i % 1024, i + n)).into())
        .unwrap();
    db.evaluate(&view).unwrap();
    Fixture { db, r, view }
}

fn incremental_step(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_view_insert_one");
    for n in [1_000u32, 10_000, 100_000] {
        let Fixture { db, r, view } = setup(n);
        let mut next = n;
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                next += 1;
                db.insert(&r, vec![(next % 1024, next)].into()).unwrap();
                db.evaluate(&view).unwrap();
            })
        });
    }
    group.finish();
}

fn clone_database(c: &mut Criterion) {
    let mut group = c.benchmark_group("clone");
    for n in [1_000u32, 10_000, 100_000] {
        let Fixture { db, .. } = setup(n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| db.clone())
        });
    }
    group.finish();
}

criterion_group!(benches, incremental_step, clone_database);
criterion_main!(benches);
