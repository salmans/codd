//! Differential testing of codd against a naive set-based reference model.
//!
//! A random byte string is decoded into a program over a small schema: inserts,
//! stored views, ad hoc evaluations and database clones ("forks"). Every
//! operation is applied both to a `codd::Database` and to a shadow model made
//! of `BTreeSet`s. Every evaluation must agree between the two.
//!
//! Only the public API is used. Closures given to codd are built from a query
//! description (`Q`) so that the same description can be interpreted by the
//! reference evaluator.

use codd::{
    expression::{
        Difference, Empty, Intersect, Join, Mono, Product, Project, Relation, Select, Singleton,
        Union, View,
    },
    Database,
};
use proptest::prelude::*;
use std::collections::BTreeSet;

type Row = Vec<u32>;
type Expr = Mono<Row>;

/// A query description that can be turned into a codd expression and
/// evaluated by the reference model.
#[derive(Clone, Debug)]
enum Q {
    Rel(usize),
    View(usize),
    Empty(usize),
    Singleton(Row),
    Select {
        e: Box<Q>,
        col: usize,
        val: u32,
    },
    Project {
        e: Box<Q>,
        cols: Vec<usize>,
    },
    Union(Box<Q>, Box<Q>),
    Intersect(Box<Q>, Box<Q>),
    Difference(Box<Q>, Box<Q>),
    Product(Box<Q>, Box<Q>),
    Join {
        l: Box<Q>,
        r: Box<Q>,
        lk: Vec<usize>,
        rk: Vec<usize>,
    },
}

/// The reference model of one database.
#[derive(Clone)]
struct Shadow {
    arities: Vec<usize>,
    rels: Vec<BTreeSet<Row>>,
    views: Vec<(View<Row, Expr>, Q, usize)>,
}

fn arity(q: &Q, s: &Shadow) -> usize {
    match q {
        Q::Rel(i) => s.arities[*i],
        Q::View(i) => s.views[*i].2,
        Q::Empty(a) => *a,
        Q::Singleton(r) => r.len(),
        Q::Select { e, .. } => arity(e, s),
        Q::Project { cols, .. } => cols.len(),
        Q::Union(l, _) | Q::Intersect(l, _) | Q::Difference(l, _) => arity(l, s),
        Q::Product(l, r) | Q::Join { l, r, .. } => arity(l, s) + arity(r, s),
    }
}

fn concat(a: &Row, b: &Row) -> Row {
    a.iter().chain(b.iter()).copied().collect()
}

fn key(t: &Row, cols: &[usize]) -> Row {
    cols.iter().map(|c| t[*c]).collect()
}

fn reference(q: &Q, s: &Shadow) -> BTreeSet<Row> {
    match q {
        Q::Rel(i) => s.rels[*i].clone(),
        Q::View(i) => reference(&s.views[*i].1, s),
        Q::Empty(_) => BTreeSet::new(),
        Q::Singleton(r) => std::iter::once(r.clone()).collect(),
        Q::Select { e, col, val } => reference(e, s)
            .into_iter()
            .filter(|t| t[*col] == *val)
            .collect(),
        Q::Project { e, cols } => reference(e, s).iter().map(|t| key(t, cols)).collect(),
        Q::Union(l, r) => &reference(l, s) | &reference(r, s),
        Q::Intersect(l, r) => &reference(l, s) & &reference(r, s),
        Q::Difference(l, r) => &reference(l, s) - &reference(r, s),
        Q::Product(l, r) => {
            let (l, r) = (reference(l, s), reference(r, s));
            l.iter()
                .flat_map(|a| r.iter().map(move |b| concat(a, b)))
                .collect()
        }
        Q::Join { l, r, lk, rk } => {
            let (l, r) = (reference(l, s), reference(r, s));
            l.iter()
                .flat_map(|a| {
                    r.iter()
                        .filter(move |b| key(a, lk) == key(b, rk))
                        .map(move |b| concat(a, b))
                })
                .collect()
        }
    }
}

fn build(q: &Q, s: &Shadow) -> Expr {
    match q {
        Q::Rel(i) => Relation::new(rel_name(*i)).into(),
        Q::View(i) => s.views[*i].0.clone().into(),
        Q::Empty(_) => Empty::new().into(),
        Q::Singleton(r) => Singleton::new(r.clone()).into(),
        Q::Select { e, col, val } => {
            let (c, v) = (*col, *val);
            Select::new(build(e, s), move |t: &Row| t[c] == v).into()
        }
        Q::Project { e, cols } => {
            let cols = cols.clone();
            Project::new(build(e, s), move |t: &Row| key(t, &cols)).into()
        }
        Q::Union(l, r) => Union::new(build(l, s), build(r, s)).into(),
        Q::Intersect(l, r) => Intersect::new(build(l, s), build(r, s)).into(),
        Q::Difference(l, r) => Difference::new(build(l, s), build(r, s)).into(),
        Q::Product(l, r) => {
            Product::new(build(l, s), build(r, s), |a: &Row, b: &Row| concat(a, b)).into()
        }
        Q::Join { l, r, lk, rk } => {
            let (lk, rk) = (lk.clone(), rk.clone());
            Join::new(
                build(l, s),
                build(r, s),
                move |t: &Row| key(t, &lk),
                move |t: &Row| key(t, &rk),
                |_: &Row, a: &Row, b: &Row| concat(a, b),
            )
            .into()
        }
    }
}

fn rel_name(i: usize) -> String {
    format!("r{}", i)
}

fn has_difference(q: &Q) -> bool {
    match q {
        Q::Rel(_) | Q::View(_) | Q::Empty(_) | Q::Singleton(_) => false,
        Q::Select { e, .. } | Q::Project { e, .. } => has_difference(e),
        Q::Difference(..) => true,
        Q::Union(l, r) | Q::Intersect(l, r) | Q::Product(l, r) | Q::Join { l, r, .. } => {
            has_difference(l) || has_difference(r)
        }
    }
}

/// A cursor over the random bytes that drive the program.
struct Bytes<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Bytes<'a> {
    fn next(&mut self) -> u8 {
        let b = self.data.get(self.pos).copied().unwrap_or(0);
        self.pos += 1;
        b
    }

    fn pick(&mut self, n: usize) -> usize {
        self.next() as usize % n.max(1)
    }

    fn done(&self) -> bool {
        self.pos >= self.data.len()
    }
}

/// Decodes a query of the given maximal depth. When `allow_diff` is false the
/// query is suitable for a stored view.
fn gen_query(b: &mut Bytes, s: &Shadow, domain: u32, depth: usize, allow_diff: bool) -> Q {
    let leaf = |b: &mut Bytes, s: &Shadow| -> Q {
        match b.pick(8) {
            0 if !s.views.is_empty() => Q::View(b.pick(s.views.len())),
            1 => {
                let a = 1 + b.pick(3);
                Q::Singleton((0..a).map(|_| b.pick(domain as usize) as u32).collect())
            }
            2 => Q::Empty(1 + b.pick(3)),
            _ => Q::Rel(b.pick(s.arities.len())),
        }
    };
    if depth == 0 {
        return leaf(b, s);
    }
    match b.pick(9) {
        0 | 1 => leaf(b, s),
        2 => {
            let e = gen_query(b, s, domain, depth - 1, allow_diff);
            let a = arity(&e, s);
            let col = b.pick(a);
            let val = b.pick(domain as usize) as u32;
            Q::Select {
                e: Box::new(e),
                col,
                val,
            }
        }
        3 => {
            let e = gen_query(b, s, domain, depth - 1, allow_diff);
            let a = arity(&e, s);
            let n = 1 + b.pick(3);
            let cols = (0..n).map(|_| b.pick(a)).collect();
            Q::Project {
                e: Box::new(e),
                cols,
            }
        }
        4..=6 => {
            let l = gen_query(b, s, domain, depth - 1, allow_diff);
            let r = gen_query(b, s, domain, depth - 1, allow_diff);
            let r = fit_arity(b, r, arity(&l, s), s);
            let (l, r) = (Box::new(l), Box::new(r));
            match b.pick(if allow_diff { 3 } else { 2 }) {
                0 => Q::Union(l, r),
                1 => Q::Intersect(l, r),
                _ => Q::Difference(l, r),
            }
        }
        7 => {
            let l = gen_query(b, s, domain, depth - 1, allow_diff);
            let r = gen_query(b, s, domain, depth - 1, allow_diff);
            Q::Product(Box::new(l), Box::new(r))
        }
        _ => {
            let l = gen_query(b, s, domain, depth - 1, allow_diff);
            let r = gen_query(b, s, domain, depth - 1, allow_diff);
            let (la, ra) = (arity(&l, s), arity(&r, s));
            let k = 1 + b.pick(la.min(ra));
            let lk = (0..k).map(|_| b.pick(la)).collect();
            let rk = (0..k).map(|_| b.pick(ra)).collect();
            Q::Join {
                l: Box::new(l),
                r: Box::new(r),
                lk,
                rk,
            }
        }
    }
}

/// Projects `q` so that it has arity `a` (columns may repeat).
fn fit_arity(b: &mut Bytes, q: Q, a: usize, s: &Shadow) -> Q {
    let qa = arity(&q, s);
    if qa == a {
        q
    } else {
        let cols = (0..a).map(|_| b.pick(qa)).collect();
        Q::Project {
            e: Box::new(q),
            cols,
        }
    }
}

fn gen_rows(b: &mut Bytes, arity: usize, domain: u32) -> Vec<Row> {
    let n = b.pick(4);
    (0..n)
        .map(|_| (0..arity).map(|_| b.pick(domain as usize) as u32).collect())
        .collect()
}

fn check(db: &Database, s: &Shadow, q: &Q, trace: &[String]) -> usize {
    let got = db.evaluate(&build(q, s)).unwrap_or_else(|e| {
        panic!(
            "evaluate failed: {:?}\nquery: {:?}\ntrace: {:#?}",
            e, q, trace
        )
    });
    let want: Vec<Row> = reference(q, s).into_iter().collect();
    let got = got.into_tuples();
    assert_eq!(
        got, want,
        "codd disagrees with the reference\nquery: {:?}\ntrace: {:#?}",
        q, trace
    );
    got.len()
}

/// Counts of what a program exercised, used to make sure the generator is
/// not degenerate.
#[derive(Default, Debug, Clone, Copy)]
struct Stats {
    inserts: usize,
    views: usize,
    checks: usize,
    forks: usize,
    max_rows: usize,
    max_result: usize,
}

fn run(data: &[u8]) -> Stats {
    let mut stats = Stats::default();
    let mut b = Bytes { data, pos: 0 };
    let n_rels = 1 + b.pick(3);
    let arities: Vec<usize> = (0..n_rels).map(|_| 1 + b.pick(3)).collect();
    let domain = 1 + b.pick(4) as u32;

    let mut db = Database::new();
    let relations: Vec<Relation<Row>> = (0..n_rels)
        .map(|i| db.add_relation::<Row>(&rel_name(i)).unwrap())
        .collect();
    let shadow = Shadow {
        arities,
        rels: vec![BTreeSet::new(); n_rels],
        views: Vec::new(),
    };
    let mut states = vec![(db, shadow)];
    let mut trace = Vec::new();

    while !b.done() {
        let k = b.pick(states.len());
        match b.pick(8) {
            0..=2 => {
                let i = b.pick(n_rels);
                let (db, s) = &mut states[k];
                let rows = gen_rows(&mut b, s.arities[i], domain);
                trace.push(format!("db{}: insert {:?} into r{}", k, rows, i));
                db.insert(&relations[i], rows.clone().into()).unwrap();
                s.rels[i].extend(rows);
                stats.inserts += 1;
                stats.max_rows = stats.max_rows.max(s.rels[i].len());
            }
            3 => {
                let (db, s) = &mut states[k];
                let q = gen_query(&mut b, s, domain, 3, false);
                assert!(!has_difference(&q));
                trace.push(format!("db{}: store view {:?}", k, q));
                let a = arity(&q, s);
                let view = db
                    .store_view(build(&q, s))
                    .unwrap_or_else(|e| panic!("store_view failed: {:?}\ntrace: {:#?}", e, trace));
                s.views.push((view, q, a));
                stats.views += 1;
            }
            4 | 5 => {
                let (db, s) = &states[k];
                let q = gen_query(&mut b, s, domain, 3, true);
                trace.push(format!("db{}: check {:?}", k, q));
                stats.max_result = stats.max_result.max(check(db, s, &q, &trace));
                stats.checks += 1;
            }
            6 => {
                if states.len() < 3 {
                    trace.push(format!("fork db{} -> db{}", k, states.len()));
                    let (db, s) = &states[k];
                    let cloned = (db.clone(), s.clone());
                    states.push(cloned);
                    stats.forks += 1;
                }
            }
            _ => {
                let (db, s) = &states[k];
                trace.push(format!("db{}: check all {} views", k, s.views.len()));
                for i in 0..s.views.len() {
                    check(db, s, &Q::View(i), &trace);
                }
            }
        }
    }
    // Final consistency check of everything in every state.
    for (k, (db, s)) in states.iter().enumerate() {
        trace.push(format!("db{}: final check", k));
        for i in 0..n_rels {
            check(db, s, &Q::Rel(i), &trace);
        }
        for i in 0..s.views.len() {
            check(db, s, &Q::View(i), &trace);
        }
    }
    stats
}

/// A fixed-seed sweep that reports what the generator exercises.
#[test]
fn generator_is_not_degenerate() {
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut total = Stats::default();
    for _ in 0..500 {
        let data: Vec<u8> = (0..300)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                (seed >> 24) as u8
            })
            .collect();
        let st = run(&data);
        total.inserts += st.inserts;
        total.views += st.views;
        total.checks += st.checks;
        total.forks += st.forks;
        total.max_rows = total.max_rows.max(st.max_rows);
        total.max_result = total.max_result.max(st.max_result);
    }
    eprintln!("harness totals over 500 programs: {:?}", total);
    assert!(total.views > 500 && total.checks > 1000 && total.forks > 100);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2000, max_shrink_iters: 20000, .. ProptestConfig::default() })]

    #[test]
    fn codd_agrees_with_reference(data in prop::collection::vec(any::<u8>(), 0..300)) {
        run(&data);
    }
}
