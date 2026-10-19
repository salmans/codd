//! Incremental evaluation of expressions over weighted batches.
//!
//! Every operator is evaluated in two forms: `collect_stable` computes the full (weighted)
//! content of an expression from the stable batches of the instances it depends on, and
//! `collect_recent` computes the *change* of the expression induced by the recent change of
//! those instances. Linear operators pass changes through; bilinear operators use
//! `ΔL⋈R + L⋈ΔR + ΔL⋈ΔR`; the non-linear operators (`Difference` as an antijoin and
//! `Distinct`) recompute the affected tuples from the stable counts of their operands.
use super::{
    expression_ext::{ExpressionExt, RecentCollector, StableCollector},
    helpers::{counts_in, intersect_helper, join_helper, merge_keys, product_helper},
    instance::{consolidate, Batch},
    Database,
};
use crate::{expression::*, Error, Tuple};
use std::rc::Rc;

/// Implements [`RecentCollector`] and [`StableCollector`] to incrementally
/// collect recent and stable tuples of instances of a database for expressions.
#[derive(Clone)]
pub(super) struct IncrementalCollector<'d> {
    /// Is the database in which the visited expression is evaluated.
    database: &'d Database,
}

impl<'d> IncrementalCollector<'d> {
    /// Creates a new collector for incremental evaluation.
    pub fn new(database: &'d Database) -> Self {
        Self { database }
    }
}

fn unsupported(name: &str) -> Error {
    Error::UnsupportedExpression {
        name: name.to_string(),
        operation: "Evaluate".to_string(),
    }
}

/// Keys a batch for joining: returns `(key, tuple, weight)` sorted by key.
fn keyed<'a, K: Tuple, T: Tuple>(
    batch: &'a Batch<T>,
    key: &dyn Fn(&T) -> K,
) -> Vec<(K, &'a T, isize)> {
    let mut result: Vec<(K, &T, isize)> =
        batch.items().iter().map(|(t, w)| (key(t), t, *w)).collect();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result
}

impl<'d> RecentCollector for IncrementalCollector<'d> {
    fn collect_full<T>(&self, _: &Full<T>) -> Result<Batch<T>, Error>
    where
        T: Tuple,
    {
        Err(unsupported("Full"))
    }

    fn collect_empty<T>(&self, _: &Empty<T>) -> Result<Batch<T>, Error>
    where
        T: Tuple,
    {
        Ok(Batch::empty())
    }

    fn collect_singleton<T>(&self, _: &Singleton<T>) -> Result<Batch<T>, Error>
    where
        T: Tuple,
    {
        Ok(Batch::empty())
    }

    fn collect_relation<T>(&self, relation: &Relation<T>) -> Result<Batch<T>, Error>
    where
        T: Tuple + 'static,
    {
        let table = self.database.relation_instance(relation)?;
        let recent = table.recent();
        Ok((**recent).clone())
    }

    fn collect_select<T, E>(&self, select: &Select<T, E>) -> Result<Batch<T>, Error>
    where
        T: Tuple,
        E: ExpressionExt<T>,
    {
        let recent = select.expression().collect_recent(self)?;
        let predicate = select.predicate();
        Ok(Batch::from_sorted(
            recent
                .into_items()
                .into_iter()
                .filter(|(t, _)| predicate(t))
                .collect(),
        ))
    }

    fn collect_union<T, L, R>(&self, union: &Union<T, L, R>) -> Result<Batch<T>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        let left = union.left().collect_recent(self)?;
        let right = union.right().collect_recent(self)?;
        Ok(left.merge(right))
    }

    fn collect_intersect<T, L, R>(&self, intersect: &Intersect<T, L, R>) -> Result<Batch<T>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        let left_recent = intersect.left().collect_recent(self)?;
        let right_recent = intersect.right().collect_recent(self)?;
        if left_recent.is_empty() && right_recent.is_empty() {
            return Ok(Batch::empty());
        }

        let mut result = Vec::new();
        if !right_recent.is_empty() {
            for batch in intersect.left().collect_stable(self)?.iter() {
                intersect_helper(batch.items(), right_recent.items(), |t, w| {
                    result.push((t.clone(), w))
                });
            }
        }
        if !left_recent.is_empty() {
            for batch in intersect.right().collect_stable(self)?.iter() {
                intersect_helper(left_recent.items(), batch.items(), |t, w| {
                    result.push((t.clone(), w))
                });
            }
        }
        intersect_helper(left_recent.items(), right_recent.items(), |t, w| {
            result.push((t.clone(), w))
        });
        Ok(Batch::from_weighted(result))
    }

    fn collect_difference<T, L, R>(
        &self,
        difference: &Difference<T, L, R>,
    ) -> Result<Batch<T>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        let left_recent = difference.left().collect_recent(self)?;
        let right_recent = difference.right().collect_recent(self)?;
        if left_recent.is_empty() && right_recent.is_empty() {
            return Ok(Batch::empty());
        }

        // The output weight of a tuple is its left weight if its right weight is zero.
        // Recompute it for every tuple touched by either change:
        let affected = merge_keys(&left_recent.keys(), &right_recent.keys());
        let left_old = counts_in(&difference.left().collect_stable(self)?, &affected);
        let right_old = counts_in(&difference.right().collect_stable(self)?, &affected);

        let mut result = Vec::new();
        for (i, t) in affected.into_iter().enumerate() {
            let (l_old, r_old) = (left_old[i], right_old[i]);
            let l_new = l_old + left_recent.weight_of(&t);
            let r_new = r_old + right_recent.weight_of(&t);
            let out_old = if r_old == 0 { l_old } else { 0 };
            let out_new = if r_new == 0 { l_new } else { 0 };
            if out_new != out_old {
                result.push((t, out_new - out_old));
            }
        }
        Ok(Batch::from_sorted(result))
    }

    fn collect_distinct<T, E>(&self, distinct: &Distinct<T, E>) -> Result<Batch<T>, Error>
    where
        T: Tuple,
        E: ExpressionExt<T>,
    {
        let recent = distinct.expression().collect_recent(self)?;
        if recent.is_empty() {
            return Ok(Batch::empty());
        }
        let keys = recent.keys();
        let old = counts_in(&distinct.expression().collect_stable(self)?, &keys);

        let mut result = Vec::new();
        for ((t, w), old) in recent.into_items().into_iter().zip(old) {
            let change = (old + w > 0) as isize - (old > 0) as isize;
            if change != 0 {
                result.push((t, change));
            }
        }
        Ok(Batch::from_sorted(result))
    }

    fn collect_project<S, T, E>(&self, project: &Project<S, T, E>) -> Result<Batch<T>, Error>
    where
        T: Tuple,
        S: Tuple,
        E: ExpressionExt<S>,
    {
        let recent = project.expression().collect_recent(self)?;
        let mapper = project.mapper();
        Ok(Batch::from_weighted(
            recent
                .items()
                .iter()
                .map(|(t, w)| (mapper(t), *w))
                .collect(),
        ))
    }

    fn collect_product<L, R, Left, Right, T>(
        &self,
        product: &Product<L, R, Left, Right, T>,
    ) -> Result<Batch<T>, Error>
    where
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>,
    {
        let left_recent = product.left().collect_recent(self)?;
        let right_recent = product.right().collect_recent(self)?;
        if left_recent.is_empty() && right_recent.is_empty() {
            return Ok(Batch::empty());
        }

        let mapper = product.mapper();
        let mut result = Vec::new();
        if !right_recent.is_empty() {
            for batch in product.left().collect_stable(self)?.iter() {
                product_helper(batch.items(), right_recent.items(), |l, r, w| {
                    result.push((mapper(l, r), w))
                });
            }
        }
        if !left_recent.is_empty() {
            for batch in product.right().collect_stable(self)?.iter() {
                product_helper(left_recent.items(), batch.items(), |l, r, w| {
                    result.push((mapper(l, r), w))
                });
            }
        }
        product_helper(left_recent.items(), right_recent.items(), |l, r, w| {
            result.push((mapper(l, r), w))
        });
        Ok(Batch::from_weighted(result))
    }

    fn collect_join<K, L, R, Left, Right, T>(
        &self,
        join: &Join<K, L, R, Left, Right, T>,
    ) -> Result<Batch<T>, Error>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>,
    {
        let left_recent = join.left().collect_recent(self)?;
        let right_recent = join.right().collect_recent(self)?;
        if left_recent.is_empty() && right_recent.is_empty() {
            return Ok(Batch::empty());
        }

        let left_key = join.left_key();
        let right_key = join.right_key();
        let mapper = join.mapper();

        let left_recent = keyed(&left_recent, left_key);
        let right_recent = keyed(&right_recent, right_key);

        let mut result = Vec::new();
        if !right_recent.is_empty() {
            for batch in join.left().collect_stable(self)?.iter() {
                let batch = keyed(batch, left_key);
                join_helper(&batch, &right_recent, |k, l, r, w| {
                    result.push((mapper(k, l, r), w))
                });
            }
        }
        if !left_recent.is_empty() {
            for batch in join.right().collect_stable(self)?.iter() {
                let batch = keyed(batch, right_key);
                join_helper(&left_recent, &batch, |k, l, r, w| {
                    result.push((mapper(k, l, r), w))
                });
            }
        }
        join_helper(&left_recent, &right_recent, |k, l, r, w| {
            result.push((mapper(k, l, r), w))
        });
        Ok(Batch::from_weighted(result))
    }

    fn collect_view<T, E>(&self, view: &View<T, E>) -> Result<Batch<T>, Error>
    where
        T: Tuple + 'static,
        E: ExpressionExt<T> + 'static,
    {
        let table = self.database.view_instance(view)?;
        let recent = table.recent();
        Ok((**recent).clone())
    }
}

impl<'d> StableCollector for IncrementalCollector<'d> {
    fn collect_full<T>(&self, _: &Full<T>) -> Result<Vec<Rc<Batch<T>>>, Error>
    where
        T: Tuple,
    {
        Err(unsupported("Full"))
    }

    fn collect_empty<T>(&self, _: &Empty<T>) -> Result<Vec<Rc<Batch<T>>>, Error>
    where
        T: Tuple,
    {
        Ok(Vec::new())
    }

    fn collect_singleton<T>(&self, singleton: &Singleton<T>) -> Result<Vec<Rc<Batch<T>>>, Error>
    where
        T: Tuple,
    {
        Ok(vec![Rc::new(Batch::from_sorted(vec![(
            singleton.tuple().clone(),
            1,
        )]))])
    }

    fn collect_relation<T>(&self, relation: &Relation<T>) -> Result<Vec<Rc<Batch<T>>>, Error>
    where
        T: Tuple + 'static,
    {
        Ok(self.database.relation_instance(relation)?.stable().clone())
    }

    fn collect_select<T, E>(&self, select: &Select<T, E>) -> Result<Vec<Rc<Batch<T>>>, Error>
    where
        T: Tuple,
        E: ExpressionExt<T>,
    {
        let stable = select.expression().collect_stable(self)?;
        let predicate = select.predicate();
        Ok(stable
            .iter()
            .map(|batch| {
                Rc::new(Batch::from_sorted(
                    batch
                        .items()
                        .iter()
                        .filter(|(t, _)| predicate(t))
                        .cloned()
                        .collect(),
                ))
            })
            .collect())
    }

    fn collect_union<T, L, R>(&self, union: &Union<T, L, R>) -> Result<Vec<Rc<Batch<T>>>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        let mut result = union.left().collect_stable(self)?;
        result.extend(union.right().collect_stable(self)?);
        Ok(result)
    }

    fn collect_intersect<T, L, R>(
        &self,
        intersect: &Intersect<T, L, R>,
    ) -> Result<Vec<Rc<Batch<T>>>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        let left = intersect.left().collect_stable(self)?;
        let right = intersect.right().collect_stable(self)?;
        let mut result = Vec::new();
        for left_batch in left.iter() {
            let mut tuples = Vec::new();
            for right_batch in right.iter() {
                intersect_helper(left_batch.items(), right_batch.items(), |t, w| {
                    tuples.push((t.clone(), w))
                });
            }
            result.push(Rc::new(Batch::from_weighted(tuples)));
        }
        Ok(result)
    }

    fn collect_difference<T, L, R>(
        &self,
        difference: &Difference<T, L, R>,
    ) -> Result<Vec<Rc<Batch<T>>>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        let left = difference.left().collect_stable(self)?;
        let right = consolidate(&difference.right().collect_stable(self)?);
        Ok(left
            .iter()
            .map(|batch| {
                Rc::new(Batch::from_sorted(
                    batch
                        .items()
                        .iter()
                        .filter(|(t, _)| right.weight_of(t) == 0)
                        .cloned()
                        .collect(),
                ))
            })
            .collect())
    }

    fn collect_distinct<T, E>(&self, distinct: &Distinct<T, E>) -> Result<Vec<Rc<Batch<T>>>, Error>
    where
        T: Tuple,
        E: ExpressionExt<T>,
    {
        let all = consolidate(&distinct.expression().collect_stable(self)?);
        Ok(vec![Rc::new(Batch::from_sorted(
            all.items()
                .iter()
                .filter(|(_, w)| *w > 0)
                .map(|(t, _)| (t.clone(), 1))
                .collect(),
        ))])
    }

    fn collect_project<S, T, E>(
        &self,
        project: &Project<S, T, E>,
    ) -> Result<Vec<Rc<Batch<T>>>, Error>
    where
        T: Tuple,
        S: Tuple,
        E: ExpressionExt<S>,
    {
        let stable = project.expression().collect_stable(self)?;
        let mapper = project.mapper();
        Ok(stable
            .iter()
            .map(|batch| {
                Rc::new(Batch::from_weighted(
                    batch.items().iter().map(|(t, w)| (mapper(t), *w)).collect(),
                ))
            })
            .collect())
    }

    fn collect_product<L, R, Left, Right, T>(
        &self,
        product: &Product<L, R, Left, Right, T>,
    ) -> Result<Vec<Rc<Batch<T>>>, Error>
    where
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>,
    {
        let left = product.left().collect_stable(self)?;
        let right = product.right().collect_stable(self)?;
        let mapper = product.mapper();
        let mut result = Vec::new();
        for left_batch in left.iter() {
            let mut tuples = Vec::new();
            for right_batch in right.iter() {
                product_helper(left_batch.items(), right_batch.items(), |l, r, w| {
                    tuples.push((mapper(l, r), w))
                });
            }
            result.push(Rc::new(Batch::from_weighted(tuples)));
        }
        Ok(result)
    }

    fn collect_join<K, L, R, Left, Right, T>(
        &self,
        join: &Join<K, L, R, Left, Right, T>,
    ) -> Result<Vec<Rc<Batch<T>>>, Error>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>,
    {
        let left_key = join.left_key();
        let right_key = join.right_key();
        let mapper = join.mapper();

        let left = join.left().collect_stable(self)?;
        let right = join.right().collect_stable(self)?;
        let right: Vec<Vec<(K, &R, isize)>> =
            right.iter().map(|batch| keyed(batch, right_key)).collect();

        let mut result = Vec::new();
        for left_batch in left.iter() {
            let left_batch = keyed(left_batch, left_key);
            let mut tuples = Vec::new();
            for right_batch in right.iter() {
                join_helper(&left_batch, right_batch, |k, l, r, w| {
                    tuples.push((mapper(k, l, r), w))
                });
            }
            result.push(Rc::new(Batch::from_weighted(tuples)));
        }
        Ok(result)
    }

    fn collect_view<T, E>(&self, view: &View<T, E>) -> Result<Vec<Rc<Batch<T>>>, Error>
    where
        T: Tuple + 'static,
        E: ExpressionExt<T> + 'static,
    {
        Ok(self.database.view_instance(view)?.stable().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Tuples;

    #[test]
    fn test_evaluate_full() {
        {
            let database = Database::new();
            let s = Full::<i32>::new();
            assert!(database.evaluate(&s).is_err());
        }
    }
    #[test]
    fn test_evaluate_empty() {
        {
            let database = Database::new();
            let s = Empty::<i32>::new();
            let result = database.evaluate(&s).unwrap();
            assert_eq!(Tuples::from(vec![]), result);
        }
    }
    #[test]
    fn test_evaluate_singleton() {
        {
            let database = Database::new();
            let s = Singleton::new(42);
            let result = database.evaluate(&s).unwrap();
            assert_eq!(Tuples::from(vec![42]), result);
        }
    }
    #[test]
    fn test_evaluate_relation() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            database.insert(&r, vec![1, 2, 3].into()).unwrap();
            let result = database.evaluate(&r).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
        {
            let database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.add_relation::<i32>("r").unwrap();

            assert!(database.evaluate(&r).is_err());
        }
    }
    #[test]
    fn test_evaluate_project() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let project = r.builder().project(|t| t * 10).build();

            let result = database.evaluate(&project).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let project = r.builder().project(|t| t * 10).build();
            database.insert(&r, vec![1, 2, 3, 4].into()).unwrap();

            let result = database.evaluate(&project).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![10, 20, 30, 40]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let p1 = r.builder().project(|t| t * 10).build();
            let p2 = p1.builder().project(|t| t + 1).build();

            database.insert(&r, vec![1, 2, 3, 4].into()).unwrap();

            let result = database.evaluate(&p2).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![11, 21, 31, 41]), result);
        }
        {
            let database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.add_relation::<i32>("r").unwrap();
            let project = r.builder().project(|t| t + 1).build();
            assert!(database.evaluate(&project).is_err());
        }
    }
    #[test]
    fn test_evaluate_select() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let select = r.builder().select(|t| t % 2 == 1).build();

            let result = database.evaluate(&select).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let database = Database::new();
            let s = Singleton::new(42);
            let select = s.builder().select(|t| t % 2 == 0).build();

            let result = database.evaluate(&select).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let select = r.builder().select(|t| t % 2 == 0).build();
            database.insert(&r, vec![1, 2, 3, 4].into()).unwrap();

            let result = database.evaluate(&select).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![2, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let p1 = r.builder().select(|t| t % 2 == 0).build();
            let p2 = p1.builder().select(|&t| t > 3).build();

            database.insert(&r, vec![1, 2, 3, 4].into()).unwrap();

            let result = database.evaluate(&p2).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4]), result);
        }
        {
            let database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.add_relation::<i32>("r").unwrap();
            let select = r.builder().select(|&t| t > 1).build();
            assert!(database.evaluate(&select).is_err());
        }
    }
    #[test]
    fn test_evaluate_product() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let u = r.builder().product(s).on(|&l, &r| l + r).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            database.insert(&r, vec![1, 2, 3].into()).unwrap();
            let u = r.builder().product(s).on(|&l, &r| l + r).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            database.insert(&s, vec![4, 5].into()).unwrap();
            let u = r.builder().product(s).on(|&l, &r| l + r).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }

        {
            let database = Database::new();
            let r = Singleton::new(42);
            let s = Singleton::new(43);
            let u = r.builder().product(s).on(|&l, &r| l + r).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![85]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let u = r.builder().product(&s).on(|&l, &r| l + r).build();

            database.insert(&r, vec![1, 2, 3, 4].into()).unwrap();
            database.insert(&s, vec![0, 4, 5, 6].into()).unwrap();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(
                Tuples::<i32>::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let t = database.add_relation::<i32>("t").unwrap();
            let u1 = r.builder().product(&s).on(|&l, &r| l + r).build();
            let u2 = u1.builder().product(&t).on(|&l, &r| l + r).build();

            database.insert(&r, vec![1, 2, 3, 4].into()).unwrap();
            database.insert(&s, vec![100, 5, 200].into()).unwrap();
            database.insert(&t, vec![40, 30, 4].into()).unwrap();

            let result = database.evaluate(&u2).unwrap();
            assert_eq!(
                Tuples::<i32>::from(vec![
                    10, 11, 12, 13, 36, 37, 38, 39, 46, 47, 48, 49, 105, 106, 107, 108, 131, 132,
                    133, 134, 141, 142, 143, 144, 205, 206, 207, 208, 231, 232, 233, 234, 241, 242,
                    243, 244
                ]),
                result
            );
        }
        {
            let mut database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let u = r.builder().product(&s).on(|&l, &r| l + r).build();
            assert!(database.evaluate(&u).is_err());
        }
    }
    #[test]
    fn test_evaluate_join() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = database.add_relation::<(i32, i32)>("s").unwrap();
            let join = r
                .builder()
                .with_key(|t| t.0)
                .join(s.builder().with_key(|t| t.0))
                .on(|_, &l, &r| (l.1, r.1))
                .build();

            let result = database.evaluate(&join).unwrap();
            assert_eq!(Tuples::<(i32, i32)>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = database.add_relation::<(i32, i32)>("s").unwrap();
            let join = r
                .builder()
                .with_key(|t| t.0)
                .join(s.builder().with_key(|t| t.0))
                .on(|_, &l, &r| (l.1, r.1))
                .build();

            database
                .insert(&r, vec![(1, 4), (2, 2), (1, 3)].into())
                .unwrap();
            let result = database.evaluate(&join).unwrap();
            assert_eq!(Tuples::<(i32, i32)>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s1 = Singleton::new((1, 2));
            let s2 = Singleton::new((3, 5));
            let r_s1 = r
                .builder()
                .with_key(|t| t.0)
                .join(s1.builder().with_key(|t| t.0))
                .on(|_, &l, &r| (l.1, r.1))
                .build();
            database
                .insert(&r, vec![(1, 4), (2, 2), (1, 3)].into())
                .unwrap();
            database.evaluate(&r_s1).unwrap(); // materialize the first view
            let r_s1_s2 = r_s1
                .builder()
                .with_key(|t| t.0)
                .join(s2.builder().with_key(|t| t.0))
                .on(|_, &l, &r| (l.1, r.1))
                .build();
            let result = database.evaluate(&r_s1_s2).unwrap();
            assert_eq!(Tuples::from(vec![(2, 5)]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = database.add_relation::<(i32, i32)>("s").unwrap();
            let join = r
                .builder()
                .with_key(|t| t.0)
                .join(s.builder().with_key(|t| t.0))
                .on(|_, &l, &r| (l.1, r.1))
                .build();
            database
                .insert(&s, vec![(1, 5), (3, 2), (1, 6)].into())
                .unwrap();

            let result = database.evaluate(&join).unwrap();
            assert_eq!(Tuples::<(i32, i32)>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = database.add_relation::<(i32, i32)>("s").unwrap();
            let join = r
                .builder()
                .with_key(|t| t.0)
                .join(s.builder().with_key(|t| t.0))
                .on(|_, &l, &r| (l.1, r.1))
                .build();
            database
                .insert(&r, vec![(1, 4), (2, 2), (1, 3)].into())
                .unwrap();
            database
                .insert(&s, vec![(1, 5), (3, 2), (1, 6)].into())
                .unwrap();

            let result = database.evaluate(&join).unwrap();
            assert_eq!(
                Tuples::<(i32, i32)>::from(vec![(3, 5), (3, 6), (4, 5), (4, 6)]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = database.add_relation::<(i32, i32)>("s").unwrap();
            let t = database.add_relation::<(i32, i32)>("t").unwrap();
            let r_s = r
                .builder()
                .with_key(|t| t.0)
                .join(s.builder().with_key(|t| t.0))
                .on(|_, &l, &r| (l.1, r.1))
                .build();

            let r_s_t = r_s
                .builder()
                .with_key(|t| t.0)
                .join(t.builder().with_key(|t| t.0))
                .on(|_, _, &r| r.1)
                .build();

            database
                .insert(&r, vec![(1, 4), (2, 2), (1, 3)].into())
                .unwrap();
            database
                .insert(&s, vec![(1, 5), (3, 2), (1, 6)].into())
                .unwrap();
            database
                .insert(&t, vec![(1, 40), (2, 41), (3, 42), (4, 43)].into())
                .unwrap();

            let result = database.evaluate(&r_s_t).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42, 43]), result);
        }
        {
            let mut database = Database::new();
            let mut dummy = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = dummy.add_relation::<(i32, i32)>("s").unwrap();
            let join = r
                .builder()
                .with_key(|t| t.0)
                .join(s.builder().with_key(|t| t.0))
                .on(|_, &l, &r| (l.1, r.1))
                .build();
            assert!(database.evaluate(&join).is_err());
        }
    }
    #[test]
    fn test_evaluate_union() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let u = r.builder().union(s).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            database.insert(&r, vec![1, 2, 3].into()).unwrap();
            let u = r.builder().union(s).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            database.insert(&s, vec![4, 5].into()).unwrap();
            let u = r.builder().union(s).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4, 5]), result);
        }

        {
            let database = Database::new();
            let r = Singleton::new(42);
            let s = Singleton::new(43);
            let u = r.builder().union(s).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42, 43]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let u = r.builder().union(&s).build();

            database.insert(&r, vec![1, 2, 3, 4].into()).unwrap();
            database.insert(&s, vec![0, 4, 5, 6].into()).unwrap();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![0, 1, 2, 3, 4, 5, 6]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let t = database.add_relation::<i32>("t").unwrap();
            let u1 = r.builder().union(&s).build();
            let u2 = u1.builder().union(&t).build();

            database.insert(&r, vec![1, 2, 3, 4].into()).unwrap();
            database.insert(&s, vec![100, 5, 200].into()).unwrap();
            database.insert(&t, vec![40, 30, 4].into()).unwrap();

            let result = database.evaluate(&u2).unwrap();
            assert_eq!(
                Tuples::<i32>::from(vec![1, 2, 3, 4, 5, 30, 40, 100, 200]),
                result
            );
        }
        {
            let mut database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let u = r.builder().union(s).build();

            assert!(database.evaluate(&u).is_err());
        }
    }
    #[test]
    fn test_evaluate_intersect() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let u = r.builder().intersect(s).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            database.insert(&r, vec![1, 2, 3].into()).unwrap();
            let u = r.builder().intersect(s).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            database.insert(&s, vec![4, 5].into()).unwrap();
            let u = r.builder().intersect(s).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }

        {
            let database = Database::new();
            let r = Singleton::new(42);
            let s = Singleton::new(43);
            let u = r.builder().intersect(s).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let u = r.builder().intersect(&s).build();

            database.insert(&r, vec![1, 2, 3, 4].into()).unwrap();
            database.insert(&s, vec![0, 4, 2, 6].into()).unwrap();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![2, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let t = database.add_relation::<i32>("t").unwrap();
            let u1 = r.builder().intersect(&s).build();
            let u2 = u1.builder().intersect(&t).build();

            database.insert(&r, vec![1, 2, 3, 4].into()).unwrap();
            database.insert(&s, vec![100, 4, 2].into()).unwrap();
            database.insert(&t, vec![40, 2, 4, 100].into()).unwrap();

            let result = database.evaluate(&u2).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![2, 4]), result);
        }
        {
            let mut database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let u = r.builder().intersect(s.builder()).build();

            assert!(database.evaluate(&u).is_err());
        }
    }
    #[test]
    fn test_evaluate_difference() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let u = r.builder().difference(s).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            database.insert(&r, vec![1, 2, 3].into()).unwrap();
            let u = r.builder().difference(s).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation("r").unwrap();
            let s = database.add_relation("s").unwrap();
            database.insert(&r, vec![vec![1], vec![2]].into()).unwrap();
            database.insert(&s, vec![vec![1]].into()).unwrap();
            let u = r.builder().difference(s).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::from(vec![vec![2]]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation("r").unwrap();
            let s = database.add_relation("s").unwrap();
            database.insert(&r, vec![2].into()).unwrap();
            database.insert(&s, vec![1, 2].into()).unwrap();
            let u = r.builder().difference(s).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            database.insert(&s, vec![4, 5].into()).unwrap();
            let u = r.builder().difference(s).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }

        {
            let database = Database::new();
            let r = Singleton::new(42);
            let s = Singleton::new(43);
            let u = r.builder().difference(s).build();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let u = r.builder().difference(&s).build();

            database.insert(&r, vec![1, 2, 3, 4].into()).unwrap();
            database.insert(&s, vec![0, 4, 2, 6].into()).unwrap();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 3]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let t = database.add_relation::<i32>("t").unwrap();
            let u1 = r.builder().difference(&s).build();
            let u2 = u1.builder().difference(&t).build();

            database.insert(&r, vec![1, 2, 3, 4, 5].into()).unwrap();
            database.insert(&s, vec![100, 4, 2].into()).unwrap();
            database.insert(&t, vec![1, 2, 4, 100].into()).unwrap();

            let result = database.evaluate(&u2).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![3, 5]), result);
        }
        {
            let mut database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let u = r.builder().intersect(s).build();

            assert!(database.evaluate(&u).is_err());
        }
    }
    #[test]
    fn test_evaluate_view() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let v = database.store_view(r.clone()).unwrap();
            database.insert(&r, vec![1, 2, 3].into()).unwrap();
            let result = database.evaluate(&v).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let v_1 = database.store_view(r.clone()).unwrap();
            let v_2 = database.store_view(v_1).unwrap();
            let v_3 = database.store_view(v_2).unwrap();

            database.insert(&r, vec![1, 2, 3].into()).unwrap();

            let result = database.evaluate(&v_3).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = database.add_relation::<(i32, i32)>("s").unwrap();
            let r_s = r
                .builder()
                .with_key(|t| t.0)
                .join(s.builder().with_key(|t| t.0))
                .on(|_, &l, &r| (l.1, r.1))
                .build();
            let view = database.store_view(r_s).unwrap();

            database
                .insert(&r, vec![(1, 4), (2, 2), (1, 3)].into())
                .unwrap();
            database
                .insert(&s, vec![(1, 5), (3, 2), (1, 6)].into())
                .unwrap();

            let result = database.evaluate(&view).unwrap();
            assert_eq!(
                Tuples::<(i32, i32)>::from(vec![(3, 5), (3, 6), (4, 5), (4, 6)]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = database.add_relation::<(i32, i32)>("s").unwrap();
            let r_s = r
                .builder()
                .with_key(|t| t.0)
                .join(s.builder().with_key(|t| t.0))
                .on(|_, &l, &r| (l.1, r.1))
                .build();

            let view = database.store_view(r_s).unwrap();

            database
                .insert(&r, vec![(1, 4), (2, 2), (1, 3)].into())
                .unwrap();
            database
                .insert(&s, vec![(1, 5), (3, 2), (1, 6)].into())
                .unwrap();

            database.evaluate(&view).unwrap();
            database.insert(&s, vec![(1, 7)].into()).unwrap();
            let result = database.evaluate(&view).unwrap();
            assert_eq!(
                Tuples::<(i32, i32)>::from(vec![(3, 5), (3, 6), (3, 7), (4, 5), (4, 6), (4, 7)]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = database.add_relation::<(i32, i32)>("s").unwrap();
            let t = database.add_relation::<(i32, i32)>("t").unwrap();
            let r_s = r
                .builder()
                .with_key(|t| t.0)
                .join(s.builder().with_key(|t| t.0))
                .on(|_, &l, &r| (l.1, r.1))
                .build();
            let r_s_t = r_s
                .builder()
                .with_key(|t| t.0)
                .join(t.builder().with_key(|t| t.0))
                .on(|_, _, &r| r.1)
                .build();
            let view = database.store_view(r_s_t).unwrap();

            database
                .insert(&r, vec![(1, 4), (2, 2), (1, 3)].into())
                .unwrap();
            database
                .insert(&s, vec![(1, 5), (3, 2), (1, 6)].into())
                .unwrap();
            database
                .insert(&t, vec![(1, 40), (2, 41), (3, 42), (4, 43)].into())
                .unwrap();

            let result = database.evaluate(&view).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42, 43]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = database.add_relation::<(i32, i32)>("s").unwrap();
            let t = database.add_relation::<(i32, i32)>("t").unwrap();
            let rs = r.builder().union(s.clone()).build();
            let rs_t = rs
                .builder()
                .with_key(|t| t.0)
                .join(t.builder().with_key(|t| t.0))
                .on(|_, &l, &r| l.1 * r.1)
                .build();
            let view = database.store_view(rs_t).unwrap();

            database
                .insert(&r, vec![(1, 4), (2, 2), (1, 3)].into())
                .unwrap();
            database
                .insert(&s, vec![(1, 5), (3, 2), (1, 6)].into())
                .unwrap();
            database
                .insert(&t, vec![(1, 40), (2, 41), (3, 42), (4, 43)].into())
                .unwrap();

            let result = database.evaluate(&view).unwrap();
            assert_eq!(
                Tuples::<i32>::from(vec![82, 84, 120, 160, 200, 240]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = database.add_relation::<(i32, i32)>("s").unwrap();
            let t = database.add_relation::<(i32, i32)>("t").unwrap();
            let rs = r.builder().intersect(s.clone()).build();
            let rs_t = rs
                .builder()
                .with_key(|t| t.0)
                .join(t.builder().with_key(|t| t.0))
                .on(|_, &l, &r| l.1 * r.1)
                .build();
            let view = database.store_view(rs_t).unwrap();

            database
                .insert(&r, vec![(1, 4), (2, 2), (1, 3)].into())
                .unwrap();
            database
                .insert(&s, vec![(1, 4), (3, 2), (2, 2)].into())
                .unwrap();
            database
                .insert(&t, vec![(1, 40), (2, 41), (3, 42), (4, 43)].into())
                .unwrap();

            let result = database.evaluate(&view).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![82, 160]), result);
        }
        {
            // Difference is maintained incrementally, in both directions:
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let s = database.add_relation::<i32>("s").unwrap();
            let rs = r.builder().difference(s.clone()).build();
            let view = database.store_view(rs).unwrap();
            database.insert(&r, vec![1, 2, 3].into()).unwrap();
            assert_eq!(
                vec![1, 2, 3],
                database.evaluate(&view).unwrap().into_tuples()
            );
            database.insert(&s, vec![2].into()).unwrap();
            assert_eq!(vec![1, 3], database.evaluate(&view).unwrap().into_tuples());
            database.retract(&s, vec![2].into()).unwrap();
            assert_eq!(
                vec![1, 2, 3],
                database.evaluate(&view).unwrap().into_tuples()
            );
            database.retract(&r, vec![1].into()).unwrap();
            database.insert(&s, vec![3, 4].into()).unwrap();
            assert_eq!(vec![2], database.evaluate(&view).unwrap().into_tuples());
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = database.add_relation::<(i32, i32)>("s").unwrap();
            let t = database.add_relation::<(i32, i32)>("t").unwrap();
            let rs = r.builder().difference(s.clone()).build();
            let rs_t = rs
                .builder()
                .with_key(|t| t.0)
                .join(t.builder().with_key(|t| t.0))
                .on(|_, &l, &r| l.1 * r.1)
                .build();
            let view = database.store_view(rs_t).unwrap();
            database
                .insert(&r, vec![(1, 4), (2, 2), (1, 3)].into())
                .unwrap();
            database.insert(&t, vec![(1, 40), (2, 41)].into()).unwrap();
            assert_eq!(
                vec![82, 120, 160],
                database.evaluate(&view).unwrap().into_tuples()
            );
            database.insert(&s, vec![(1, 4)].into()).unwrap();
            assert_eq!(
                vec![82, 120],
                database.evaluate(&view).unwrap().into_tuples()
            );
        }
        {
            // Test new view initialization after a refering relation is already stable:
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let v1 = database.store_view(r.clone()).unwrap();
            database.insert(&r, vec![1, 2, 3].into()).unwrap();
            let _ = database.evaluate(&v1).unwrap();

            let v2 = database.store_view(r).unwrap();
            let result = database.evaluate(&v2).unwrap();
            assert_eq!(vec![1, 2, 3], result.into_tuples());
        }
        {
            // Do not recalculate an instance in the same update cycle:
            //   If `r` is recalculated twice (because of dependency from `v2`),
            //   it will lose its recent tuples, so `v3` will be empty.
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let v1 = database.store_view(r.clone()).unwrap();
            let r_v1 = r
                .builder()
                .with_key(|&t| t)
                .join(v1.builder().with_key(|&t| t))
                .on(|_, &l, &r| l + r)
                .build();
            let v2 = database.store_view(r_v1).unwrap();
            let v3 = database.store_view(r.clone()).unwrap();
            database.insert(&r, vec![1, 2].into()).unwrap();

            assert_eq!(vec![1, 2], database.evaluate(&v1).unwrap().into_tuples());
            assert_eq!(vec![2, 4], database.evaluate(&v2).unwrap().into_tuples());
            assert_eq!(vec![1, 2], database.evaluate(&v3).unwrap().into_tuples());
        }
        {
            // Do not recalculate a view in the same update cycle:
            //   If `u` is recalculated twice (because of dependency from `v2`),
            //   it will lose its recent tuples, so `v3` will be empty.
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r").unwrap();
            let u = database.store_view(r.clone()).unwrap();
            let v1 = database.store_view(u.clone()).unwrap();
            let u_v1 = r
                .builder()
                .with_key(|&t| t)
                .join(v1.builder().with_key(|&t| t))
                .on(|_, &l, &r| l + r)
                .build();
            let v2 = database.store_view(u_v1).unwrap();
            let v3 = database.store_view(u).unwrap();
            database.insert(&r, vec![1, 2].into()).unwrap();

            assert_eq!(vec![1, 2], database.evaluate(&v1).unwrap().into_tuples());
            assert_eq!(vec![2, 4], database.evaluate(&v2).unwrap().into_tuples());
            assert_eq!(vec![1, 2], database.evaluate(&v3).unwrap().into_tuples());
        }
    }
}
