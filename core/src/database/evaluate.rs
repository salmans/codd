/// Implements an incremental algorithm for evaluating an expression in a database.
use super::{
    expression_ext::{ExpressionExt, RecentCollector, StableCollector},
    helpers::{diff_helper, intersect_helper, join_helper, product_helper, project_helper},
    Database, Tuples,
};
use crate::{expression::*, Error, Tuple};

/// Implements `crate::expression::RecentCollector` and `crate::expression::StableCollector`
/// to incrementally collect recent and stable tuples of `Instance`s of a database for
/// expressions.
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

impl<'d> RecentCollector for IncrementalCollector<'d> {
    fn collect_full<T>(&self, _: &Full<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
    {
        // `Full` is not range restricted, so cannot be evaluated.
        Err(Error::UnsupportedExpression {
            name: "Full".to_string(),
            operation: "Evaluate".to_string(),
        })
    }

    fn collect_empty<T>(&self, _: &Empty<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
    {
        Ok(Vec::new().into())
    }

    fn collect_singleton<T>(&self, _: &Singleton<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
    {
        Ok(Vec::new().into())
    }

    fn collect_relation<T>(&self, relation: &Relation<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple + 'static,
    {
        let table = self.database.relation_instance(relation)?;
        Ok(table.recent().clone())
    }

    fn collect_select<T, E>(&self, select: &Select<T, E>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        E: ExpressionExt<T>,
    {
        let mut result = Vec::new();
        let recent = select.expression().collect_recent(self)?;
        let mut predicate = select.predicate_mut();
        for tuple in &recent[..] {
            if predicate(tuple) {
                result.push(tuple.clone());
            }
        }
        Ok(result.into())
    }

    fn collect_union<T, L, R>(&self, union: &Union<T, L, R>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        let mut result = Vec::new();

        let left_recent = union.left().collect_recent(self)?;
        let right_recent = union.right().collect_recent(self)?;

        for tuple in &left_recent[..] {
            result.push(tuple.clone());
        }
        for tuple in &right_recent[..] {
            result.push(tuple.clone());
        }

        Ok(result.into())
    }

    fn collect_intersect<T, L, R>(&self, intersect: &Intersect<T, L, R>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        let mut result = Vec::new();
        let incremental = IncrementalCollector::new(self.database);

        let left_recent = intersect.left().collect_recent(self)?;
        let right_recent = intersect.right().collect_recent(self)?;

        let left_stable = intersect.left().collect_stable(&incremental)?;
        let right_stable = intersect.right().collect_stable(&incremental)?;

        for batch in left_stable.iter() {
            intersect_helper(&batch, &right_recent, |t| result.push(t.clone()))
        }
        for batch in right_stable.iter() {
            intersect_helper(&left_recent, &batch, |t| result.push(t.clone()))
        }

        intersect_helper(&left_recent, &right_recent, |t| result.push(t.clone()));
        Ok(result.into())
    }

    fn collect_difference<T, L, R>(
        &self,
        difference: &Difference<T, L, R>,
    ) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        let mut result = Vec::new();
        let incremental = IncrementalCollector::new(self.database);

        let left_recent = difference.left().collect_recent(self)?;
        let left_stable = difference.left().collect_stable(&incremental)?;
        let right_stable = difference.right().collect_stable(&incremental)?;
        let right_stable_slices = right_stable.iter().map(|t| &t[..]).collect::<Vec<_>>();

        for batch in left_stable.iter() {
            diff_helper(&batch, &right_stable_slices, |t| result.push(t.clone()));
        }

        diff_helper(&left_recent, &right_stable_slices, |t| {
            result.push(t.clone())
        });
        Ok(result.into())
    }

    fn collect_project<S, T, E>(&self, project: &Project<S, T, E>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        S: Tuple,
        E: ExpressionExt<S>,
    {
        let mut result = Vec::new();
        let recent = project.expression().collect_recent(self)?;
        let mut mapper = project.mapper_mut();

        project_helper(&recent, |t| result.push(mapper(t)));
        Ok(result.into())
    }

    fn collect_product<L, R, Left, Right, T>(
        &self,
        product: &Product<L, R, Left, Right, T>,
    ) -> Result<Tuples<T>, Error>
    where
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>,
    {
        let mut result = Vec::new();
        let incremental = IncrementalCollector::new(self.database);

        let left_recent = product.left().collect_recent(self)?;
        let right_recent = product.right().collect_recent(self)?;

        let left_stable = product.left().collect_stable(&incremental)?;
        let right_stable = product.right().collect_stable(&incremental)?;

        let mut mapper = product.mapper_mut();

        for batch in left_stable.iter() {
            product_helper(&batch, &right_recent, |v1, v2| result.push(mapper(v1, v2)));
        }
        for batch in right_stable.iter() {
            product_helper(&left_recent, &batch, |v1, v2| result.push(mapper(v1, v2)));
        }

        product_helper(&left_recent, &right_recent, |v1, v2| {
            result.push(mapper(v1, v2))
        });

        Ok(result.into())
    }

    fn collect_join<K, L, R, Left, Right, T>(
        &self,
        join: &Join<K, L, R, Left, Right, T>,
    ) -> Result<Tuples<T>, Error>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>,
    {
        let mut result = Vec::new();
        let incremental = IncrementalCollector::new(self.database);

        let mut left_key = join.left_key_mut();
        let mut right_key = join.right_key_mut();

        let left_recent = join.left().collect_recent(self)?;
        let left_recent: Tuples<(K, &L)> = left_recent.iter().map(|t| (left_key(&t), t)).into();
        let right_recent = join.right().collect_recent(self)?;
        let right_recent: Tuples<(K, &R)> = right_recent.iter().map(|t| (right_key(&t), t)).into();

        let left_stable = join.left().collect_stable(&incremental)?;
        let left_stable: Vec<Tuples<(K, &L)>> = left_stable
            .iter()
            .map(|batch| batch.iter().map(|t| (left_key(&t), t)).into())
            .collect();

        let right_stable = join.right().collect_stable(&incremental)?;
        let right_stable: Vec<Tuples<(K, &R)>> = right_stable
            .iter()
            .map(|batch| batch.iter().map(|t| (right_key(&t), t)).into())
            .collect();

        let mut joiner = join.mapper_mut();

        for batch in left_stable.iter() {
            join_helper(&batch, &right_recent, |k, v1, v2| {
                result.push(joiner(k, v1, v2))
            });
        }
        for batch in right_stable.iter() {
            join_helper(&left_recent, &batch, |k, v1, v2| {
                result.push(joiner(k, v1, v2))
            });
        }
        join_helper(&left_recent, &right_recent, |k, v1, v2| {
            result.push(joiner(k, v1, v2))
        });

        Ok(result.into())
    }

    fn collect_view<T, E>(&self, view: &View<T, E>) -> Result<Tuples<T>, Error>
    where
        T: Tuple + 'static,
        E: ExpressionExt<T> + 'static,
    {
        let table = self.database.view_instance(view)?;
        Ok(table.recent().clone())
    }
}

impl<'d> StableCollector for IncrementalCollector<'d> {
    fn collect_full<T>(&self, _: &Full<T>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
    {
        // `Full` cannot be evaluated.
        Err(Error::UnsupportedExpression {
            name: "Full".to_string(),
            operation: "Evaluate".to_string(),
        })
    }

    fn collect_empty<T>(&self, _: &Empty<T>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
    {
        Ok(Vec::new().into())
    }

    fn collect_singleton<T>(&self, singleton: &Singleton<T>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
    {
        Ok(vec![vec![singleton.tuple().clone()].into()])
    }

    fn collect_relation<T>(&self, relation: &Relation<T>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple + 'static,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let table = self.database.relation_instance(&relation)?;
        for batch in table.stable().iter() {
            result.push(batch.clone());
        }
        Ok(result)
    }

    fn collect_select<T, E>(&self, select: &Select<T, E>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        E: ExpressionExt<T>,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let stable = select.expression().collect_stable(self)?;
        let mut predicate = select.predicate_mut();
        for batch in stable.iter() {
            let mut tuples = Vec::new();
            for tuple in &batch[..] {
                if predicate(tuple) {
                    tuples.push(tuple.clone());
                }
            }
            result.push(tuples.into());
        }
        Ok(result)
    }

    fn collect_union<T, L, R>(&self, union: &Union<T, L, R>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let left_stable = union.left().collect_stable(self)?;
        let right_stable = union.right().collect_stable(self)?;

        for batch in left_stable.iter() {
            let mut tuples = Vec::new();
            project_helper(&batch, |t| tuples.push(t.clone()));
            result.push(tuples.into());
        }
        for batch in right_stable.iter() {
            let mut tuples = Vec::new();
            project_helper(&batch, |t| tuples.push(t.clone()));
            result.push(tuples.into());
        }

        Ok(result)
    }

    fn collect_intersect<T, L, R>(
        &self,
        intersect: &Intersect<T, L, R>,
    ) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let left = intersect.left().collect_stable(self)?;
        let right = intersect.right().collect_stable(self)?;

        for left_batch in left.iter() {
            let mut tuples = Vec::new();
            for right_batch in right.iter() {
                intersect_helper(&left_batch, &right_batch, |t| tuples.push(t.clone()));
            }
            result.push(tuples.into());
        }
        Ok(result)
    }

    fn collect_difference<T, L, R>(
        &self,
        difference: &Difference<T, L, R>,
    ) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let left = difference.left().collect_stable(self)?;
        let right = difference.right().collect_stable(self)?;
        let right_slices = right.iter().map(|t| &t[..]).collect::<Vec<_>>();

        for batch in left.iter() {
            let mut tuples = Vec::new();
            diff_helper(&batch, &right_slices, |t| tuples.push(t.clone()));
            result.push(tuples.into());
        }
        Ok(result)
    }

    fn collect_project<S, T, E>(&self, project: &Project<S, T, E>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        S: Tuple,
        E: ExpressionExt<S>,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let stable = project.expression().collect_stable(self)?;
        let mut mapper = project.mapper_mut();
        for batch in stable.iter() {
            let mut tuples = Vec::new();
            project_helper(&batch, |t| tuples.push(mapper(t)));
            result.push(tuples.into());
        }
        Ok(result)
    }

    fn collect_product<L, R, Left, Right, T>(
        &self,
        product: &Product<L, R, Left, Right, T>,
    ) -> Result<Vec<Tuples<T>>, Error>
    where
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let left = product.left().collect_stable(self)?;
        let right = product.right().collect_stable(self)?;

        let mut mapper = product.mapper_mut();
        for left_batch in left.iter() {
            let mut tuples = Vec::new();
            for right_batch in right.iter() {
                product_helper(&left_batch, &right_batch, |v1, v2| {
                    tuples.push(mapper(v1, v2))
                });
            }
            result.push(tuples.into());
        }
        Ok(result)
    }

    fn collect_join<K, L, R, Left, Right, T>(
        &self,
        join: &Join<K, L, R, Left, Right, T>,
    ) -> Result<Vec<Tuples<T>>, Error>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let mut left_key = join.left_key_mut();
        let mut right_key = join.right_key_mut();

        let left = join.left().collect_stable(self)?;
        let left: Vec<Tuples<(K, &L)>> = left
            .iter()
            .map(|batch| batch.iter().map(|t| (left_key(&t), t)).into())
            .collect();

        let right = join.right().collect_stable(self)?;
        let right: Vec<Tuples<(K, &R)>> = right
            .iter()
            .map(|batch| batch.iter().map(|t| (right_key(&t), t)).into())
            .collect();

        let mut joiner = join.mapper_mut();
        for left_batch in left.iter() {
            let mut tuples = Vec::new();
            for right_batch in right.iter() {
                join_helper(&left_batch, &right_batch, |k, v1, v2| {
                    tuples.push(joiner(k, v1, v2))
                });
            }
            result.push(tuples.into());
        }
        Ok(result)
    }

    fn collect_view<T, E>(&self, view: &View<T, E>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple + 'static,
        E: ExpressionExt<T> + 'static,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let table = self.database.view_instance(&view)?;
        for batch in table.stable().iter() {
            result.push(batch.clone());
        }
        Ok(result)
    }
}

/// Is an incremental evaluator for evaluating expressions in a database.
#[derive(Clone)]
pub(super) struct Evaluator<'d> {
    /// Is the database in which the visited expression is evaluated.
    database: &'d Database,
}

impl<'d> Evaluator<'d> {
    /// Creates a new `Evaluator`.
    pub fn new(database: &'d Database) -> Self {
        Self { database }
    }
}

impl<'d> RecentCollector for Evaluator<'d> {
    fn collect_full<T>(&self, _: &Full<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
    {
        Err(Error::UnsupportedExpression {
            name: "Full".to_string(),
            operation: "Evaluate".to_string(),
        })
    }

    fn collect_empty<T>(&self, _: &Empty<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
    {
        Ok(Vec::new().into())
    }

    fn collect_singleton<T>(&self, singleton: &Singleton<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
    {
        Ok(vec![singleton.tuple().clone()].into())
    }

    fn collect_relation<T>(&self, relation: &Relation<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple + 'static,
    {
        // stabilize the instance corresponding to this relation before evaluating the relation:
        self.database.stabilize_relation(relation.name())?;
        let table = self.database.relation_instance(&relation)?;

        assert!(table.recent().is_empty());
        assert!(table.to_add().is_empty());

        let incremental = IncrementalCollector::new(self.database);

        let mut result = relation.collect_recent(&incremental)?;
        for batch in relation.collect_stable(&incremental)? {
            result = result.merge(batch);
        }

        Ok(result)
    }

    fn collect_select<T, E>(&self, select: &Select<T, E>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        E: ExpressionExt<T>,
    {
        // stabilize the dependencies of the expression before evaluating it:
        for r in select.relation_dependencies() {
            self.database.stabilize_relation(&r)?;
        }
        for r in select.view_dependencies() {
            self.database.stabilize_view(&r)?;
        }

        let incremental = IncrementalCollector::new(self.database);

        let mut result = select.collect_recent(&incremental)?;
        for batch in select.collect_stable(&incremental)? {
            result = result.merge(batch);
        }
        Ok(result)
    }

    fn collect_union<T, L, R>(&self, union: &Union<T, L, R>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        for r in union.relation_dependencies() {
            self.database.stabilize_relation(&r)?;
        }
        for r in union.view_dependencies() {
            self.database.stabilize_view(&r)?;
        }

        let incremental = IncrementalCollector::new(self.database);

        let mut result = union.collect_recent(&incremental)?;
        for batch in union.collect_stable(&incremental)? {
            result = result.merge(batch);
        }
        Ok(result)
    }

    fn collect_intersect<T, L, R>(&self, intersect: &Intersect<T, L, R>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        for r in intersect.relation_dependencies() {
            self.database.stabilize_relation(&r)?;
        }
        for r in intersect.view_dependencies() {
            self.database.stabilize_view(&r)?;
        }

        let incremental = IncrementalCollector::new(self.database);

        let mut result = intersect.collect_recent(&incremental)?;
        for batch in intersect.collect_stable(&incremental)? {
            result = result.merge(batch);
        }

        Ok(result)
    }

    fn collect_difference<T, L, R>(
        &self,
        difference: &Difference<T, L, R>,
    ) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        L: ExpressionExt<T>,
        R: ExpressionExt<T>,
    {
        for r in difference.relation_dependencies() {
            self.database.stabilize_relation(&r)?;
        }
        for r in difference.view_dependencies() {
            self.database.stabilize_view(&r)?;
        }

        let incremental = IncrementalCollector::new(self.database);

        let mut result = difference.collect_recent(&incremental)?;
        for batch in difference.collect_stable(&incremental)? {
            result = result.merge(batch);
        }

        Ok(result)
    }

    fn collect_project<S, T, E>(&self, project: &Project<S, T, E>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        S: Tuple,
        E: ExpressionExt<S>,
    {
        for r in project.relation_dependencies() {
            self.database.stabilize_relation(&r)?;
        }
        for r in project.view_dependencies() {
            self.database.stabilize_view(&r)?;
        }

        let incremental = IncrementalCollector::new(self.database);

        let mut result = project.collect_recent(&incremental)?;
        for batch in project.collect_stable(&incremental)? {
            result = result.merge(batch);
        }
        Ok(result)
    }

    fn collect_product<L, R, Left, Right, T>(
        &self,
        product: &Product<L, R, Left, Right, T>,
    ) -> Result<Tuples<T>, Error>
    where
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>,
    {
        for r in product.relation_dependencies() {
            self.database.stabilize_relation(&r)?;
        }
        for r in product.view_dependencies() {
            self.database.stabilize_view(&r)?;
        }

        let incremental = IncrementalCollector::new(self.database);

        let mut result = product.collect_recent(&incremental)?;
        for batch in product.collect_stable(&incremental)? {
            result = result.merge(batch);
        }

        Ok(result)
    }

    fn collect_join<K, L, R, Left, Right, T>(
        &self,
        join: &Join<K, L, R, Left, Right, T>,
    ) -> Result<Tuples<T>, Error>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: ExpressionExt<L>,
        Right: ExpressionExt<R>,
    {
        for r in join.relation_dependencies() {
            self.database.stabilize_relation(&r)?;
        }
        for r in join.view_dependencies() {
            self.database.stabilize_view(&r)?;
        }

        let incremental = IncrementalCollector::new(self.database);

        let mut result = join.collect_recent(&incremental)?;
        for batch in join.collect_stable(&incremental)? {
            result = result.merge(batch);
        }

        Ok(result)
    }

    fn collect_view<T, E>(&self, view: &View<T, E>) -> Result<Tuples<T>, Error>
    where
        T: Tuple + 'static,
        E: ExpressionExt<T> + 'static,
    {
        self.database.stabilize_view(view.reference())?;
        let table = self.database.view_instance(view)?;
        assert!(table.recent().is_empty());
        assert!(table.to_add().is_empty());

        let incremental = IncrementalCollector::new(self.database);

        let mut result = view.collect_recent(&incremental)?;
        for batch in view.collect_stable(&incremental)? {
            result = result.merge(batch);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = database.add_relation::<(i32, i32)>("s").unwrap();
            let rs = r.builder().difference(s.clone()).build();

            assert!(database.store_view(rs).is_err());
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r").unwrap();
            let s = database.add_relation::<(i32, i32)>("s").unwrap();
            let t = database.add_relation::<(i32, i32)>("t").unwrap();
            let rs = r.builder().difference(s).build();
            let rs_t = rs
                .builder()
                .with_key(|t| t.0)
                .join(t.builder().with_key(|t| t.0))
                .on(|_, &l, &r| l.1 * r.1)
                .build();
            assert!(database.store_view(rs_t).is_err());
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
