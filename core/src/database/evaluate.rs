use super::{elements::Elements, Database, Tuples};
use crate::{
    expression::{
        Collector, Expression, Join, ListCollector, Project, Relation, Select, Singleton, Union,
        View,
    },
    tools::join_helper,
    tools::project_helper,
    Tuple,
};
use anyhow::Result;

pub(crate) struct Incremental<'d>(pub &'d Database);

impl<'d> Collector for Incremental<'d> {
    fn collect_singleton<T>(&self, _: &Singleton<T>) -> Result<Tuples<T>>
    where
        T: Tuple,
    {
        Ok(Vec::new().into())
    }

    fn collect_relation<T>(&self, relation: &Relation<T>) -> Result<Tuples<T>>
    where
        T: Tuple,
    {
        let table = self.0.relation_instance(relation)?;
        Ok(table.recent.borrow().clone())
    }

    fn collect_select<T, E>(&self, select: &Select<T, E>) -> Result<Tuples<T>>
    where
        T: Tuple,
        E: Expression<T>,
    {
        let mut result = Vec::new();
        let recent = select.expression().collect(self)?;
        let predicate = &mut (*select.predicate().borrow_mut());
        for tuple in &recent[..] {
            if predicate(tuple) {
                result.push(tuple.clone());
            }
        }
        Ok(result.into())
    }

    fn collect_union<T, L, R>(&self, union: &Union<T, L, R>) -> Result<Tuples<T>>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        let mut result = Vec::new();

        let left_recent = union.left().collect(self)?;
        let right_recent = union.right().collect(self)?;

        project_helper(&left_recent, |t| result.push(t.clone()));
        project_helper(&right_recent, |t| result.push(t.clone()));

        Ok(result.into())
    }

    fn collect_project<S, T, E>(&self, project: &Project<S, T, E>) -> Result<Tuples<T>>
    where
        T: Tuple,
        S: Tuple,
        E: Expression<S>,
    {
        let mut result = Vec::new();
        let recent = project.expression().collect(self)?;
        let mapper = &mut (*project.mapper().borrow_mut());
        project_helper(&recent, |t| result.push(mapper(t)));
        Ok(result.into())
    }

    fn collect_join<K, L, R, Left, Right, T>(
        &self,
        join: &crate::Join<K, L, R, Left, Right, T>,
    ) -> Result<Tuples<T>>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: Expression<(K, L)>,
        Right: Expression<(K, R)>,
    {
        let mut result = Vec::new();
        let incremental = Incremental(self.0);

        let left_recent = join.left().collect(self)?;
        let right_recent = join.right().collect(self)?;

        let left_stable = join.left().collect_list(&incremental)?;
        let right_stable = join.right().collect_list(&incremental)?;

        let mapper = &mut (*join.mapper().borrow_mut());

        for left_batch in left_stable.iter() {
            join_helper(&left_batch, &right_recent, |k, v1, v2| {
                result.push(mapper(k, v1, v2))
            });
        }

        for right_batch in right_stable.iter() {
            join_helper(&left_recent, &right_batch, |k, v1, v2| {
                result.push(mapper(k, v1, v2))
            });
        }

        join_helper(&left_recent, &right_recent, |k, v1, v2| {
            result.push(mapper(k, v1, v2))
        });

        Ok(result.into())
    }

    fn collect_view<T, E>(&self, view: &View<T, E>) -> Result<Tuples<T>>
    where
        T: Tuple,
        E: Expression<T> + 'static,
    {
        let table = self.0.view_instance(view)?;
        Ok(table.recent.borrow().clone())
    }
}

impl<'d> ListCollector for Incremental<'d> {
    fn collect_singleton<T>(&self, singleton: &Singleton<T>) -> Result<Vec<Tuples<T>>>
    where
        T: Tuple,
    {
        Ok(vec![vec![singleton.0.clone()].into()])
    }

    fn collect_relation<T>(&self, relation: &Relation<T>) -> Result<Vec<Tuples<T>>>
    where
        T: Tuple,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let table = self.0.relation_instance(&relation)?;
        for batch in table.stable.borrow().iter() {
            result.push(batch.clone());
        }
        Ok(result)
    }

    fn collect_select<T, E>(&self, select: &Select<T, E>) -> Result<Vec<Tuples<T>>>
    where
        T: Tuple,
        E: Expression<T>,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let stable = select.expression().collect_list(self)?;
        let predicate = &mut (*select.predicate().borrow_mut());
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

    fn collect_union<T, L, R>(&self, union: &Union<T, L, R>) -> Result<Vec<Tuples<T>>>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let left_stable = union.left().collect_list(self)?;
        let right_stable = union.right().collect_list(self)?;

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

    fn collect_project<S, T, E>(&self, project: &Project<S, T, E>) -> Result<Vec<Tuples<T>>>
    where
        T: Tuple,
        S: Tuple,
        E: Expression<S>,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let stable = project.expression().collect_list(self)?;
        let mapper = &mut (*project.mapper().borrow_mut());
        for batch in stable.iter() {
            let mut tuples = Vec::new();
            project_helper(&batch, |t| tuples.push(mapper(t)));
            result.push(tuples.into());
        }
        Ok(result)
    }

    fn collect_join<K, L, R, Left, Right, T>(
        &self,
        join: &crate::Join<K, L, R, Left, Right, T>,
    ) -> Result<Vec<Tuples<T>>>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: Expression<(K, L)>,
        Right: Expression<(K, R)>,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let left = join.left().collect_list(self)?;
        let right = join.right().collect_list(self)?;

        let mapper = &mut (*join.mapper().borrow_mut());
        for left_batch in left.iter() {
            let mut tuples = Vec::new();
            for right_batch in right.iter() {
                join_helper(&left_batch, &right_batch, |k, v1, v2| {
                    tuples.push(mapper(k, v1, v2))
                });
            }
            result.push(tuples.into());
        }
        Ok(result)
    }

    fn collect_view<T, E>(&self, view: &View<T, E>) -> Result<Vec<Tuples<T>>>
    where
        T: Tuple,
        E: Expression<T> + 'static,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let table = self.0.view_instance(&view)?;
        for batch in table.stable.borrow().iter() {
            result.push(batch.clone());
        }
        Ok(result)
    }
}

pub(crate) struct Evaluator<'d>(pub &'d Database);

impl<'d> Collector for Evaluator<'d> {
    fn collect_singleton<T>(&self, singleton: &Singleton<T>) -> Result<Tuples<T>>
    where
        T: Tuple,
    {
        Ok(vec![singleton.0.clone()].into())
    }

    fn collect_relation<T>(&self, relation: &Relation<T>) -> Result<Tuples<T>>
    where
        T: Tuple,
    {
        self.0.recalculate_relation(&relation.name)?;
        let table = self.0.relation_instance(&relation)?;
        assert!(table.recent.borrow().is_empty());
        assert!(table.to_add.borrow().is_empty());

        let incremental = Incremental(self.0);

        let mut result = relation.collect(&incremental)?;
        for batch in relation.collect_list(&incremental)? {
            result = result.merge(batch);
        }

        Ok(result)
    }

    fn collect_select<T, E>(&self, select: &Select<T, E>) -> Result<Tuples<T>>
    where
        T: Tuple,
        E: Expression<T>,
    {
        let mut elements = crate::database::elements::Elements::new();
        select.visit(&mut elements);

        for r in elements.relations() {
            self.0.recalculate_relation(&r)?;
        }

        for r in elements.views() {
            self.0.recalculate_view(&r)?;
        }

        let incremental = Incremental(self.0);

        let mut result = select.collect(&incremental)?;
        for batch in select.collect_list(&incremental)? {
            result = result.merge(batch);
        }
        Ok(result)
    }

    fn collect_union<T, L, R>(&self, union: &Union<T, L, R>) -> Result<Tuples<T>>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        let mut elements = crate::database::elements::Elements::new();
        union.visit(&mut elements);

        for r in elements.relations() {
            self.0.recalculate_relation(&r)?;
        }

        for r in elements.views() {
            self.0.recalculate_view(&r)?;
        }

        let incremental = Incremental(self.0);

        let mut result = union.collect(&incremental)?;
        for batch in union.collect_list(&incremental)? {
            result = result.merge(batch);
        }
        Ok(result)
    }

    fn collect_project<S, T, E>(&self, project: &Project<S, T, E>) -> Result<Tuples<T>>
    where
        T: Tuple,
        S: Tuple,
        E: Expression<S>,
    {
        let mut elements = crate::database::elements::Elements::new();
        project.visit(&mut elements);

        for r in elements.relations() {
            self.0.recalculate_relation(&r)?;
        }

        for r in elements.views() {
            self.0.recalculate_view(&r)?;
        }

        let incremental = Incremental(self.0);

        let mut result = project.collect(&incremental)?;
        for batch in project.collect_list(&incremental)? {
            result = result.merge(batch);
        }
        Ok(result)
    }

    fn collect_join<K, L, R, Left, Right, T>(
        &self,
        join: &Join<K, L, R, Left, Right, T>,
    ) -> Result<Tuples<T>>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: Expression<(K, L)>,
        Right: Expression<(K, R)>,
    {
        let mut elements = Elements::new();
        join.visit(&mut elements);

        for r in elements.relations() {
            self.0.recalculate_relation(&r)?;
        }

        for r in elements.views() {
            self.0.recalculate_view(&r)?;
        }

        let incremental = Incremental(self.0);

        let mut result = join.collect(&incremental)?;
        for batch in join.collect_list(&incremental)? {
            result = result.merge(batch);
        }

        Ok(result)
    }

    fn collect_view<T, E>(&self, view: &View<T, E>) -> Result<Tuples<T>>
    where
        T: Tuple,
        E: Expression<T> + 'static,
    {
        self.0.recalculate_view(&view.reference)?;
        let table = self.0.view_instance(view)?;
        assert!(table.recent.borrow().is_empty());
        assert!(table.to_add.borrow().is_empty());

        let incremental = Incremental(self.0);

        let mut result = view.collect(&incremental)?;
        for batch in view.collect_list(&incremental)? {
            result = result.merge(batch);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

    #[test]
    fn test_clone_select() {
        let mut database = Database::new();
        let r = database.add_relation::<i32>("r");
        let s = database.add_relation::<i32>("s");
        r.insert(vec![1, 2, 3].into(), &database).unwrap();
        r.insert(vec![4, 5].into(), &database).unwrap();
        let u = Union::new(&r, &s).clone();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 2, 3, 4, 5]),
            database.evaluate(&u).unwrap()
        );
    }

    #[test]
    fn test_evaluate_select() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            let u = Union::new(&r, &s);

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            r.insert(vec![1, 2, 3].into(), &database).unwrap();
            let u = Union::new(&r, &s);

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            s.insert(vec![4, 5].into(), &database).unwrap();
            let u = Union::new(&r, &s);

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4, 5]), result);
        }

        {
            let database = Database::new();
            let r = Singleton(42);
            let s = Singleton(43);
            let u = Union::new(&r, &s);

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42, 43]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            let u = Union::new(&r, &s);
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();
            s.insert(vec![0, 4, 5, 6].into(), &database).unwrap();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![0, 1, 2, 3, 4, 5, 6]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            let t = database.add_relation::<i32>("t");
            let u1 = Union::new(&r, &s);
            let u2 = Union::new(&u1, &t);

            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();
            s.insert(vec![100, 5, 200].into(), &database).unwrap();
            t.insert(vec![40, 30, 4].into(), &database).unwrap();

            let result = database.evaluate(&u2).unwrap();
            assert_eq!(
                Tuples::<i32>::from(vec![1, 2, 3, 4, 5, 30, 40, 100, 200]),
                result
            );
        }
        {
            let mut database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            let u = Union::new(&r, &s);
            assert!(database.evaluate(&u).is_err());
        }
    }
}
