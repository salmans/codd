use super::{elements::Elements, Database, Tuples};
use crate::{
    expression::*,
    tools::{diff_helper, intersect_helper, join_helper, project_helper},
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

    fn collect_intersect<T, L, R>(&self, intersect: &Intersect<T, L, R>) -> Result<Tuples<T>>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        let mut result = Vec::new();
        let incremental = Incremental(self.0);

        let left_recent = intersect.left().collect(self)?;
        let right_recent = intersect.right().collect(self)?;

        let left_stable = intersect.left().collect_list(&incremental)?;
        let right_stable = intersect.right().collect_list(&incremental)?;

        for batch in left_stable.iter() {
            intersect_helper(&batch, &right_recent, &mut result)
        }

        for batch in right_stable.iter() {
            intersect_helper(&left_recent, &batch, &mut result)
        }

        intersect_helper(&left_recent, &right_recent, &mut result);
        Ok(result.into())
    }

    fn collect_diff<T, L, R>(&self, diff: &Diff<T, L, R>) -> Result<Tuples<T>>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        let mut result = Vec::new();
        let incremental = Incremental(self.0);

        let left_recent = diff.left().collect(self)?;
        let left_stable = diff.left().collect_list(&incremental)?;
        let right_stable = diff.right().collect_list(&incremental)?;

        for batch in left_stable.iter() {
            diff_helper(&batch, &right_stable, &mut result)
        }

        diff_helper(&left_recent, &right_stable, &mut result);
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
        let mut result = Vec::new();
        let incremental = Incremental(self.0);

        let left_recent = join.left().collect(self)?;
        let right_recent = join.right().collect(self)?;

        let left_stable = join.left().collect_list(&incremental)?;
        let right_stable = join.right().collect_list(&incremental)?;

        let mapper = &mut (*join.mapper().borrow_mut());

        for batch in left_stable.iter() {
            join_helper(&batch, &right_recent, |k, v1, v2| {
                result.push(mapper(k, v1, v2))
            });
        }

        for batch in right_stable.iter() {
            join_helper(&left_recent, &batch, |k, v1, v2| {
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

    fn collect_intersect<T, L, R>(&self, intersect: &Intersect<T, L, R>) -> Result<Vec<Tuples<T>>>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let left = intersect.left().collect_list(self)?;
        let right = intersect.right().collect_list(self)?;

        for left_batch in left.iter() {
            let mut tuples = Vec::new();
            for right_batch in right.iter() {
                intersect_helper(&left_batch, &right_batch, &mut tuples);
            }
            result.push(tuples.into());
        }
        Ok(result)
    }

    fn collect_diff<T, L, R>(&self, diff: &Diff<T, L, R>) -> Result<Vec<Tuples<T>>>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        let mut result = Vec::<Tuples<T>>::new();
        let left = diff.left().collect_list(self)?;
        let right = diff.right().collect_list(self)?;

        for batch in left.iter() {
            let mut tuples = Vec::new();
            diff_helper(&batch, &right, &mut tuples);
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
        join: &Join<K, L, R, Left, Right, T>,
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

    fn collect_intersect<T, L, R>(&self, intersect: &Intersect<T, L, R>) -> Result<Tuples<T>>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        let mut elements = Elements::new();
        intersect.visit(&mut elements);

        for r in elements.relations() {
            self.0.recalculate_relation(&r)?;
        }

        for r in elements.views() {
            self.0.recalculate_view(&r)?;
        }

        let incremental = Incremental(self.0);

        let mut result = intersect.collect(&incremental)?;
        for batch in intersect.collect_list(&incremental)? {
            result = result.merge(batch);
        }

        Ok(result)
    }

    fn collect_diff<T, L, R>(&self, diff: &Diff<T, L, R>) -> Result<Tuples<T>>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        let mut elements = Elements::new();
        diff.visit(&mut elements);

        for r in elements.relations() {
            self.0.recalculate_relation(&r)?;
        }

        for r in elements.views() {
            self.0.recalculate_view(&r)?;
        }

        let incremental = Incremental(self.0);

        let mut result = diff.collect(&incremental)?;
        for batch in diff.collect_list(&incremental)? {
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

    #[test]
    fn test_evaluate_singleton() {
        {
            let database = Database::new();
            let s = Singleton(42);
            let result = database.evaluate(&s).unwrap();
            assert_eq!(Tuples::from(vec![42]), result);
        }
    }
    #[test]
    fn test_evaluate_relation() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            r.insert(vec![1, 2, 3].into(), &database).unwrap();
            let result = database.evaluate(&r).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
        {
            let database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.add_relation::<i32>("r");

            assert!(database.evaluate(&r).is_err());
        }
    }
    #[test]
    fn test_evaluate_project() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let project = Project::new(&r, |t| t * 10);

            let result = database.evaluate(&project).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let project = Project::new(&r, |t| t * 10);
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();

            let result = database.evaluate(&project).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![10, 20, 30, 40]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let p1 = Project::new(&r, |t| t * 10);
            let p2 = Project::new(&p1, |t| t + 1);

            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();

            let result = database.evaluate(&p2).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![11, 21, 31, 41]), result);
        }
        {
            let database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.add_relation::<i32>("r");
            let project = Project::new(&r, |t| t + 1);
            assert!(database.evaluate(&project).is_err());
        }
    }
    #[test]
    fn test_evaluate_select() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let project = Select::new(&r, |t| t % 2 == 1);

            let result = database.evaluate(&project).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let database = Database::new();
            let s = Singleton(42);
            let select = Select::new(&s, |t| t % 2 == 0);

            let result = database.evaluate(&select).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let select = Select::new(&r, |t| t % 2 == 0);
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();

            let result = database.evaluate(&select).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![2, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let p1 = Select::new(&r, |t| t % 2 == 0);
            let p2 = Select::new(&p1, |&t| t > 3);

            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();

            let result = database.evaluate(&p2).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4]), result);
        }
        {
            let database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.add_relation::<i32>("r");
            let select = Select::new(&r, |&t| t > 1);
            assert!(database.evaluate(&select).is_err());
        }
    }
    #[test]
    fn test_evaluate_join() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r");
            let s = database.add_relation::<(i32, i32)>("s");
            let join = Join::new(&r, &s, |_, &l, &r| (l, r));

            let result = database.evaluate(&join).unwrap();
            assert_eq!(Tuples::<(i32, i32)>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r");
            let s = database.add_relation::<(i32, i32)>("s");
            let join = Join::new(&r, &s, |_, &l, &r| (l, r));
            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            let result = database.evaluate(&join).unwrap();
            assert_eq!(Tuples::<(i32, i32)>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r");
            let s1 = Singleton((1, 2));
            let s2 = Singleton((3, 5));
            let r_s1 = Join::new(&r, &s1, |_, &l, &r| (l, r));
            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            database.evaluate(&r_s1).unwrap(); // materialize the first view
            let r_s1_s2 = Join::new(&r_s1, &s2, |_, &l, &r| (l, r));
            let result = database.evaluate(&r_s1_s2).unwrap();
            assert_eq!(Tuples::from(vec![(2, 5)]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r");
            let s = database.add_relation::<(i32, i32)>("s");
            let join = Join::new(&r, &s, |_, &l, &r| (l, r));
            s.insert(vec![(1, 5), (3, 2), (1, 6)].into(), &database)
                .unwrap();

            let result = database.evaluate(&join).unwrap();
            assert_eq!(Tuples::<(i32, i32)>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r");
            let s = database.add_relation::<(i32, i32)>("s");
            let join = Join::new(&r, &s, |_, &l, &r| (l, r));
            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            s.insert(vec![(1, 5), (3, 2), (1, 6)].into(), &database)
                .unwrap();

            let result = database.evaluate(&join).unwrap();
            assert_eq!(
                Tuples::<(i32, i32)>::from(vec![(3, 5), (3, 6), (4, 5), (4, 6)]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r");
            let s = database.add_relation::<(i32, i32)>("s");
            let t = database.add_relation::<(i32, i32)>("t");
            let r_s = Join::new(&r, &s, |_, &l, &r| (l, r));
            let r_s_t = Join::new(&r_s, &t, |_, _, &r| r);

            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            s.insert(vec![(1, 5), (3, 2), (1, 6)].into(), &database)
                .unwrap();
            t.insert(vec![(1, 40), (2, 41), (3, 42), (4, 43)].into(), &database)
                .unwrap();

            let result = database.evaluate(&r_s_t).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42, 43]), result);
        }
        {
            let mut database = Database::new();
            let mut dummy = Database::new();
            let r = database.add_relation::<(i32, i32)>("r");
            let s = dummy.add_relation::<(i32, i32)>("s");
            let join = Join::new(&r, &s, |_, &l, &r| (l, r));
            assert!(database.evaluate(&join).is_err());
        }
    }
    #[test]
    fn test_evaluate_union() {
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
    #[test]
    fn test_evaluate_intersect() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            let u = Intersect::new(&r, &s);

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            r.insert(vec![1, 2, 3].into(), &database).unwrap();
            let u = Intersect::new(&r, &s);

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            s.insert(vec![4, 5].into(), &database).unwrap();
            let u = Intersect::new(&r, &s);

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }

        {
            let database = Database::new();
            let r = Singleton(42);
            let s = Singleton(43);
            let u = Intersect::new(&r, &s);

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            let u = Intersect::new(&r, &s);
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();
            s.insert(vec![0, 4, 2, 6].into(), &database).unwrap();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![2, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            let t = database.add_relation::<i32>("t");
            let u1 = Intersect::new(&r, &s);
            let u2 = Intersect::new(&u1, &t);

            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();
            s.insert(vec![100, 4, 2].into(), &database).unwrap();
            t.insert(vec![40, 2, 4, 100].into(), &database).unwrap();

            let result = database.evaluate(&u2).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![2, 4]), result);
        }
        {
            let mut database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            let u = Intersect::new(&r, &s);
            assert!(database.evaluate(&u).is_err());
        }
    }
    #[test]
    fn test_evaluate_diff() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            let u = Diff::new(&r, &s);

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            r.insert(vec![1, 2, 3].into(), &database).unwrap();
            let u = Diff::new(&r, &s);

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            s.insert(vec![4, 5].into(), &database).unwrap();
            let u = Diff::new(&r, &s);

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }

        {
            let database = Database::new();
            let r = Singleton(42);
            let s = Singleton(43);
            let u = Diff::new(&r, &s);

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            let u = Diff::new(&r, &s);
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();
            s.insert(vec![0, 4, 2, 6].into(), &database).unwrap();

            let result = database.evaluate(&u).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 3]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            let t = database.add_relation::<i32>("t");
            let u1 = Diff::new(&r, &s);
            let u2 = Diff::new(&u1, &t);

            r.insert(vec![1, 2, 3, 4, 5].into(), &database).unwrap();
            s.insert(vec![100, 4, 2].into(), &database).unwrap();
            t.insert(vec![1, 2, 4, 100].into(), &database).unwrap();

            let result = database.evaluate(&u2).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![3, 5]), result);
        }
        {
            let mut database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.add_relation::<i32>("r");
            let s = database.add_relation::<i32>("s");
            let u = Diff::new(&r, &s);
            assert!(database.evaluate(&u).is_err());
        }
    }
    #[test]
    fn test_evaluate_view() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let v = database.store_view(&r);
            r.insert(vec![1, 2, 3].into(), &database).unwrap();
            let result = database.evaluate(&v).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let v_1 = database.store_view(&r);
            let v_2 = database.store_view(&v_1);
            let v_3 = database.store_view(&v_2);
            r.insert(vec![1, 2, 3].into(), &database).unwrap();
            let result = database.evaluate(&v_3).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r");
            let s = database.add_relation::<(i32, i32)>("s");
            let r_s = Join::new(&r, &s, |_, &l, &r| (l, r));
            let view = database.store_view(&r_s);

            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            s.insert(vec![(1, 5), (3, 2), (1, 6)].into(), &database)
                .unwrap();

            let result = database.evaluate(&view).unwrap();
            assert_eq!(
                Tuples::<(i32, i32)>::from(vec![(3, 5), (3, 6), (4, 5), (4, 6)]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r");
            let s = database.add_relation::<(i32, i32)>("s");
            let r_s = Join::new(&r, &s, |_, &l, &r| (l, r));
            let view = database.store_view(&r_s);

            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            s.insert(vec![(1, 5), (3, 2), (1, 6)].into(), &database)
                .unwrap();

            database.evaluate(&view).unwrap();
            s.insert(vec![(1, 7)].into(), &database).unwrap();
            let result = database.evaluate(&view).unwrap();
            assert_eq!(
                Tuples::<(i32, i32)>::from(vec![(3, 5), (3, 6), (3, 7), (4, 5), (4, 6), (4, 7)]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r");
            let s = database.add_relation::<(i32, i32)>("s");
            let t = database.add_relation::<(i32, i32)>("t");
            let r_s = Join::new(&r, &s, |_, &l, &r| (l, r));
            let r_s_t = Join::new(&r_s, &t, |_, _, &r| r);
            let view = database.store_view(&r_s_t);

            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            s.insert(vec![(1, 5), (3, 2), (1, 6)].into(), &database)
                .unwrap();
            t.insert(vec![(1, 40), (2, 41), (3, 42), (4, 43)].into(), &database)
                .unwrap();

            let result = database.evaluate(&view).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42, 43]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r");
            let s = database.add_relation::<(i32, i32)>("s");
            let t = database.add_relation::<(i32, i32)>("t");
            let rs = Union::new(&r, &s);
            let rs_t = Join::new(&rs, &t, |_, &l, &r| l * r);
            let view = database.store_view(&rs_t);

            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            s.insert(vec![(1, 5), (3, 2), (1, 6)].into(), &database)
                .unwrap();
            t.insert(vec![(1, 40), (2, 41), (3, 42), (4, 43)].into(), &database)
                .unwrap();

            let result = database.evaluate(&view).unwrap();
            assert_eq!(
                Tuples::<i32>::from(vec![82, 84, 120, 160, 200, 240]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r");
            let s = database.add_relation::<(i32, i32)>("s");
            let t = database.add_relation::<(i32, i32)>("t");
            let rs = Intersect::new(&r, &s);
            let rs_t = Join::new(&rs, &t, |_, &l, &r| l * r);
            let view = database.store_view(&rs_t);

            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            s.insert(vec![(1, 4), (3, 2), (2, 2)].into(), &database)
                .unwrap();
            t.insert(vec![(1, 40), (2, 41), (3, 42), (4, 43)].into(), &database)
                .unwrap();

            let result = database.evaluate(&view).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![82, 160]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<(i32, i32)>("r");
            let s = database.add_relation::<(i32, i32)>("s");
            let t = database.add_relation::<(i32, i32)>("t");
            let rs = Diff::new(&r, &s);
            let rs_t = Join::new(&rs, &t, |_, &l, &r| l * r);
            let view = database.store_view(&rs_t);

            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            s.insert(vec![(1, 4), (3, 2), (1, 6)].into(), &database)
                .unwrap();
            t.insert(vec![(1, 40), (2, 41), (3, 42), (4, 43)].into(), &database)
                .unwrap();

            let result = database.evaluate(&view).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![82, 120]), result);
        }
    }
}
