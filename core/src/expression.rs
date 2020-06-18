use crate::{
    database::{Database, Tuples, ViewRef},
    tools::{join_helper, project_helper},
    Tuple,
};
use anyhow::Result;
use std::{cell::RefCell, marker::PhantomData, rc::Rc};

impl<T: Tuple, E: Expression<T>> Expression<T> for &E {
    fn evaluate(&self, db: &Database) -> Result<Tuples<T>> {
        (*self).evaluate(db)
    }
    fn recent_tuples(&self, db: &Database) -> Result<Tuples<T>> {
        (*self).recent_tuples(db)
    }
    fn stable_tuples(&self, db: &Database) -> Result<Vec<Tuples<T>>> {
        (*self).stable_tuples(db)
    }
    fn duplicate(&self) -> Box<dyn Expression<T>> {
        (*self).duplicate()
    }
}

pub trait Expression<T: Tuple> {
    fn evaluate(&self, db: &Database) -> Result<Tuples<T>>;

    fn recent_tuples(&self, db: &Database) -> Result<Tuples<T>>;

    fn stable_tuples(&self, db: &Database) -> Result<Vec<Tuples<T>>>;

    fn duplicate(&self) -> Box<dyn Expression<T>>;
}

#[derive(Clone)]
pub struct Relation<T: Tuple> {
    pub(crate) name: String,
    _phantom: PhantomData<T>,
}

impl<T: Tuple + 'static> Relation<T> {
    pub(crate) fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            _phantom: PhantomData,
        }
    }

    pub fn insert(&self, tuples: Tuples<T>, db: &Database) -> Result<()> {
        let relation = db.relation(&self)?;
        relation.insert(tuples);
        Ok(())
    }
}

impl<T: Tuple + 'static> Expression<T> for Relation<T> {
    fn evaluate(&self, db: &Database) -> Result<Tuples<T>> {
        db.update_views()?;
        let table = db.relation(&self)?;
        assert!(table.recent.borrow().is_empty());
        assert!(table.to_add.borrow().is_empty());

        let mut result = self.recent_tuples(&db)?;
        for batch in self.stable_tuples(&db)? {
            result = result.merge(batch);
        }

        Ok(result)
    }

    fn recent_tuples(&self, db: &Database) -> Result<Tuples<T>> {
        let table = db.relation(&self)?;
        Ok(table.recent.borrow().clone())
    }

    fn stable_tuples(&self, db: &Database) -> Result<Vec<Tuples<T>>> {
        let mut result = Vec::<Tuples<T>>::new();
        let table = db.relation(&self)?;
        for batch in table.stable.borrow().iter() {
            result.push(batch.clone());
        }
        Ok(result)
    }

    fn duplicate(&self) -> Box<dyn Expression<T>> {
        Box::new(Self::new(&self.name))
    }
}

pub struct Select<T>
where
    T: Tuple,
{
    expression: Box<dyn Expression<T>>,
    predicate: Rc<RefCell<dyn FnMut(&T) -> bool>>,
}

impl<T> Select<T>
where
    T: Tuple,
{
    pub fn new(
        expression: &impl Expression<T>,
        predicate: impl FnMut(&T) -> bool + 'static,
    ) -> Self {
        Self {
            expression: expression.duplicate(),
            predicate: Rc::new(RefCell::new(predicate)),
        }
    }
}

impl<T> Expression<T> for Select<T>
where
    T: Tuple + 'static,
{
    fn evaluate(&self, db: &Database) -> Result<Tuples<T>> {
        db.update_views()?;
        let mut result = self.recent_tuples(&db)?;
        for batch in self.stable_tuples(&db)? {
            result = result.merge(batch);
        }
        Ok(result)
    }
    fn recent_tuples(&self, db: &Database) -> Result<Tuples<T>> {
        let mut result = Vec::new();
        let recent = self.expression.recent_tuples(&db)?;
        let predicate = &mut (*self.predicate.borrow_mut());
        for tuple in &recent[..] {
            if predicate(tuple) {
                result.push(tuple.clone());
            }
        }
        Ok(result.into())
    }
    fn stable_tuples(&self, db: &Database) -> Result<Vec<Tuples<T>>> {
        let mut result = Vec::<Tuples<T>>::new();
        let stable = self.expression.stable_tuples(&db)?;
        let predicate = &mut (*self.predicate.borrow_mut());
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
    fn duplicate(&self) -> Box<dyn Expression<T>> {
        Box::new(Self {
            expression: self.expression.duplicate(),
            predicate: self.predicate.clone(),
        })
    }
}

pub struct Project<S, T>
where
    S: Tuple,
    T: Tuple,
{
    expression: Box<dyn Expression<S>>,
    mapper: Rc<RefCell<dyn FnMut(&S) -> T>>,
}

impl<S, T> Project<S, T>
where
    S: Tuple,
    T: Tuple,
{
    pub fn new(expression: &impl Expression<S>, project: impl FnMut(&S) -> T + 'static) -> Self {
        Self {
            expression: expression.duplicate(),
            mapper: Rc::new(RefCell::new(project)),
        }
    }
}

impl<S, T> Expression<T> for Project<S, T>
where
    S: Tuple + 'static,
    T: Tuple + 'static,
{
    fn evaluate(&self, db: &Database) -> Result<Tuples<T>> {
        db.update_views()?;
        let mut result = self.recent_tuples(&db)?;
        for batch in self.stable_tuples(&db)? {
            result = result.merge(batch);
        }
        Ok(result)
    }
    fn recent_tuples(&self, db: &Database) -> Result<Tuples<T>> {
        let mut result = Vec::new();
        let recent = self.expression.recent_tuples(&db)?;
        let mapper = &mut (*self.mapper.borrow_mut());
        project_helper(&recent, |t| result.push(mapper(t)));
        Ok(result.into())
    }
    fn stable_tuples(&self, db: &Database) -> Result<Vec<Tuples<T>>> {
        let mut result = Vec::<Tuples<T>>::new();
        let stable = self.expression.stable_tuples(&db)?;
        let mapper = &mut (*self.mapper.borrow_mut());
        for batch in stable.iter() {
            let mut tuples = Vec::new();
            project_helper(&batch, |t| tuples.push(mapper(t)));
            result.push(tuples.into());
        }
        Ok(result)
    }
    fn duplicate(&self) -> Box<dyn Expression<T>> {
        Box::new(Self {
            expression: self.expression.duplicate(),
            mapper: self.mapper.clone(),
        })
    }
}

pub struct Join<K, L, R, T>
where
    K: Tuple,
    L: Tuple,
    R: Tuple,
    T: Tuple,
{
    left: Box<dyn Expression<(K, L)>>,
    right: Box<dyn Expression<(K, R)>>,
    mapper: Rc<RefCell<dyn FnMut(&K, &L, &R) -> T>>,
}

impl<K, L, R, T> Join<K, L, R, T>
where
    K: Tuple,
    L: Tuple,
    R: Tuple,
    T: Tuple,
{
    pub fn new(
        left: &impl Expression<(K, L)>,
        right: &impl Expression<(K, R)>,
        project: impl FnMut(&K, &L, &R) -> T + 'static,
    ) -> Self {
        Self {
            left: left.duplicate(),
            right: right.duplicate(),
            mapper: Rc::new(RefCell::new(project)),
        }
    }
}

impl<K, L, R, T> Expression<T> for Join<K, L, R, T>
where
    K: Tuple + 'static,
    L: Tuple + 'static,
    R: Tuple + 'static,
    T: Tuple + 'static,
{
    fn evaluate(&self, db: &Database) -> Result<Tuples<T>> {
        db.update_views()?;

        let mut result = self.recent_tuples(&db)?;
        for batch in self.stable_tuples(&db)? {
            result = result.merge(batch);
        }

        Ok(result)
    }
    fn recent_tuples(&self, db: &Database) -> Result<Tuples<T>> {
        let mut result = Vec::new();
        let left_recent = self.left.recent_tuples(&db)?;
        let left_stable = self.left.stable_tuples(&db)?;
        let right_recent = self.right.recent_tuples(&db)?;
        let right_stable = self.right.stable_tuples(&db)?;

        let mapper = &mut (*self.mapper.borrow_mut());

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
    fn stable_tuples(&self, db: &Database) -> Result<Vec<Tuples<T>>> {
        // TODO do not clone recent and stable recursively.
        let mut result = Vec::<Tuples<T>>::new();
        let left = self.left.stable_tuples(&db)?;
        let right = self.right.stable_tuples(&db)?;

        let mapper = &mut (*self.mapper.borrow_mut());
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
    fn duplicate(&self) -> Box<dyn Expression<T>> {
        Box::new(Self {
            left: self.left.duplicate(),
            right: self.right.duplicate(),
            mapper: self.mapper.clone(),
        })
    }
}

#[derive(Clone)]
pub struct View<T: Tuple> {
    pub(crate) reference: ViewRef,
    _phantom: PhantomData<T>,
}

impl<T: Tuple + 'static> View<T> {
    pub(crate) fn new(reference: ViewRef) -> Self {
        Self {
            reference,
            _phantom: PhantomData,
        }
    }
}

impl<T: Tuple + 'static> Expression<T> for View<T> {
    fn evaluate(&self, db: &Database) -> Result<Tuples<T>> {
        db.update_views()?;
        let table = db.view(&self)?;
        assert!(table.recent.borrow().is_empty());
        assert!(table.to_add.borrow().is_empty());

        let mut result = self.recent_tuples(&db)?;
        for batch in self.stable_tuples(&db)? {
            result = result.merge(batch);
        }

        Ok(result)
    }

    fn recent_tuples(&self, db: &Database) -> Result<Tuples<T>> {
        let table = db.view(&self)?;
        Ok(table.recent.borrow().clone())
    }

    fn stable_tuples(&self, db: &Database) -> Result<Vec<Tuples<T>>> {
        let mut result = Vec::<Tuples<T>>::new();
        let table = db.view(&self)?;
        for batch in table.stable.borrow().iter() {
            result.push(batch.clone());
        }
        Ok(result)
    }

    fn duplicate(&self) -> Box<dyn Expression<T>> {
        Box::new(Self::new(self.reference.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_relation() {
        assert_eq!("a".to_string(), Relation::<i32>::new("a").name);
    }

    #[test]
    fn test_insert_relation() {
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            assert!(r.insert(vec![1, 2, 3].into(), &database).is_ok());
            assert_eq!(
                Tuples::<i32>::from(vec![1, 2, 3]),
                database.relation(&r).unwrap().to_add.borrow()[0]
            );
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            assert!(r.insert(vec![1, 2, 3].into(), &database).is_ok());
            assert!(r.insert(vec![1, 4].into(), &database).is_ok());
            assert_eq!(
                Tuples::<i32>::from(vec![1, 2, 3]),
                database.relation(&r).unwrap().to_add.borrow()[0]
            );
            assert_eq!(
                Tuples::<i32>::from(vec![1, 4]),
                database.relation(&r).unwrap().to_add.borrow()[1]
            );
        }
        {
            let database = Database::new();
            let r = Database::new().new_relation("r"); // dummy database
            assert!(r.insert(vec![1, 2, 3].into(), &database).is_err());
        }
    }

    #[test]
    fn test_duplicate_relation() {
        let mut database = Database::new();
        let r = database.new_relation::<i32>("r");
        r.insert(vec![1, 2, 3].into(), &database).unwrap();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 2, 3]),
            r.duplicate().evaluate(&database).unwrap()
        );
    }

    #[test]
    fn test_duplicate_select() {
        let mut database = Database::new();
        let r = database.new_relation::<i32>("r");
        r.insert(vec![1, 2, 3].into(), &database).unwrap();
        let p = Select::new(&r, |&t| t % 2 == 1).duplicate();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 3]),
            p.evaluate(&database).unwrap()
        );
    }

    #[test]
    fn test_duplicate_project() {
        let mut database = Database::new();
        let r = database.new_relation::<i32>("r");
        r.insert(vec![1, 2, 3].into(), &database).unwrap();
        let p = Project::new(&r, |&t| t * 10).duplicate();
        assert_eq!(
            Tuples::<i32>::from(vec![10, 20, 30]),
            p.evaluate(&database).unwrap()
        );
    }

    #[test]
    fn test_duplicate_join() {
        let mut database = Database::new();
        let r = database.new_relation::<(i32, i32)>("r");
        let s = database.new_relation::<(i32, i32)>("s");
        r.insert(vec![(1, 10)].into(), &database).unwrap();
        s.insert(vec![(1, 100)].into(), &database).unwrap();
        let v = Join::new(&r, &s, |_, &l, &r| (l, r)).duplicate();
        assert_eq!(
            Tuples::<(i32, i32)>::from(vec![(10, 100)]),
            v.evaluate(&database).unwrap()
        );
    }

    #[test]
    fn test_duplicate_view() {
        let mut database = Database::new();
        let r = database.new_relation::<i32>("r");
        let v = database.new_view(&r).duplicate();
        r.insert(vec![1, 2, 3].into(), &database).unwrap();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 2, 3]),
            v.evaluate(&database).unwrap()
        );
    }

    #[test]
    fn test_evaluate_relation() {
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            r.insert(vec![1, 2, 3].into(), &database).unwrap();
            let result = r.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
        {
            let database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.new_relation::<i32>("r");

            assert!(r.evaluate(&database).is_err());
        }
    }

    #[test]
    fn test_evaluate_select() {
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let project = Select::new(&r, |t| t % 2 == 1);

            let result = project.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let select = Select::new(&r, |t| t % 2 == 0);
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();

            let result = select.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![2, 4]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let p1 = Select::new(&r, |t| t % 2 == 0);
            let p2 = Select::new(&p1, |&t| t > 3);

            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();

            let result = p2.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![4]), result);
        }
        {
            let database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.new_relation::<i32>("r");
            let select = Select::new(&r, |&t| t > 1);
            assert!(select.evaluate(&database).is_err());
        }
    }

    #[test]
    fn test_evaluate_project() {
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let project = Project::new(&r, |t| t * 10);

            let result = project.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let project = Project::new(&r, |t| t * 10);
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();

            let result = project.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![10, 20, 30, 40]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let p1 = Project::new(&r, |t| t * 10);
            let p2 = Project::new(&p1, |t| t + 1);

            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();

            let result = p2.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![11, 21, 31, 41]), result);
        }
        {
            let database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.new_relation::<i32>("r");
            let project = Project::new(&r, |t| t + 1);
            assert!(project.evaluate(&database).is_err());
        }
    }

    #[test]
    fn test_evaluate_join() {
        {
            let mut database = Database::new();
            let r = database.new_relation::<(i32, i32)>("r");
            let s = database.new_relation::<(i32, i32)>("s");
            let join = Join::new(&r, &s, |_, &l, &r| (l, r));

            let result = join.evaluate(&database).unwrap();
            assert_eq!(Tuples::<(i32, i32)>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<(i32, i32)>("r");
            let s = database.new_relation::<(i32, i32)>("s");
            let join = Join::new(&r, &s, |_, &l, &r| (l, r));
            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            let result = join.evaluate(&database).unwrap();
            assert_eq!(Tuples::<(i32, i32)>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<(i32, i32)>("r");
            let s = database.new_relation::<(i32, i32)>("s");
            let join = Join::new(&r, &s, |_, &l, &r| (l, r));
            s.insert(vec![(1, 5), (3, 2), (1, 6)].into(), &database)
                .unwrap();

            let result = join.evaluate(&database).unwrap();
            assert_eq!(Tuples::<(i32, i32)>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<(i32, i32)>("r");
            let s = database.new_relation::<(i32, i32)>("s");
            let join = Join::new(&r, &s, |_, &l, &r| (l, r));
            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            s.insert(vec![(1, 5), (3, 2), (1, 6)].into(), &database)
                .unwrap();

            let result = join.evaluate(&database).unwrap();
            assert_eq!(
                Tuples::<(i32, i32)>::from(vec![(3, 5), (3, 6), (4, 5), (4, 6)]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<(i32, i32)>("r");
            let s = database.new_relation::<(i32, i32)>("s");
            let t = database.new_relation::<(i32, i32)>("t");
            let r_s = Join::new(&r, &s, |_, &l, &r| (l, r));
            let r_s_t = Join::new(&r_s, &t, |_, _, &r| r);

            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            s.insert(vec![(1, 5), (3, 2), (1, 6)].into(), &database)
                .unwrap();
            t.insert(vec![(1, 40), (2, 41), (3, 42), (4, 43)].into(), &database)
                .unwrap();

            let result = r_s_t.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42, 43]), result);
        }
        {
            let mut database = Database::new();
            let mut dummy = Database::new();
            let r = database.new_relation::<(i32, i32)>("r");
            let s = dummy.new_relation::<(i32, i32)>("s");
            let join = Join::new(&r, &s, |_, &l, &r| (l, r));
            assert!(join.evaluate(&database).is_err());
        }
    }

    #[test]
    fn test_evaluate_view() {
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let v = database.new_view(&r);
            r.insert(vec![1, 2, 3].into(), &database).unwrap();
            let result = v.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let v_1 = database.new_view(&r);
            let v_2 = database.new_view(&v_1);
            let v_3 = database.new_view(&v_2);
            r.insert(vec![1, 2, 3].into(), &database).unwrap();
            let result = v_3.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<(i32, i32)>("r");
            let s = database.new_relation::<(i32, i32)>("s");
            let r_s = Join::new(&r, &s, |_, &l, &r| (l, r));
            let view = database.new_view(&r_s);

            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            s.insert(vec![(1, 5), (3, 2), (1, 6)].into(), &database)
                .unwrap();

            let result = view.evaluate(&database).unwrap();
            assert_eq!(
                Tuples::<(i32, i32)>::from(vec![(3, 5), (3, 6), (4, 5), (4, 6)]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<(i32, i32)>("r");
            let s = database.new_relation::<(i32, i32)>("s");
            let r_s = Join::new(&r, &s, |_, &l, &r| (l, r));
            let view = database.new_view(&r_s);

            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            s.insert(vec![(1, 5), (3, 2), (1, 6)].into(), &database)
                .unwrap();

            view.evaluate(&database).unwrap();
            s.insert(vec![(1, 7)].into(), &database).unwrap();
            let result = view.evaluate(&database).unwrap();
            assert_eq!(
                Tuples::<(i32, i32)>::from(vec![(3, 5), (3, 6), (3, 7), (4, 5), (4, 6), (4, 7)]),
                result
            );
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<(i32, i32)>("r");
            let s = database.new_relation::<(i32, i32)>("s");
            let t = database.new_relation::<(i32, i32)>("t");
            let r_s = Join::new(&r, &s, |_, &l, &r| (l, r));
            let r_s_t = Join::new(&r_s, &t, |_, _, &r| r);
            let view = database.new_view(&r_s_t);

            r.insert(vec![(1, 4), (2, 2), (1, 3)].into(), &database)
                .unwrap();
            s.insert(vec![(1, 5), (3, 2), (1, 6)].into(), &database)
                .unwrap();
            t.insert(vec![(1, 40), (2, 41), (3, 42), (4, 43)].into(), &database)
                .unwrap();

            let result = view.evaluate(&database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![42, 43]), result);
        }
    }
}
