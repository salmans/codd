use super::{Expression, Visitor};
use crate::{database::Tuples, Tuple};
use anyhow::Result;
use std::{cell::RefCell, rc::Rc};

#[derive(Clone)]
pub struct Join<K, L, R, Left, Right, T>
where
    K: Tuple,
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<(K, L)>,
    Right: Expression<(K, R)>,
{
    left: Left,
    right: Right,
    mapper: Rc<RefCell<dyn FnMut(&K, &L, &R) -> T>>,
}

impl<K, L, R, Left, Right, T> Join<K, L, R, Left, Right, T>
where
    K: Tuple,
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<(K, L)>,
    Right: Expression<(K, R)>,
{
    pub fn new(left: &Left, right: &Right, project: impl FnMut(&K, &L, &R) -> T + 'static) -> Self {
        Self {
            left: left.clone(),
            right: right.clone(),
            mapper: Rc::new(RefCell::new(project)),
        }
    }

    pub fn left(&self) -> &Left {
        &self.left
    }

    pub fn right(&self) -> &Right {
        &self.right
    }

    pub fn mapper(&self) -> &Rc<RefCell<dyn FnMut(&K, &L, &R) -> T>> {
        &self.mapper
    }
}

impl<K, L, R, Left, Right, T> Expression<T> for Join<K, L, R, Left, Right, T>
where
    K: Tuple,
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<(K, L)>,
    Right: Expression<(K, R)>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_join(&self);
    }

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>>
    where
        C: super::Collector,
    {
        collector.collect_join(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>>
    where
        C: super::ListCollector,
    {
        collector.collect_join(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

    #[test]
    fn test_clone_join() {
        let mut database = Database::new();
        let r = database.add_relation::<(i32, i32)>("r");
        let s = database.add_relation::<(i32, i32)>("s");
        r.insert(vec![(1, 10)].into(), &database).unwrap();
        s.insert(vec![(1, 100)].into(), &database).unwrap();
        let v = Join::new(&r, &s, |_, &l, &r| (l, r)).clone();
        assert_eq!(
            Tuples::<(i32, i32)>::from(vec![(10, 100)]),
            database.evaluate(&v).unwrap()
        );
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
}
