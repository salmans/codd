use super::{Expression, Visitor};
use crate::{
    database::{Tuples, ViewRef},
    Tuple,
};
use anyhow::Result;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct View<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    pub(crate) reference: ViewRef,
    _phantom: PhantomData<(T, E)>,
}

impl<T, E> View<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    pub(crate) fn new(reference: ViewRef) -> Self {
        Self {
            reference,
            _phantom: PhantomData,
        }
    }
}

impl<T, E> Expression<T> for View<T, E>
where
    T: Tuple + 'static,
    E: Expression<T> + 'static,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_view(&self);
    }

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>>
    where
        C: super::Collector,
    {
        collector.collect_view(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>>
    where
        C: super::ListCollector,
    {
        collector.collect_view(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{Join, Union};
    use super::*;
    use crate::Database;

    #[test]
    fn test_clone_view() {
        let mut database = Database::new();
        let r = database.add_relation::<i32>("r");
        let v = database.store_view(&r).clone();
        r.insert(vec![1, 2, 3].into(), &database).unwrap();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 2, 3]),
            database.evaluate(&v).unwrap()
        );
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
    }
}
