use super::{Expression, Visitor};
use crate::{
    database::{Tuples, ViewRef},
    expression::Error,
    Tuple,
};
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

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
    where
        C: super::Collector,
    {
        collector.collect_view(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
    where
        C: super::ListCollector,
    {
        collector.collect_view(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

    #[test]
    fn test_clone() {
        let mut database = Database::new();
        let r = database.add_relation::<i32>("r").unwrap();
        let v = database.store_view(&r).clone();
        database.insert(&r, vec![1, 2, 3].into()).unwrap();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 2, 3]),
            database.evaluate(&v).unwrap()
        );
    }
}
