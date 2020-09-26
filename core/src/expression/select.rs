use super::{Expression, Visitor};
use crate::{expression::Error, Tuple, Tuples};
use std::{cell::RefCell, marker::PhantomData, rc::Rc};

#[derive(Clone)]
pub struct Select<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    expression: E,
    predicate: Rc<RefCell<dyn FnMut(&T) -> bool>>,
}

impl<T, E> Select<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    pub fn new<P>(expression: &E, predicate: P) -> Self
    where
        P: FnMut(&T) -> bool + 'static,
    {
        Self {
            expression: expression.clone(),
            predicate: Rc::new(RefCell::new(predicate)),
        }
    }

    pub fn expression(&self) -> &E {
        &self.expression
    }

    pub(crate) fn predicate(&self) -> &Rc<RefCell<dyn FnMut(&T) -> bool>> {
        &self.predicate
    }
}

impl<T, E> Expression<T> for Select<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_select(&self);
    }

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
    where
        C: super::Collector,
    {
        collector.collect_select(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
    where
        C: super::ListCollector,
    {
        collector.collect_select(&self)
    }
}

#[derive(Debug)]
struct Debuggable<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    expression: E,
    _marker: PhantomData<T>,
}

impl<T, E> std::fmt::Debug for Select<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debuggable {
            expression: self.expression.clone(),
            _marker: PhantomData,
        }
        .fmt(f)
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
        database.insert(&r, vec![1, 2, 3].into()).unwrap();
        let p = Select::new(&r, |&t| t % 2 == 1).clone();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 3]),
            database.evaluate(&p).unwrap()
        );
    }
}
