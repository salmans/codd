use super::{Expression, Visitor};
use crate::{Tuple, Tuples};
use anyhow::Result;
use std::{cell::RefCell, rc::Rc};

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

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>>
    where
        C: super::Collector,
    {
        collector.collect_select(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>>
    where
        C: super::ListCollector,
    {
        collector.collect_select(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Database, Singleton};

    #[test]
    fn test_clone_select() {
        let mut database = Database::new();
        let r = database.add_relation::<i32>("r");
        r.insert(vec![1, 2, 3].into(), &database).unwrap();
        let p = Select::new(&r, |&t| t % 2 == 1).clone();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 3]),
            database.evaluate(&p).unwrap()
        );
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
}
