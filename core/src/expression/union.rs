use super::{Collector, Expression, ListCollector, Visitor};
use crate::{Error, Tuple, Tuples};
use std::marker::PhantomData;

#[derive(Clone)]
pub struct Union<T, L, R>
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
{
    left: L,
    right: R,
    _marker: PhantomData<T>,
}

impl<T, L, R> Union<T, L, R>
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
{
    pub fn new(left: &L, right: &R) -> Self {
        Self {
            left: left.clone(),
            right: right.clone(),
            _marker: PhantomData,
        }
    }

    pub fn left(&self) -> &L {
        &self.left
    }

    pub fn right(&self) -> &R {
        &self.right
    }
}

impl<T, L, R> Expression<T> for Union<T, L, R>
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_union(&self);
    }

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
    where
        C: Collector,
    {
        collector.collect_union(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
    where
        C: ListCollector,
    {
        collector.collect_union(&self)
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
        let s = database.add_relation::<i32>("s").unwrap();
        database.insert(&r, vec![1, 2, 3].into()).unwrap();
        database.insert(&s, vec![4, 5].into()).unwrap();
        let u = Union::new(&r, &s).clone();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 2, 3, 4, 5]),
            database.evaluate(&u).unwrap()
        );
    }
}
