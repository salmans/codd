use super::{Expression, Visitor};
use crate::{database::Tuples, Tuple};
use anyhow::Result;
use std::marker::PhantomData;

#[derive(Clone)]
pub struct Relation<T>
where
    T: Tuple,
{
    pub(crate) name: String,
    _phantom: PhantomData<T>,
}

impl<T> Relation<T>
where
    T: Tuple,
{
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            _phantom: PhantomData,
        }
    }
}

impl<T> Expression<T> for Relation<T>
where
    T: Tuple,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_relation(&self);
    }

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>>
    where
        C: super::Collector,
    {
        collector.collect_relation(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>>
    where
        C: super::ListCollector,
    {
        collector.collect_relation(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;

    #[test]
    fn test_new() {
        assert_eq!("a".to_string(), Relation::<i32>::new("a").name);
    }

    #[test]
    fn test_clone() {
        let mut database = Database::new();
        let r = database.add_relation::<i32>("r");
        database.insert(&r, vec![1, 2, 3].into()).unwrap();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 2, 3]),
            database.evaluate(&r.clone()).unwrap()
        );
    }
}
