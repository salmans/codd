use super::{Expression, Visitor};
use crate::{
    database::{Database, Tuples},
    Tuple,
};
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

    pub fn insert(&self, tuples: Tuples<T>, db: &Database) -> Result<()> {
        let relation = db.relation_instance(&self)?;
        relation.insert(tuples);
        Ok(())
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

    #[test]
    fn test_new() {
        assert_eq!("a".to_string(), Relation::<i32>::new("a").name);
    }

    #[test]
    fn test_insert() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            assert!(r.insert(vec![1, 2, 3].into(), &database).is_ok());
            assert_eq!(
                Tuples::<i32>::from(vec![1, 2, 3]),
                database.relation_instance(&r).unwrap().to_add.borrow()[0]
            );
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            assert!(r.insert(vec![1, 2, 3].into(), &database).is_ok());
            assert!(r.insert(vec![1, 4].into(), &database).is_ok());
            assert_eq!(
                Tuples::<i32>::from(vec![1, 2, 3]),
                database.relation_instance(&r).unwrap().to_add.borrow()[0]
            );
            assert_eq!(
                Tuples::<i32>::from(vec![1, 4]),
                database.relation_instance(&r).unwrap().to_add.borrow()[1]
            );
        }
        {
            let database = Database::new();
            let r = Database::new().add_relation("r"); // dummy database
            assert!(r.insert(vec![1, 2, 3].into(), &database).is_err());
        }
    }

    #[test]
    fn test_clone() {
        let mut database = Database::new();
        let r = database.add_relation::<i32>("r");
        r.insert(vec![1, 2, 3].into(), &database).unwrap();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 2, 3]),
            database.evaluate(&r.clone()).unwrap()
        );
    }
}
