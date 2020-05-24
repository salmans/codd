use crate::{
    database::{Database, Table, Tuples, ViewRef},
    tools::project_helper,
    Tuple,
};
use anyhow::Result;
use std::marker::PhantomData;

pub trait Expression<T: Tuple> {
    fn evaluate(&self, db: &Database) -> Result<Tuples<T>>;

    fn update_to(&self, db: &Database, output: &Table<T>) -> Result<()>;

    fn duplicate(&self) -> Box<dyn Expression<T>>;
}

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
        db.relation(&self).map(|tbl| tbl.tuples())
    }

    fn update_to(&self, db: &Database, output: &Table<T>) -> Result<()> {
        let mut results = Vec::new();
        let relation = db.relation(&self)?;

        project_helper(&relation.recent.borrow(), |v| results.push(v.clone()));

        for batch in relation.stable.borrow().iter() {
            project_helper(&batch, |v| results.push(v.clone()));
        }

        output.insert(results.into());
        Ok(())
    }

    fn duplicate(&self) -> Box<dyn Expression<T>> {
        Box::new(Self::new(&self.name))
    }
}

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
        db.view(&self).map(|table| table.tuples())
    }

    fn update_to(&self, db: &Database, output: &Table<T>) -> Result<()> {
        let mut results = Vec::new();
        let table = &db.view(&self)?;

        project_helper(&table.recent.borrow(), |v| results.push(v.clone()));

        for batch in table.stable.borrow().iter() {
            project_helper(&batch, |v| results.push(v.clone()));
        }

        output.insert(results.into());
        Ok(())
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
            assert!(r.insert(vec![1, 2, 3].into(), &mut database).is_ok());
            assert_eq!(
                Tuples::<i32>::from(vec![1, 2, 3]),
                database.relation(&r).unwrap().to_add.borrow()[0]
            );
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            assert!(r.insert(vec![1, 2, 3].into(), &mut database).is_ok());
            assert!(r.insert(vec![1, 4].into(), &mut database).is_ok());
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
            let mut database = Database::new();
            let r = Database::new().new_relation("r"); // dummy database
            assert!(r.insert(vec![1, 2, 3].into(), &mut database).is_err());
        }
    }

    #[test]
    fn test_evaluate_relation() {
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            r.insert(vec![1, 2, 3].into(), &mut database).unwrap();
            let result = r.evaluate(&mut database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
    }

    #[test]
    fn test_evaluate_view() {
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let v = database.new_view(&r);
            r.insert(vec![1, 2, 3].into(), &mut database).unwrap();
            let result = v.evaluate(&mut database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
        {
            let mut database = Database::new();
            let r = database.new_relation::<i32>("r");
            let v_1 = database.new_view(&r);
            let v_2 = database.new_view(&v_1);
            let v_3 = database.new_view(&v_2);
            r.insert(vec![1, 2, 3].into(), &mut database).unwrap();
            let result = v_3.evaluate(&mut database).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![1, 2, 3]), result);
        }
    }
}
