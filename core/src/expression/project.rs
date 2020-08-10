use super::{Expression, Visitor};
use crate::{database::Tuples, Tuple};
use anyhow::Result;
use std::{cell::RefCell, rc::Rc};

#[derive(Clone)]
pub struct Project<S, T, E>
where
    S: Tuple,
    T: Tuple,
    E: Expression<S>,
{
    expression: E,
    mapper: Rc<RefCell<dyn FnMut(&S) -> T>>,
}

impl<S, T, E> Project<S, T, E>
where
    S: Tuple,
    T: Tuple,
    E: Expression<S>,
{
    pub fn new(expression: &E, project: impl FnMut(&S) -> T + 'static) -> Self {
        Self {
            expression: expression.clone(),
            mapper: Rc::new(RefCell::new(project)),
        }
    }

    pub fn expression(&self) -> &E {
        &self.expression
    }

    pub(crate) fn mapper(&self) -> &Rc<RefCell<dyn FnMut(&S) -> T>> {
        &self.mapper
    }
}

impl<S, T, E> Expression<T> for Project<S, T, E>
where
    S: Tuple,
    T: Tuple,
    E: Expression<S>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_project(&self);
    }

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>>
    where
        C: super::Collector,
    {
        collector.collect_project(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>>
    where
        C: super::ListCollector,
    {
        collector.collect_project(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

    #[test]
    fn test_clone_project() {
        let mut database = Database::new();
        let r = database.add_relation::<i32>("r");
        r.insert(vec![1, 2, 3].into(), &database).unwrap();
        let p = Project::new(&r, |&t| t * 10).clone();
        assert_eq!(
            Tuples::<i32>::from(vec![10, 20, 30]),
            database.evaluate(&p).unwrap()
        );
    }

    #[test]
    fn test_evaluate_project() {
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let project = Project::new(&r, |t| t * 10);

            let result = database.evaluate(&project).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let project = Project::new(&r, |t| t * 10);
            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();

            let result = database.evaluate(&project).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![10, 20, 30, 40]), result);
        }
        {
            let mut database = Database::new();
            let r = database.add_relation::<i32>("r");
            let p1 = Project::new(&r, |t| t * 10);
            let p2 = Project::new(&p1, |t| t + 1);

            r.insert(vec![1, 2, 3, 4].into(), &database).unwrap();

            let result = database.evaluate(&p2).unwrap();
            assert_eq!(Tuples::<i32>::from(vec![11, 21, 31, 41]), result);
        }
        {
            let database = Database::new();
            let mut dummy = Database::new();
            let r = dummy.add_relation::<i32>("r");
            let project = Project::new(&r, |t| t + 1);
            assert!(database.evaluate(&project).is_err());
        }
    }
}
