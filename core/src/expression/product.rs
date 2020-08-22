use super::{Expression, Visitor};
use crate::{database::Tuples, Tuple};
use anyhow::Result;
use std::{cell::RefCell, rc::Rc};

#[derive(Clone)]
pub struct Product<L, R, Left, Right, T>
where
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
{
    left: Left,
    right: Right,
    mapper: Rc<RefCell<dyn FnMut(&L, &R) -> T>>,
}

impl<L, R, Left, Right, T> Product<L, R, Left, Right, T>
where
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
{
    pub fn new(left: &Left, right: &Right, project: impl FnMut(&L, &R) -> T + 'static) -> Self {
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

    pub fn mapper(&self) -> &Rc<RefCell<dyn FnMut(&L, &R) -> T>> {
        &self.mapper
    }
}

impl<L, R, Left, Right, T> Expression<T> for Product<L, R, Left, Right, T>
where
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_product(&self);
    }

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>>
    where
        C: super::Collector,
    {
        collector.collect_product(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>>
    where
        C: super::ListCollector,
    {
        collector.collect_product(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

    #[test]
    fn test_clone() {
        let mut database = Database::new();
        let r = database.add_relation::<i32>("r");
        let s = database.add_relation::<i32>("s");
        database.insert(&r, vec![1, 10].into()).unwrap();
        database.insert(&s, vec![1, 100].into()).unwrap();
        let v = Product::new(&r, &s, |&l, &r| l + r).clone();
        assert_eq!(
            Tuples::from(vec![2, 11, 101, 110]),
            database.evaluate(&v).unwrap()
        );
    }
}
