use super::{Expression, Visitor};
use crate::{database::Tuples, Tuple};
use anyhow::Result;
use std::{cell::RefCell, rc::Rc};

#[derive(Clone)]
pub struct Join<K, L, R, Left, Right, T>
where
    K: Tuple,
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<(K, L)>,
    Right: Expression<(K, R)>,
{
    left: Left,
    right: Right,
    mapper: Rc<RefCell<dyn FnMut(&K, &L, &R) -> T>>,
}

impl<K, L, R, Left, Right, T> Join<K, L, R, Left, Right, T>
where
    K: Tuple,
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<(K, L)>,
    Right: Expression<(K, R)>,
{
    pub fn new(left: &Left, right: &Right, project: impl FnMut(&K, &L, &R) -> T + 'static) -> Self {
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

    pub fn mapper(&self) -> &Rc<RefCell<dyn FnMut(&K, &L, &R) -> T>> {
        &self.mapper
    }
}

impl<K, L, R, Left, Right, T> Expression<T> for Join<K, L, R, Left, Right, T>
where
    K: Tuple,
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<(K, L)>,
    Right: Expression<(K, R)>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_join(&self);
    }

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>>
    where
        C: super::Collector,
    {
        collector.collect_join(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>>
    where
        C: super::ListCollector,
    {
        collector.collect_join(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Database;

    #[test]
    fn test_clone() {
        let mut database = Database::new();
        let r = database.add_relation::<(i32, i32)>("r");
        let s = database.add_relation::<(i32, i32)>("s");
        r.insert(vec![(1, 10)].into(), &database).unwrap();
        s.insert(vec![(1, 100)].into(), &database).unwrap();
        let v = Join::new(&r, &s, |_, &l, &r| (l, r)).clone();
        assert_eq!(
            Tuples::<(i32, i32)>::from(vec![(10, 100)]),
            database.evaluate(&v).unwrap()
        );
    }
}
