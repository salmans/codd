use super::{Expression, Visitor};
use crate::{database::Tuples, expression::Error, Tuple};
use std::{cell::RefCell, rc::Rc};

#[derive(Clone)]
pub struct Join<K, L, R, Left, Right, T>
where
    K: Tuple,
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
{
    left: Left,
    right: Right,
    left_key: Rc<RefCell<dyn FnMut(&L) -> K>>,
    right_key: Rc<RefCell<dyn FnMut(&R) -> K>>,
    joiner: Rc<RefCell<dyn FnMut(&K, &L, &R) -> T>>,
}

impl<K, L, R, Left, Right, T> Join<K, L, R, Left, Right, T>
where
    K: Tuple,
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
{
    pub fn new(
        left: &Left,
        right: &Right,
        left_key: impl FnMut(&L) -> K + 'static,
        right_key: impl FnMut(&R) -> K + 'static,
        joiner: impl FnMut(&K, &L, &R) -> T + 'static,
    ) -> Self {
        Self {
            left: left.clone(),
            right: right.clone(),
            left_key: Rc::new(RefCell::new(left_key)),
            right_key: Rc::new(RefCell::new(right_key)),
            joiner: Rc::new(RefCell::new(joiner)),
        }
    }

    pub fn left(&self) -> &Left {
        &self.left
    }

    pub fn right(&self) -> &Right {
        &self.right
    }

    pub fn left_key(&self) -> &Rc<RefCell<dyn FnMut(&L) -> K>> {
        &self.left_key
    }

    pub fn right_key(&self) -> &Rc<RefCell<dyn FnMut(&R) -> K>> {
        &self.right_key
    }

    pub fn mapper(&self) -> &Rc<RefCell<dyn FnMut(&K, &L, &R) -> T>> {
        &self.joiner
    }
}

impl<K, L, R, Left, Right, T> Expression<T> for Join<K, L, R, Left, Right, T>
where
    K: Tuple,
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
        visitor.visit_join(&self);
    }

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
    where
        C: super::Collector,
    {
        collector.collect_join(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
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
        database.insert(&r, vec![(1, 10)].into()).unwrap();
        database.insert(&s, vec![(1, 100)].into()).unwrap();
        let v = Join::new(&r, &s, |t| t.0, |t| t.0, |_, &l, &r| (l.1, r.1)).clone();
        assert_eq!(
            Tuples::<(i32, i32)>::from(vec![(10, 100)]),
            database.evaluate(&v).unwrap()
        );
    }
}
