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
