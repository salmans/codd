use super::{Expression, Visitor};
use crate::{database::Tuples, expression::Error, Tuple};
use std::marker::PhantomData;

#[derive(Clone)]
pub struct Full<T>
where
    T: Tuple,
{
    _phantom: PhantomData<T>,
}

impl<T> Full<T>
where
    T: Tuple,
{
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T> Expression<T> for Full<T>
where
    T: Tuple,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_full(&self);
    }

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
    where
        C: super::Collector,
    {
        collector.collect_full(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
    where
        C: super::ListCollector,
    {
        collector.collect_full(&self)
    }
}
