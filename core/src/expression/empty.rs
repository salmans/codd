use super::{Expression, Visitor};
use crate::{database::Tuples, expression::Error, Tuple};
use std::marker::PhantomData;

#[derive(Clone, Debug)]
pub struct Empty<T>
where
    T: Tuple,
{
    _phantom: PhantomData<T>,
}

impl<T> Empty<T>
where
    T: Tuple,
{
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

impl<T> Expression<T> for Empty<T>
where
    T: Tuple,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_empty(&self);
    }

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
    where
        C: super::Collector,
    {
        collector.collect_empty(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
    where
        C: super::ListCollector,
    {
        collector.collect_empty(&self)
    }
}
