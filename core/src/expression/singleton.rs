use super::Expression;
use crate::{Error, Tuple};

#[derive(Clone, Debug)]
pub struct Singleton<T>(pub T)
where
    T: Tuple;

impl<T> Expression<T> for Singleton<T>
where
    T: Tuple,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: super::Visitor,
    {
        visitor.visit_singleton(&self)
    }

    fn collect<C>(&self, collector: &C) -> Result<crate::Tuples<T>, Error>
    where
        C: super::Collector,
    {
        collector.collect_singleton(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<crate::Tuples<T>>, Error>
    where
        C: super::ListCollector,
    {
        collector.collect_singleton(&self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        assert_eq!(42, Singleton::<i32>(42).0);
    }

    #[test]
    fn test_clone() {
        let s = Singleton(42);
        assert_eq!(42, s.clone().0);
    }
}
