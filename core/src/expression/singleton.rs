use super::Expression;
use crate::Tuple;

#[derive(Clone)]
pub struct Singleton<T>(pub(crate) T)
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

    fn collect<C>(&self, collector: &C) -> anyhow::Result<crate::Tuples<T>>
    where
        C: super::Collector,
    {
        collector.collect_singleton(&self)
    }

    fn collect_list<C>(&self, collector: &C) -> anyhow::Result<Vec<crate::Tuples<T>>>
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
    fn test_new_singleton() {
        assert_eq!(42, Singleton::<i32>(42).0);
    }

    #[test]
    fn test_clone_singleton() {
        let s = Singleton(42);
        assert_eq!(42, s.clone().0);
    }
}
