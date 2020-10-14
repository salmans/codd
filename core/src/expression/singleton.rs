use super::Expression;
use crate::Tuple;

/// Represents a single tuple of type `T`.
///
/// **Example**:
/// ```rust
/// use codd::{Database, Singleton};
///
/// let mut db = Database::new();
/// let hello = Singleton::new("Hello".to_string());
///
/// assert_eq!(vec!["Hello".to_string()], db.evaluate(&hello).unwrap().into_tuples());
/// ```
#[derive(Clone, Debug)]
pub struct Singleton<T>(T)
where
    T: Tuple;

impl<T: Tuple> Singleton<T> {
    /// Create a new instance of `Singleton` with `tuple` as its inner value.
    pub fn new(tuple: T) -> Self {
        Self(tuple)
    }

    /// Returns the inner value of the receiver.
    #[inline(always)]
    pub fn tuple(&self) -> &T {
        &self.0
    }

    /// Consumes the receiver and returns its inner value.
    #[inline(always)]
    pub fn into_tuple(self) -> T {
        self.0
    }
}

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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        assert_eq!(42, Singleton::new(42).into_tuple());
    }

    #[test]
    fn test_clone() {
        let s = Singleton::new(42);
        assert_eq!(42, s.clone().into_tuple());
    }
}
