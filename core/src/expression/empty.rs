use super::{Builder, Expression, Visitor};
use crate::Tuple;
use std::marker::PhantomData;

/// Represents an empty instance containing no tuples.
///
/// **Example**:
/// ```rust
/// use codd::{Database, Empty};
///
/// let mut db = Database::new();
/// let empty = Empty::<i32>::new();
///
/// assert_eq!(Vec::<i32>::new(), db.evaluate(&empty).unwrap().into_tuples());
/// ```
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
    /// Creates a new instance of `Empty`.
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    pub fn builder(&self) -> Builder<T, Self> {
        Builder::from(self.clone())
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
}
