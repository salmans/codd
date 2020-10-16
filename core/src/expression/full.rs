use super::{Builder, Expression, Visitor};
use crate::Tuple;
use std::marker::PhantomData;

/// Is a placeholder for a "full" instance, containing *all* tuples of type `T`.
///
/// **Note**: because `Full` expression cannot be described by a range-restricted
/// (see [chapter 2] of Foundations of Databases) query, any query containing
/// `Full` as a subexpression cannot be evaluated in a database safely.
///
/// **Example**:
/// ```rust
/// use codd::{Database, Full};
///
/// let mut db = Database::new();
/// let full = Full::<i32>::new();
///
/// assert!(db.evaluate(&full).is_err()); // cannot be evaluated
/// ```
///
/// [chapter 2]: http://webdam.inria.fr/Alice/pdfs/Chapter-5.pdf
#[derive(Clone, Debug)]
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
    /// Creates a new instance of `Full`.
    pub fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }

    pub fn builder(&self) -> Builder<T, Self> {
        Builder::from(self.clone())
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
}
