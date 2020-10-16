use super::{Expression, Visitor};
use crate::Tuple;
use std::marker::PhantomData;

/// Is the type of the view identifiers in a database.
#[derive(PartialEq, Eq, Clone, Hash, Debug)]
pub struct ViewRef(pub(crate) i32);

/// Represents a view in the database.
///
/// **Example**:
/// ```rust
/// use codd::{Database, expression::{Product, View}};
///
/// let mut db = Database::new();
/// let dividends = db.add_relation("dividends").unwrap();
/// let divisors = db.add_relation("divisors").unwrap();
///
/// db.insert(&dividends, vec![6, 12, 18].into()).unwrap();
/// db.insert(&divisors, vec![2, 3].into()).unwrap();
///
/// // divide all elements of `dividends` by all elements of `divisors`:
/// let quotients = Product::new(
///     dividends.clone(),
///     divisors.clone(),
///     |&l, &r| l/r
/// );
/// let view = db.store_view(quotients.clone()).unwrap();
///
/// // `view` and `quotients` evaluate to the same result:
/// assert_eq!(vec![2, 3, 4, 6, 9], db.evaluate(&quotients).unwrap().into_tuples());
/// assert_eq!(vec![2, 3, 4, 6, 9], db.evaluate(&view).unwrap().into_tuples());
///
/// db.insert(&dividends, vec![24, 30].into());
/// db.insert(&divisors, vec![1].into());
///
/// // the view gets updated automatically:
/// assert_eq!(
///    vec![2, 3, 4, 6, 8, 9, 10, 12, 15, 18, 24, 30],
///    db.evaluate(&view).unwrap().into_tuples()
/// );
///
/// use codd::expression::Difference;
/// // incremental view update for `Difference` is currently not supported:
/// assert!(db.store_view(Difference::new(dividends, divisors)).is_err());
/// ```
#[derive(Clone, Debug)]
pub struct View<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    reference: ViewRef,
    view_deps: Vec<ViewRef>,
    _phantom: PhantomData<(T, E)>,
}

impl<T, E> View<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    /// Creates a new view with a given reference.
    pub(crate) fn new(reference: ViewRef) -> Self {
        Self {
            view_deps: vec![reference.clone()],
            reference,
            _phantom: PhantomData,
        }
    }

    /// Returns the reference of this view.
    #[inline(always)]
    pub(crate) fn reference(&self) -> &ViewRef {
        &self.reference
    }

    /// Returns a reference to view dependencies of the receiver.
    #[inline(always)]
    pub(crate) fn view_deps(&self) -> &[ViewRef] {
        &self.view_deps
    }
}

impl<T, E> Expression<T> for View<T, E>
where
    T: Tuple + 'static,
    E: Expression<T> + 'static,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_view(&self);
    }
}

#[cfg(test)]
mod tests {
    use crate::{Database, Tuples};

    #[test]
    fn test_clone() {
        let mut database = Database::new();
        let r = database.add_relation::<i32>("r").unwrap();
        let v = database.store_view(r.clone()).unwrap().clone();
        database.insert(&r, vec![1, 2, 3].into()).unwrap();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 2, 3]),
            database.evaluate(&v).unwrap()
        );
    }
}
