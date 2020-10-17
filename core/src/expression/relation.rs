use super::{Expression, Visitor};
use crate::Tuple;
use std::marker::PhantomData;

/// Is an expression that points to a relation with tuples of type `T` that identified
/// by a relation `name`.
///
/// **Example**:
/// ```rust
/// use codd::{Database, expression::Relation};
///
/// let mut db = Database::new();
/// let r = db.add_relation("R").unwrap();
///
/// db.insert(&r, vec![0, 1, 2, 3].into()).unwrap(); // insert into the relation instance
///
/// assert_eq!(vec![0, 1, 2, 3], db.evaluate(&r).unwrap().into_tuples());
/// ```
#[derive(Clone, Debug)]
pub struct Relation<T>
where
    T: Tuple,
{
    name: String,
    relation_deps: Vec<String>,
    _phantom: PhantomData<T>,
}

impl<T> Relation<T>
where
    T: Tuple,
{
    /// Creates a new `Relation` with a given `name`.
    pub fn new<S>(name: S) -> Self
    where
        S: Into<String>,
    {
        let name = name.into();
        Self {
            relation_deps: vec![name.clone()],
            name,
            _phantom: PhantomData,
        }
    }

    /// Returns the name of this relation.
    #[inline(always)]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns a reference to relation dependencies of the receiver.
    #[inline(always)]
    pub(crate) fn relation_deps(&self) -> &[String] {
        &self.relation_deps
    }
}

impl<T> Expression<T> for Relation<T>
where
    T: Tuple + 'static,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_relation(&self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Database, Tuples};

    #[test]
    fn test_new() {
        assert_eq!("a".to_string(), Relation::<i32>::new("a").name);
    }

    #[test]
    fn test_clone() {
        let mut database = Database::new();
        let r = database.add_relation::<i32>("r").unwrap();
        database.insert(&r, vec![1, 2, 3].into()).unwrap();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 2, 3]),
            database.evaluate(&r.clone()).unwrap()
        );
    }
}
