use super::{view::ViewRef, Expression, Visitor};
use crate::Tuple;
use std::marker::PhantomData;

/// Evaluates to all tuples that are in both `left` and `right` expressions.
///
/// **Example**:
/// ```rust
/// use codd::{Database, Intersect};
///
/// let mut db = Database::new();
/// let r = db.add_relation::<i32>("R").unwrap();
/// let s = db.add_relation::<i32>("S").unwrap();
///
/// db.insert(&r, vec![0, 1, 2].into());
/// db.insert(&s, vec![2, 4].into());
///
/// let intersect = Intersect::new(&r, &s);
///
/// assert_eq!(vec![2], db.evaluate(&intersect).unwrap().into_tuples());
/// ```
#[derive(Clone, Debug)]
pub struct Intersect<T, L, R>
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
{
    left: L,
    right: R,
    relation_deps: Vec<String>,
    view_deps: Vec<ViewRef>,
    _marker: PhantomData<T>,
}

impl<T, L, R> Intersect<T, L, R>
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
{
    /// Creates a new instance of `Intersect` for `left ∩ right`.
    pub fn new(left: &L, right: &R) -> Self {
        use super::dependency;

        let mut deps = dependency::DependencyVisitor::new();
        left.visit(&mut deps);
        right.visit(&mut deps);
        let (relation_deps, view_deps) = deps.into_dependencies();

        Self {
            left: left.clone(),
            right: right.clone(),
            relation_deps: relation_deps.into_iter().collect(),
            view_deps: view_deps.into_iter().collect(),
            _marker: PhantomData,
        }
    }

    /// Returns a reference to the expression on left.
    #[inline(always)]
    pub fn left(&self) -> &L {
        &self.left
    }

    /// Returns a reference to the expression on right.
    #[inline(always)]
    pub fn right(&self) -> &R {
        &self.right
    }

    /// Returns a reference to relation dependencies of the receiver.
    #[inline(always)]
    pub(crate) fn relation_deps(&self) -> &[String] {
        &self.relation_deps
    }

    /// Returns a reference to view dependencies of the receiver.
    #[inline(always)]
    pub(crate) fn view_deps(&self) -> &[ViewRef] {
        &self.view_deps
    }
}

impl<T, L, R> Expression<T> for Intersect<T, L, R>
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_intersect(&self);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Database, Tuples};

    #[test]
    fn test_clone() {
        let mut database = Database::new();
        let r = database.add_relation::<i32>("r").unwrap();
        let s = database.add_relation::<i32>("s").unwrap();
        database.insert(&r, vec![1, 2, 3].into()).unwrap();
        database.insert(&s, vec![1, 4, 3, 5].into()).unwrap();
        let u = Intersect::new(&r, &s).clone();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 3]),
            database.evaluate(&u).unwrap()
        );
    }
}
