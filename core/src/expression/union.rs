use super::{view::ViewRef, Expression, IntoExpression, Visitor};
use crate::Tuple;
use std::marker::PhantomData;

/// Evaluates to the union of the tuples in its `left` and `right` sub-expressions.
///
/// **Example**:
/// ```rust
/// use codd::{Database, expression::Union};
///
/// let mut db = Database::new();
/// let r = db.add_relation::<i32>("R").unwrap();
/// let s = db.add_relation::<i32>("S").unwrap();
///
/// db.insert(&r, vec![0, 1, 2].into());
/// db.insert(&s, vec![2, 4].into());
///
/// let union = Union::new(&r, &s);
///
/// assert_eq!(vec![0, 1, 2, 4], db.evaluate(&union).unwrap().into_tuples());
/// ```
#[derive(Clone, Debug)]
pub struct Union<T, L, R>
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

impl<T, L, R> Union<T, L, R>
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
{
    /// Creates a new instance of [`Union`] corresponding to `left ∪ right`.
    pub fn new<IL, IR>(left: IL, right: IR) -> Self
    where
        IL: IntoExpression<T, L>,
        IR: IntoExpression<T, R>,
    {
        use super::dependency;
        let left = left.into_expression();
        let right = right.into_expression();

        let mut deps = dependency::DependencyVisitor::new();
        left.visit(&mut deps);
        right.visit(&mut deps);
        let (relation_deps, view_deps) = deps.into_dependencies();

        Self {
            left,
            right,
            relation_deps: relation_deps.into_iter().collect(),
            view_deps: view_deps.into_iter().collect(),
            _marker: PhantomData,
        }
    }

    /// Returns a reference to the left sub-expression.
    #[inline(always)]
    pub fn left(&self) -> &L {
        &self.left
    }

    /// Returns a reference to the right sub-expression.
    #[inline(always)]
    pub fn right(&self) -> &R {
        &self.right
    }

    /// Returns a reference to the relation dependencies of the receiver.
    #[inline(always)]
    pub(crate) fn relation_deps(&self) -> &[String] {
        &self.relation_deps
    }

    /// Returns a reference to the view dependencies of the receiver.
    #[inline(always)]
    pub(crate) fn view_deps(&self) -> &[ViewRef] {
        &self.view_deps
    }
}

impl<T, L, R> Expression<T> for Union<T, L, R>
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_union(&self);
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
        database.insert(&s, vec![4, 5].into()).unwrap();
        let u = Union::new(&r, &s).clone();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 2, 3, 4, 5]),
            database.evaluate(&u).unwrap()
        );
    }
}
