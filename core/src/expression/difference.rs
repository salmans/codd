use super::{view::ViewRef, Expression, IntoExpression, Visitor};
use crate::Tuple;
use std::marker::PhantomData;

/// Evaluates to all tuples that are in `left` but not in `right` (`left - right`).
///
/// **Example**:
/// ```rust
/// use codd::{Database, expression::Difference};
///
/// let mut db = Database::new();
/// let r = db.add_relation::<i32>("R").unwrap();
/// let s = db.add_relation::<i32>("S").unwrap();
///
/// db.insert(&r, vec![0, 1, 2].into());
/// db.insert(&s, vec![2, 4].into());
///
/// let r_s = Difference::new(&r, &s);
/// let s_r = Difference::new(&s, &r);
///
/// assert_eq!(vec![0, 1], db.evaluate(&r_s).unwrap().into_tuples());
/// assert_eq!(vec![4], db.evaluate(&s_r).unwrap().into_tuples());
/// ```
#[derive(Clone, Debug)]
pub struct Difference<T, L, R>
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

impl<T, L, R> Difference<T, L, R>
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
{
    /// Creates a new instance of `Difference` corresponding to
    /// `left` - `right`.
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

impl<T, L, R> Expression<T> for Difference<T, L, R>
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_difference(&self);
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
        database.insert(&r, vec![1, 2, 3, 6].into()).unwrap();
        database.insert(&s, vec![1, 4, 3, 5].into()).unwrap();
        let u = Difference::new(&r, &s).clone();
        assert_eq!(
            Tuples::<i32>::from(vec![2, 6]),
            database.evaluate(&u).unwrap()
        );
    }
}
