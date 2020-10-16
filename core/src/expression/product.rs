use super::{view::ViewRef, Builder, Expression, Visitor};
use crate::Tuple;
use std::{
    cell::{RefCell, RefMut},
    marker::PhantomData,
    rc::Rc,
};

/// Corresponds to the cartesian product of two expression.
///
/// **Example**:
/// ```rust
/// use codd::{Database, expression::Product};
///
/// let mut db = Database::new();
/// let r = db.add_relation::<i32>("R").unwrap();
/// let s = db.add_relation::<i32>("S").unwrap();
///
/// db.insert(&r, vec![0, 1, 2].into());
/// db.insert(&s, vec![2, 4].into());
///
/// let prod = Product::new(&r, &s, |l, r| l*r);
///
/// assert_eq!(vec![0, 2, 4, 8], db.evaluate(&prod).unwrap().into_tuples());
/// ```
#[derive(Clone)]
pub struct Product<L, R, Left, Right, T>
where
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
{
    left: Left,
    right: Right,
    mapper: Rc<RefCell<dyn FnMut(&L, &R) -> T>>,
    relation_deps: Vec<String>,
    view_deps: Vec<ViewRef>,
}

impl<L, R, Left, Right, T> Product<L, R, Left, Right, T>
where
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
{
    /// Creates a `Product` expression over `left` and `right` with `mapper` as the closure
    /// that produces the tuples of the resulting expression from tuples of `left` and `right`.
    pub fn new(left: &Left, right: &Right, project: impl FnMut(&L, &R) -> T + 'static) -> Self {
        use super::dependency;

        let mut deps = dependency::DependencyVisitor::new();
        left.visit(&mut deps);
        right.visit(&mut deps);
        let (relation_deps, view_deps) = deps.into_dependencies();

        Self {
            left: left.clone(),
            right: right.clone(),
            mapper: Rc::new(RefCell::new(project)),
            relation_deps: relation_deps.into_iter().collect(),
            view_deps: view_deps.into_iter().collect(),
        }
    }

    /// Returns a reference to the expression on left.
    #[inline(always)]
    pub(crate) fn left(&self) -> &Left {
        &self.left
    }

    /// Returns a reference to the expression on right.
    #[inline(always)]
    pub(crate) fn right(&self) -> &Right {
        &self.right
    }

    /// Returns a mutable reference (of type `std::cell::RefMut`) to the mapping closure.
    #[inline(always)]
    pub(crate) fn mapper_mut(&self) -> RefMut<dyn FnMut(&L, &R) -> T> {
        self.mapper.borrow_mut()
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

    pub fn builder(&self) -> Builder<T, Self> {
        Builder::from(self.clone())
    }
}

impl<L, R, Left, Right, T> Expression<T> for Product<L, R, Left, Right, T>
where
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_product(&self);
    }
}

// A hack for debugging purposes:
#[derive(Debug)]
struct Debuggable<L, R, Left, Right>
where
    L: Tuple,
    R: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
{
    left: Left,
    right: Right,
    _marker: PhantomData<(L, R)>,
}

impl<L, R, Left, Right, T> std::fmt::Debug for Product<L, R, Left, Right, T>
where
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debuggable {
            left: self.left.clone(),
            right: self.right.clone(),
            _marker: PhantomData,
        }
        .fmt(f)
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
        database.insert(&r, vec![1, 10].into()).unwrap();
        database.insert(&s, vec![1, 100].into()).unwrap();
        let v = Product::new(&r, &s, |&l, &r| l + r).clone();
        assert_eq!(
            Tuples::from(vec![2, 11, 101, 110]),
            database.evaluate(&v).unwrap()
        );
    }
}
