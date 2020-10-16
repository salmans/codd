use super::{view::ViewRef, Expression, IntoExpression, Visitor};
use crate::Tuple;
use std::{
    cell::{RefCell, RefMut},
    marker::PhantomData,
    rc::Rc,
};

/// Is the join of `left` and `right` expressions.
///
/// **Example**:
/// ```rust
/// use codd::{Database, expression::Join};
///
/// let mut db = Database::new();
/// let fruit = db.add_relation::<(i32, String)>("R").unwrap();
/// let numbers = db.add_relation::<i32>("S").unwrap();
///
/// db.insert(&fruit, vec![
///    (0, "Apple".to_string()),
///    (1, "Banana".to_string()),
///    (2, "Cherry".to_string())
/// ].into());
/// db.insert(&numbers, vec![0, 2].into());
///
/// let join = Join::new(
///     &fruit,
///     &numbers,
///     |t| t.0,  // first element of tuples in `r` is the key for join
///     |&t| t,   // the values in `s` are keys for join
///     // make resulting values from key `k`, left value `l` and right value `r`:
///     |k, l, r| format!("{}{}", l.1, k + r)
/// );
///
/// assert_eq!(vec!["Apple0", "Cherry4"], db.evaluate(&join).unwrap().into_tuples());
/// ```
#[derive(Clone)]
pub struct Join<K, L, R, Left, Right, T>
where
    K: Tuple,
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
{
    left: Left,
    right: Right,
    left_key: Rc<RefCell<dyn FnMut(&L) -> K>>,
    right_key: Rc<RefCell<dyn FnMut(&R) -> K>>,
    mapper: Rc<RefCell<dyn FnMut(&K, &L, &R) -> T>>,
    relation_deps: Vec<String>,
    view_deps: Vec<ViewRef>,
}

impl<K, L, R, Left, Right, T> Join<K, L, R, Left, Right, T>
where
    K: Tuple,
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
{
    /// Creates a new `Join` expression over `left` and `right` where `left_key`
    /// and `right_key` are closures that return the join key for tuples of
    /// `left` and `right` respectively. The closure `mapper` computes the tuples
    /// of the resulting expression from the join keys and the tuples of `left` and
    /// `right`.
    pub fn new<IL, IR>(
        left: IL,
        right: IR,
        left_key: impl FnMut(&L) -> K + 'static,
        right_key: impl FnMut(&R) -> K + 'static,
        mapper: impl FnMut(&K, &L, &R) -> T + 'static,
    ) -> Self
    where
        IL: IntoExpression<L, Left>,
        IR: IntoExpression<R, Right>,
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
            left_key: Rc::new(RefCell::new(left_key)),
            right_key: Rc::new(RefCell::new(right_key)),
            mapper: Rc::new(RefCell::new(mapper)),
            relation_deps: relation_deps.into_iter().collect(),
            view_deps: view_deps.into_iter().collect(),
        }
    }

    /// Returns a reference to the expression on left.
    #[inline(always)]
    pub fn left(&self) -> &Left {
        &self.left
    }

    /// Returns a reference to the expression on right.
    #[inline(always)]
    pub fn right(&self) -> &Right {
        &self.right
    }

    /// Returns a mutable reference (of type `RefMut`) of the key closure for
    /// the left expression.
    #[inline(always)]
    pub(crate) fn left_key_mut(&self) -> RefMut<dyn FnMut(&L) -> K> {
        self.left_key.borrow_mut()
    }

    /// Returns a mutable reference (of type `RefMut`) of the key closure for
    /// the right expression.
    #[inline(always)]
    pub(crate) fn right_key_mut(&self) -> RefMut<dyn FnMut(&R) -> K> {
        self.right_key.borrow_mut()
    }

    /// Returns a mutable reference (of type `std::cell::RefMut`) to the joining closure.
    #[inline(always)]
    pub(crate) fn mapper_mut(&self) -> RefMut<dyn FnMut(&K, &L, &R) -> T> {
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
}

impl<K, L, R, Left, Right, T> Expression<T> for Join<K, L, R, Left, Right, T>
where
    K: Tuple,
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
        visitor.visit_join(&self);
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

impl<K, L, R, Left, Right, T> std::fmt::Debug for Join<K, L, R, Left, Right, T>
where
    K: Tuple,
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
        let r = database.add_relation::<(i32, i32)>("r").unwrap();
        let s = database.add_relation::<(i32, i32)>("s").unwrap();
        database.insert(&r, vec![(1, 10)].into()).unwrap();
        database.insert(&s, vec![(1, 100)].into()).unwrap();
        let v = Join::new(&r, &s, |t| t.0, |t| t.0, |_, &l, &r| (l.1, r.1)).clone();
        assert_eq!(
            Tuples::<(i32, i32)>::from(vec![(10, 100)]),
            database.evaluate(&v).unwrap()
        );
    }
}
