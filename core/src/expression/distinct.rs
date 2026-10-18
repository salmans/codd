use super::{view::ViewRef, Expression, IntoExpression, Visitor};
use crate::Tuple;
use std::marker::PhantomData;

/// Keeps every tuple of the underlying sub-expression once, regardless of how many times it
/// is derived. Views keep a derivation count per tuple, so `Distinct` is what turns a union
/// or a projection back into a set when the multiplicity matters downstream.
///
/// **Example**:
/// ```rust
/// use codd::{Database, expression::{Distinct, Project}};
///
/// let mut db = Database::new();
/// let r = db.add_relation::<(i32, i32)>("r").unwrap();
/// db.insert(&r, vec![(1, 10), (1, 20), (2, 30)].into()).unwrap();
///
/// let firsts = Project::new(r.clone(), |t: &(i32, i32)| t.0);
/// let distinct = db.store_view(Distinct::new(firsts)).unwrap();
///
/// assert_eq!(vec![1, 2], db.evaluate(&distinct).unwrap().into_tuples());
/// db.retract(&r, vec![(1, 10)].into()).unwrap();
/// assert_eq!(vec![1, 2], db.evaluate(&distinct).unwrap().into_tuples());
/// db.retract(&r, vec![(1, 20)].into()).unwrap();
/// assert_eq!(vec![2], db.evaluate(&distinct).unwrap().into_tuples());
/// ```
#[derive(Clone)]
pub struct Distinct<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    expression: E,
    relation_deps: Vec<String>,
    view_deps: Vec<ViewRef>,
    _marker: PhantomData<T>,
}

impl<T, E> Distinct<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    /// Creates a new [`Distinct`] expression over `expression`.
    pub fn new<I>(expression: I) -> Self
    where
        I: IntoExpression<T, E>,
    {
        use super::dependency;
        let expression = expression.into_expression();

        let mut deps = dependency::DependencyVisitor::new();
        expression.visit(&mut deps);
        let (relation_deps, view_deps) = deps.into_dependencies();

        Self {
            expression,
            relation_deps: relation_deps.into_iter().collect(),
            view_deps: view_deps.into_iter().collect(),
            _marker: PhantomData,
        }
    }

    /// Returns a reference to the underlying sub-expression.
    #[inline(always)]
    pub fn expression(&self) -> &E {
        &self.expression
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

impl<T, E> Expression<T> for Distinct<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_distinct(self);
    }
}

impl<T, E> std::fmt::Debug for Distinct<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Distinct")
            .field("expression", &self.expression)
            .finish()
    }
}
