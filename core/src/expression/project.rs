use super::{view::ViewRef, Expression, IntoExpression, Visitor};
use crate::Tuple;
use std::{
    cell::{RefCell, RefMut},
    marker::PhantomData,
    rc::Rc,
};

/// Projects the inner expression of type `S` to an expression of type `T`.
///
/// **Example**:
/// ```rust
/// use codd::{Database, expression::Project};
///
/// let mut db = Database::new();
/// let fruit = db.add_relation::<String>("R").unwrap();
///
/// db.insert(&fruit, vec!["Apple".to_string(), "BANANA".to_string(), "cherry".to_string()].into());
///
/// let lower = Project::new(
///     &fruit,
///     |t| t.to_lowercase(), // projecting closure
/// );
///
/// assert_eq!(vec!["apple", "banana", "cherry"], db.evaluate(&lower).unwrap().into_tuples());
/// ```
#[derive(Clone)]
pub struct Project<S, T, E>
where
    S: Tuple,
    T: Tuple,
    E: Expression<S>,
{
    expression: E,
    mapper: Rc<RefCell<dyn FnMut(&S) -> T>>,
    relation_deps: Vec<String>,
    view_deps: Vec<ViewRef>,
}

impl<S, T, E> Project<S, T, E>
where
    S: Tuple,
    T: Tuple,
    E: Expression<S>,
{
    /// Creates a new `Project` expression over `expression` with a closure `mapper` that
    /// projects tuples of `expression` to the resulting tuples.
    pub fn new<I>(expression: I, mapper: impl FnMut(&S) -> T + 'static) -> Self
    where
        I: IntoExpression<S, E>,
    {
        use super::dependency;
        let expression = expression.into_expression();

        let mut deps = dependency::DependencyVisitor::new();
        expression.visit(&mut deps);
        let (relation_deps, view_deps) = deps.into_dependencies();

        Self {
            expression: expression.clone(),
            mapper: Rc::new(RefCell::new(mapper)),
            relation_deps: relation_deps.into_iter().collect(),
            view_deps: view_deps.into_iter().collect(),
        }
    }

    /// Returns a reference to the underlying expression.
    #[inline(always)]
    pub fn expression(&self) -> &E {
        &self.expression
    }

    /// Returns a mutable reference (of type `std::cell::RefMut`) to the projecting closure.
    #[inline(always)]
    pub(crate) fn mapper_mut(&self) -> RefMut<dyn FnMut(&S) -> T> {
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

impl<S, T, E> Expression<T> for Project<S, T, E>
where
    S: Tuple,
    T: Tuple,
    E: Expression<S>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_project(&self);
    }
}

// A hack:
#[derive(Debug)]
struct Debuggable<S, E>
where
    S: Tuple,
    E: Expression<S>,
{
    expression: E,
    _marker: PhantomData<S>,
}

impl<S, T, E> std::fmt::Debug for Project<S, T, E>
where
    S: Tuple,
    T: Tuple,
    E: Expression<S>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debuggable {
            expression: self.expression.clone(),
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
        database.insert(&r, vec![1, 2, 3].into()).unwrap();
        let p = Project::new(&r, |&t| t * 10).clone();
        assert_eq!(
            Tuples::<i32>::from(vec![10, 20, 30]),
            database.evaluate(&p).unwrap()
        );
    }
}
