use super::{view::ViewRef, Expression, IntoExpression, Visitor};
use crate::Tuple;
use std::{marker::PhantomData, rc::Rc};

/// Selects tuples of the underlying sub-expression according to a given predicate.
///
/// **Example**:
/// ```rust
/// use codd::{Database, expression::Select};
///
/// let mut db = Database::new();
/// let fruit = db.add_relation::<String>("Fruit").unwrap();
///
/// db.insert(&fruit, vec!["Apple".to_string(), "BANANA".to_string(), "cherry".to_string()].into());
///
/// let select = Select::new(
///     &fruit,
///     |t| t.contains('A'), // select predicate
/// );
///
/// assert_eq!(vec!["Apple", "BANANA"], db.evaluate(&select).unwrap().into_tuples());
/// ```
#[derive(Clone)]
pub struct Select<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    expression: E,
    predicate: Rc<dyn Fn(&T) -> bool>,
    relation_deps: Vec<String>,
    view_deps: Vec<ViewRef>,
}

impl<T, E> Select<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    /// Creates a new [`Select`] expression over `expression` according to the `predicate` closure.
    pub fn new<I, P>(expression: I, predicate: P) -> Self
    where
        I: IntoExpression<T, E>,
        P: Fn(&T) -> bool + 'static,
    {
        use super::dependency;
        let expression = expression.into_expression();

        let (relation_deps, view_deps) = dependency::expression_dependencies(&expression);

        Self {
            expression,
            predicate: Rc::new(predicate),
            relation_deps: relation_deps.into_iter().collect(),
            view_deps: view_deps.into_iter().collect(),
        }
    }

    /// Returns a reference to the underlying sub-expression.
    #[inline(always)]
    pub fn expression(&self) -> &E {
        &self.expression
    }

    /// Returns the select predicate.
    #[inline(always)]
    pub(crate) fn predicate(&self) -> &dyn Fn(&T) -> bool {
        &*self.predicate
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

impl<T, E> Expression<T> for Select<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        visitor.visit_select(self);
    }
}

#[derive(Debug)]
#[allow(dead_code)] // fields exist only for the derived `Debug` output
struct Debuggable<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    expression: E,
    _marker: PhantomData<T>,
}

impl<T, E> std::fmt::Debug for Select<T, E>
where
    T: Tuple,
    E: Expression<T>,
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
        let p = Select::new(&r, |&t| t % 2 == 1).clone();
        assert_eq!(
            Tuples::<i32>::from(vec![1, 3]),
            database.evaluate(&p).unwrap()
        );
    }
}
