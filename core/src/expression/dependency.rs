use crate::{
    expression::{view::ViewRef, Expression, Relation, View, Visitor},
    Tuple,
};
use std::collections::HashSet;

/// Implements the [`Visitor`] to collect the relations and views to which
/// the visited expression depends.
struct DependencyVisitor {
    relations: HashSet<String>,
    views: HashSet<ViewRef>,
}

impl DependencyVisitor {
    /// Creates a new [`DependencyVisitor`].
    fn new() -> Self {
        Self {
            relations: HashSet::new(),
            views: HashSet::new(),
        }
    }

    /// Consumes the reciever and returns a pair of relation and view dependencies.
    fn into_dependencies(self) -> (HashSet<String>, HashSet<ViewRef>) {
        (self.relations, self.views)
    }
}

impl Visitor for DependencyVisitor {
    fn visit_relation<T>(&mut self, relation: &Relation<T>)
    where
        T: Tuple,
    {
        self.relations.insert(relation.name().into());
    }

    fn visit_view<T, E>(&mut self, view: &View<T, E>)
    where
        T: Tuple,
        E: Expression<T>,
    {
        self.views.insert(view.reference().clone());
    }
}

/// Returns the relations and views that `expression` reads.
pub(crate) fn expression_dependencies<T, E>(expression: &E) -> (HashSet<String>, HashSet<ViewRef>)
where
    T: Tuple,
    E: Expression<T>,
{
    let mut deps = DependencyVisitor::new();
    expression.visit(&mut deps);

    deps.into_dependencies()
}

/// Returns the relations and views that the operands of a binary expression read.
pub(crate) fn binary_dependencies<L, R, Left, Right>(
    left: &Left,
    right: &Right,
) -> (HashSet<String>, HashSet<ViewRef>)
where
    L: Tuple,
    R: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
{
    let mut deps = DependencyVisitor::new();
    left.visit(&mut deps);
    right.visit(&mut deps);

    deps.into_dependencies()
}
