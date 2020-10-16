/*! Defines relational algebraic expressions as generic over [`Tuple`] types and
can be evaluated in [`Database`].

[`Tuple`]: ../trait.Tuple.html
[`Database`]: ./database/struct.Database.html
*/
mod builder;
pub(crate) mod dependency;
mod difference;
mod empty;
mod full;
mod intersect;
mod join;
mod mono;
mod product;
mod project;
mod relation;
mod select;
mod singleton;
mod union;
pub(crate) mod view;

use crate::Tuple;
pub use builder::Builder;
pub use difference::Difference;
pub use empty::Empty;
pub use full::Full;
pub use intersect::Intersect;
pub use join::Join;
pub use mono::Mono;
pub use product::Product;
pub use project::Project;
pub use relation::Relation;
pub use select::Select;
pub use singleton::Singleton;
pub use union::Union;
pub use view::View;

/// Is the trait of expressions in relational algebra that can be evaluated in
/// a database.
pub trait Expression<T: Tuple>: Clone + std::fmt::Debug {
    /// Visits this node by a [`Visitor`].
    ///
    /// [`Visitor`]: ./trait.Visitor.html
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor;

    fn builder(&self) -> Builder<T, Self> {
        Builder::from(self.clone())
    }
}

impl<T, E> Expression<T> for &E
where
    T: Tuple,
    E: Expression<T>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        (*self).visit(visitor)
    }
}

impl<T, E> Expression<T> for Box<E>
where
    T: Tuple,
    E: Expression<T>,
{
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        (**self).visit(visitor)
    }
}

/// Is the trait of types that can be turned into an [`Expression`].
///
/// [`Expression`]: ./trait.Expression.html
pub trait IntoExpression<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    /// Consumes the receiver and returns an expression.
    fn into_expression(self) -> E;
}

impl<T, E> IntoExpression<T, E> for E
where
    T: Tuple,
    E: Expression<T>,
{
    fn into_expression(self) -> E {
        self
    }
}

/// Is the trait of objects that visit [`Expression`]s. The default implementation guides
/// the visitor through all subexpressions of the expressions that is visited.
///
/// [`Expression`]: ./trait.Expression.html
pub trait Visitor: Sized {
    /// Visits the `Full` expression.
    fn visit_full<T>(&mut self, full: &Full<T>)
    where
        T: Tuple,
    {
        walk_full(self, full)
    }

    /// Visits the `Empty` expression.
    fn visit_empty<T>(&mut self, empty: &Empty<T>)
    where
        T: Tuple,
    {
        walk_empty(self, empty)
    }

    /// Visits a `Singlenton` expression.
    fn visit_singleton<T>(&mut self, singleton: &Singleton<T>)
    where
        T: Tuple,
    {
        walk_singlenton(self, singleton)
    }

    /// Visits a `Relation` expression.
    fn visit_relation<T>(&mut self, relation: &Relation<T>)
    where
        T: Tuple,
    {
        walk_relation(self, relation)
    }

    /// Visits a `Select` expression.
    fn visit_select<T, E>(&mut self, select: &Select<T, E>)
    where
        T: Tuple,
        E: Expression<T>,
    {
        walk_select(self, select);
    }

    /// Visits a `Union` expression.    
    fn visit_union<T, L, R>(&mut self, union: &Union<T, L, R>)
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        walk_union(self, union);
    }

    /// Visits an `Intersect` expression.    
    fn visit_intersect<T, L, R>(&mut self, intersect: &Intersect<T, L, R>)
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        walk_intersect(self, intersect);
    }

    /// Visits a `Difference` expression.    
    fn visit_difference<T, L, R>(&mut self, difference: &Difference<T, L, R>)
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        walk_difference(self, difference);
    }

    /// Visits a `Project` expression.    
    fn visit_project<S, T, E>(&mut self, project: &Project<S, T, E>)
    where
        T: Tuple,
        S: Tuple,
        E: Expression<S>,
    {
        walk_project(self, project);
    }

    /// Visits a `Product` expression.    
    fn visit_product<L, R, Left, Right, T>(&mut self, product: &Product<L, R, Left, Right, T>)
    where
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: Expression<L>,
        Right: Expression<R>,
    {
        walk_product(self, product);
    }

    /// Visits a `Join` expression.    
    fn visit_join<K, L, R, Left, Right, T>(&mut self, join: &Join<K, L, R, Left, Right, T>)
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: Expression<L>,
        Right: Expression<R>,
    {
        walk_join(self, join);
    }

    /// Visits a `View` expression.    
    fn visit_view<T, E>(&mut self, view: &View<T, E>)
    where
        T: Tuple,
        E: Expression<T>,
    {
        walk_view(self, view);
    }
}

fn walk_full<T, V>(_: &mut V, _: &Full<T>)
where
    T: Tuple,
    V: Visitor,
{
    // nothing to do
}

fn walk_empty<T, V>(_: &mut V, _: &Empty<T>)
where
    T: Tuple,
    V: Visitor,
{
    // nothing to do
}

fn walk_singlenton<T, V>(_: &mut V, _: &Singleton<T>)
where
    T: Tuple,
    V: Visitor,
{
    // nothing to do
}

fn walk_relation<T, V>(_: &mut V, _: &Relation<T>)
where
    T: Tuple,
    V: Visitor,
{
    // nothing to do
}

fn walk_select<T, E, V>(visitor: &mut V, select: &Select<T, E>)
where
    T: Tuple,
    E: Expression<T>,
    V: Visitor,
{
    select.expression().visit(visitor);
}

fn walk_union<T, L, R, V>(visitor: &mut V, union: &Union<T, L, R>)
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
    V: Visitor,
{
    union.left().visit(visitor);
    union.right().visit(visitor);
}

fn walk_intersect<T, L, R, V>(visitor: &mut V, intersect: &Intersect<T, L, R>)
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
    V: Visitor,
{
    intersect.left().visit(visitor);
    intersect.right().visit(visitor);
}

fn walk_difference<T, L, R, V>(visitor: &mut V, difference: &Difference<T, L, R>)
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
    V: Visitor,
{
    difference.left().visit(visitor);
    difference.right().visit(visitor);
}

fn walk_project<S, T, E, V>(visitor: &mut V, project: &Project<S, T, E>)
where
    T: Tuple,
    S: Tuple,
    E: Expression<S>,
    V: Visitor,
{
    project.expression().visit(visitor);
}

fn walk_product<L, R, Left, Right, T, V>(visitor: &mut V, product: &Product<L, R, Left, Right, T>)
where
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
    V: Visitor,
{
    product.left().visit(visitor);
    product.right().visit(visitor);
}

fn walk_join<K, L, R, Left, Right, T, V>(visitor: &mut V, join: &Join<K, L, R, Left, Right, T>)
where
    K: Tuple,
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
    V: Visitor,
{
    join.left().visit(visitor);
    join.right().visit(visitor);
}

fn walk_view<T, E, V>(_: &mut V, _: &View<T, E>)
where
    T: Tuple,
    E: Expression<T>,
    V: Visitor,
{
    // nothing to do
}
