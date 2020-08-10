mod join;
mod project;
mod relation;
mod select;
mod view;

use crate::{database::Tuples, Tuple};
use anyhow::Result;

pub use join::Join;
pub use project::Project;
pub use relation::Relation;
pub use select::Select;
pub use view::View;

pub trait Expression<T: Tuple>: Clone {
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor;

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>>
    where
        C: Collector;

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>>
    where
        C: ListCollector;
}

pub trait Visitor: Sized {
    fn visit_relation<T>(&mut self, relation: &Relation<T>)
    where
        T: Tuple,
    {
        walk_relation(self, relation)
    }

    fn visit_select<T, E>(&mut self, select: &Select<T, E>)
    where
        T: Tuple,
        E: Expression<T>,
    {
        walk_select(self, select);
    }

    fn visit_project<S, T, E>(&mut self, project: &Project<S, T, E>)
    where
        T: Tuple,
        S: Tuple,
        E: Expression<S>,
    {
        walk_project(self, project);
    }

    fn visit_join<K, L, R, Left, Right, T>(&mut self, join: &Join<K, L, R, Left, Right, T>)
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: Expression<(K, L)>,
        Right: Expression<(K, R)>,
    {
        walk_join(self, join);
    }

    fn visit_view<T, E>(&mut self, view: &View<T, E>)
    where
        T: Tuple,
        E: Expression<T>,
    {
        walk_view(self, view);
    }
}

pub fn walk_relation<T, V>(_: &mut V, _: &Relation<T>)
where
    T: Tuple,
    V: Visitor,
{
    // nothing to do
}

pub fn walk_select<T, E, V>(visitor: &mut V, select: &Select<T, E>)
where
    T: Tuple,
    E: Expression<T>,
    V: Visitor,
{
    select.expression().visit(visitor);
}

pub fn walk_project<S, T, E, V>(visitor: &mut V, project: &Project<S, T, E>)
where
    T: Tuple,
    S: Tuple,
    E: Expression<S>,
    V: Visitor,
{
    project.expression().visit(visitor);
}

pub fn walk_join<K, L, R, Left, Right, T, V>(visitor: &mut V, join: &Join<K, L, R, Left, Right, T>)
where
    K: Tuple,
    L: Tuple,
    R: Tuple,
    T: Tuple,
    Left: Expression<(K, L)>,
    Right: Expression<(K, R)>,
    V: Visitor,
{
    join.left().visit(visitor);
    join.right().visit(visitor);
}

pub fn walk_view<T, E, V>(_: &mut V, _: &View<T, E>)
where
    T: Tuple,
    E: Expression<T>,
    V: Visitor,
{
    // nothing to do
}

pub trait Collector {
    fn collect_relation<T>(&self, relation: &Relation<T>) -> Result<Tuples<T>>
    where
        T: Tuple;

    fn collect_select<T, E>(&self, select: &Select<T, E>) -> Result<Tuples<T>>
    where
        T: Tuple,
        E: Expression<T>;

    fn collect_project<S, T, E>(&self, project: &Project<S, T, E>) -> Result<Tuples<T>>
    where
        T: Tuple,
        S: Tuple,
        E: Expression<S>;

    fn collect_join<K, L, R, Left, Right, T>(
        &self,
        join: &Join<K, L, R, Left, Right, T>,
    ) -> Result<Tuples<T>>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: Expression<(K, L)>,
        Right: Expression<(K, R)>;

    fn collect_view<T, E>(&self, view: &View<T, E>) -> Result<Tuples<T>>
    where
        T: Tuple,
        E: Expression<T> + 'static;
}

pub trait ListCollector {
    fn collect_relation<T>(&self, relation: &Relation<T>) -> Result<Vec<Tuples<T>>>
    where
        T: Tuple;

    fn collect_select<T, E>(&self, select: &Select<T, E>) -> Result<Vec<Tuples<T>>>
    where
        T: Tuple,
        E: Expression<T>;

    fn collect_project<S, T, E>(&self, project: &Project<S, T, E>) -> Result<Vec<Tuples<T>>>
    where
        T: Tuple,
        S: Tuple,
        E: Expression<S>;

    fn collect_join<K, L, R, Left, Right, T>(
        &self,
        join: &Join<K, L, R, Left, Right, T>,
    ) -> Result<Vec<Tuples<T>>>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: Expression<(K, L)>,
        Right: Expression<(K, R)>;

    fn collect_view<T, E>(&self, view: &View<T, E>) -> Result<Vec<Tuples<T>>>
    where
        T: Tuple,
        E: Expression<T> + 'static;
}
