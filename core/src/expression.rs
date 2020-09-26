mod difference;
mod empty;
mod full;
mod intersect;
mod join;
mod product;
mod project;
mod relation;
mod select;
mod singleton;
mod union;
mod view;

use crate::{database::Tuples, Tuple};

use crate::Error;
pub use difference::Difference;
pub use empty::Empty;
pub use full::Full;
pub use intersect::Intersect;
pub use join::Join;
pub use product::Product;
pub use project::Project;
pub use relation::Relation;
pub use select::Select;
pub use singleton::Singleton;
pub use union::Union;
pub use view::View;

pub trait Expression<T: Tuple>: Clone + std::fmt::Debug {
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor;

    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
    where
        C: Collector;

    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
    where
        C: ListCollector;
}

pub trait Visitor: Sized {
    fn visit_full<T>(&mut self, full: &Full<T>)
    where
        T: Tuple,
    {
        walk_full(self, full)
    }

    fn visit_empty<T>(&mut self, empty: &Empty<T>)
    where
        T: Tuple,
    {
        walk_empty(self, empty)
    }

    fn visit_singleton<T>(&mut self, singleton: &Singleton<T>)
    where
        T: Tuple,
    {
        walk_singlenton(self, singleton)
    }

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

    fn visit_union<T, L, R>(&mut self, union: &Union<T, L, R>)
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        walk_union(self, union);
    }

    fn visit_intersect<T, L, R>(&mut self, intersect: &Intersect<T, L, R>)
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        walk_intersect(self, intersect);
    }

    fn visit_difference<T, L, R>(&mut self, difference: &Difference<T, L, R>)
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>,
    {
        walk_difference(self, difference);
    }

    fn visit_project<S, T, E>(&mut self, project: &Project<S, T, E>)
    where
        T: Tuple,
        S: Tuple,
        E: Expression<S>,
    {
        walk_project(self, project);
    }

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

    fn visit_view<T, E>(&mut self, view: &View<T, E>)
    where
        T: Tuple,
        E: Expression<T>,
    {
        walk_view(self, view);
    }
}

pub fn walk_full<T, V>(_: &mut V, _: &Full<T>)
where
    T: Tuple,
    V: Visitor,
{
    // nothing to do
}

pub fn walk_empty<T, V>(_: &mut V, _: &Empty<T>)
where
    T: Tuple,
    V: Visitor,
{
    // nothing to do
}

pub fn walk_singlenton<T, V>(_: &mut V, _: &Singleton<T>)
where
    T: Tuple,
    V: Visitor,
{
    // nothing to do
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

pub fn walk_union<T, L, R, V>(visitor: &mut V, union: &Union<T, L, R>)
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
    V: Visitor,
{
    union.left().visit(visitor);
    union.right().visit(visitor);
}

pub fn walk_intersect<T, L, R, V>(visitor: &mut V, intersect: &Intersect<T, L, R>)
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
    V: Visitor,
{
    intersect.left().visit(visitor);
    intersect.right().visit(visitor);
}

pub fn walk_difference<T, L, R, V>(visitor: &mut V, difference: &Difference<T, L, R>)
where
    T: Tuple,
    L: Expression<T>,
    R: Expression<T>,
    V: Visitor,
{
    difference.left().visit(visitor);
    difference.right().visit(visitor);
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

pub fn walk_product<L, R, Left, Right, T, V>(
    visitor: &mut V,
    product: &Product<L, R, Left, Right, T>,
) where
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

pub fn walk_join<K, L, R, Left, Right, T, V>(visitor: &mut V, join: &Join<K, L, R, Left, Right, T>)
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

pub fn walk_view<T, E, V>(_: &mut V, _: &View<T, E>)
where
    T: Tuple,
    E: Expression<T>,
    V: Visitor,
{
    // nothing to do
}

pub trait Collector {
    fn collect_full<T>(&self, full: &Full<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple;

    fn collect_empty<T>(&self, empty: &Empty<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple;

    fn collect_singleton<T>(&self, singleton: &Singleton<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple;

    fn collect_relation<T>(&self, relation: &Relation<T>) -> Result<Tuples<T>, Error>
    where
        T: Tuple + 'static;

    fn collect_select<T, E>(&self, select: &Select<T, E>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        E: Expression<T>;

    fn collect_union<T, L, R>(&self, union: &Union<T, L, R>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>;

    fn collect_intersect<T, L, R>(
        &self,
        intersect: &Intersect<T, L, R>,
    ) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>;

    fn collect_difference<T, L, R>(
        &self,
        difference: &Difference<T, L, R>,
    ) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>;

    fn collect_project<S, T, E>(&self, project: &Project<S, T, E>) -> Result<Tuples<T>, Error>
    where
        T: Tuple,
        S: Tuple,
        E: Expression<S>;

    fn collect_product<L, R, Left, Right, T>(
        &self,
        product: &Product<L, R, Left, Right, T>,
    ) -> Result<Tuples<T>, Error>
    where
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: Expression<L>,
        Right: Expression<R>;

    fn collect_join<K, L, R, Left, Right, T>(
        &self,
        join: &Join<K, L, R, Left, Right, T>,
    ) -> Result<Tuples<T>, Error>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: Expression<L>,
        Right: Expression<R>;

    fn collect_view<T, E>(&self, view: &View<T, E>) -> Result<Tuples<T>, Error>
    where
        T: Tuple + 'static,
        E: Expression<T> + 'static;
}

pub trait ListCollector {
    fn collect_full<T>(&self, full: &Full<T>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple;

    fn collect_empty<T>(&self, empty: &Empty<T>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple;

    fn collect_singleton<T>(&self, singleton: &Singleton<T>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple;

    fn collect_relation<T>(&self, relation: &Relation<T>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple + 'static;

    fn collect_select<T, E>(&self, select: &Select<T, E>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        E: Expression<T>;

    fn collect_union<T, L, R>(&self, union: &Union<T, L, R>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>;

    fn collect_intersect<T, L, R>(
        &self,
        intersect: &Intersect<T, L, R>,
    ) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>;

    fn collect_difference<T, L, R>(
        &self,
        difference: &Difference<T, L, R>,
    ) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        L: Expression<T>,
        R: Expression<T>;

    fn collect_project<S, T, E>(&self, project: &Project<S, T, E>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple,
        S: Tuple,
        E: Expression<S>;

    fn collect_product<L, R, Left, Right, T>(
        &self,
        product: &Product<L, R, Left, Right, T>,
    ) -> Result<Vec<Tuples<T>>, Error>
    where
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: Expression<L>,
        Right: Expression<R>;

    fn collect_join<K, L, R, Left, Right, T>(
        &self,
        join: &Join<K, L, R, Left, Right, T>,
    ) -> Result<Vec<Tuples<T>>, Error>
    where
        K: Tuple,
        L: Tuple,
        R: Tuple,
        T: Tuple,
        Left: Expression<L>,
        Right: Expression<R>;

    fn collect_view<T, E>(&self, view: &View<T, E>) -> Result<Vec<Tuples<T>>, Error>
    where
        T: Tuple + 'static,
        E: Expression<T> + 'static;
}

#[derive(Clone, Debug)]
pub enum Mono<T: Tuple + 'static> {
    Full(Full<T>),
    Empty(Empty<T>),
    Singleton(Singleton<T>),
    Relation(Relation<T>),
    Select(Select<T, Box<Mono<T>>>),
    Project(Project<T, T, Box<Mono<T>>>),
    Union(Union<T, Box<Mono<T>>, Box<Mono<T>>>),
    Intersect(Intersect<T, Box<Mono<T>>, Box<Mono<T>>>),
    Difference(Difference<T, Box<Mono<T>>, Box<Mono<T>>>),
    Product(Product<T, T, Box<Mono<T>>, Box<Mono<T>>, T>),
    Join(Join<T, T, T, Box<Mono<T>>, Box<Mono<T>>, T>),
    View(View<T, Box<Mono<T>>>),
}

impl<T: Tuple + 'static> Expression<T> for Mono<T> {
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        match self {
            Mono::Full(exp) => exp.visit(visitor),
            Mono::Empty(exp) => exp.visit(visitor),
            Mono::Singleton(exp) => exp.visit(visitor),
            Mono::Relation(exp) => exp.visit(visitor),
            Mono::Select(exp) => exp.visit(visitor),
            Mono::Project(exp) => exp.visit(visitor),
            Mono::Union(exp) => exp.visit(visitor),
            Mono::Intersect(exp) => exp.visit(visitor),
            Mono::Difference(exp) => exp.visit(visitor),
            Mono::Product(exp) => exp.visit(visitor),
            Mono::Join(exp) => exp.visit(visitor),
            Mono::View(exp) => exp.visit(visitor),
        }
    }
    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
    where
        C: Collector,
    {
        match self {
            Mono::Full(exp) => exp.collect(collector),
            Mono::Empty(exp) => exp.collect(collector),
            Mono::Singleton(exp) => exp.collect(collector),
            Mono::Relation(exp) => exp.collect(collector),
            Mono::Select(exp) => exp.collect(collector),
            Mono::Project(exp) => exp.collect(collector),
            Mono::Union(exp) => exp.collect(collector),
            Mono::Intersect(exp) => exp.collect(collector),
            Mono::Difference(exp) => exp.collect(collector),
            Mono::Product(exp) => exp.collect(collector),
            Mono::Join(exp) => exp.collect(collector),
            Mono::View(exp) => exp.collect(collector),
        }
    }
    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
    where
        C: ListCollector,
    {
        match self {
            Mono::Full(exp) => exp.collect_list(collector),
            Mono::Empty(exp) => exp.collect_list(collector),
            Mono::Singleton(exp) => exp.collect_list(collector),
            Mono::Relation(exp) => exp.collect_list(collector),
            Mono::Select(exp) => exp.collect_list(collector),
            Mono::Project(exp) => exp.collect_list(collector),
            Mono::Union(exp) => exp.collect_list(collector),
            Mono::Intersect(exp) => exp.collect_list(collector),
            Mono::Difference(exp) => exp.collect_list(collector),
            Mono::Product(exp) => exp.collect_list(collector),
            Mono::Join(exp) => exp.collect_list(collector),
            Mono::View(exp) => exp.collect_list(collector),
        }
    }
}

impl<T: Tuple + 'static> Expression<T> for Box<Mono<T>> {
    fn visit<V>(&self, visitor: &mut V)
    where
        V: Visitor,
    {
        (**self).visit(visitor)
    }
    fn collect<C>(&self, collector: &C) -> Result<Tuples<T>, Error>
    where
        C: Collector,
    {
        (**self).collect(collector)
    }
    fn collect_list<C>(&self, collector: &C) -> Result<Vec<Tuples<T>>, Error>
    where
        C: ListCollector,
    {
        (**self).collect_list(collector)
    }
}
