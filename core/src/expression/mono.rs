/*! Implements [`Mono`], a recursive expression where all subexpressions act on the
same [`Tuple`] type.

[`Tuple`]: ../../trait.Tuple.html
[`Mono`]: ./struct.Mono.html
*/

use super::*;

/// Is a recursive [expression] where all subexpressions act on the same [`Tuple`] type.;
///
/// [`Tuple`]: ../trait.Tuple.html
/// [expression]: ./trait.Expression.html
#[derive(Clone, Debug)]
#[allow(clippy::type_complexity)]
pub enum Mono<T>
where
    T: Tuple + 'static,
{
    Full(Full<T>),
    Empty(Empty<T>),
    Singleton(Singleton<T>),
    Relation(Relation<T>),
    Select(Box<Select<T, Mono<T>>>),
    Project(Box<Project<T, T, Mono<T>>>),
    Union(Box<Union<T, Mono<T>, Mono<T>>>),
    Intersect(Box<Intersect<T, Mono<T>, Mono<T>>>),
    Difference(Box<Difference<T, Mono<T>, Mono<T>>>),
    Product(Box<Product<T, T, Mono<T>, Mono<T>, T>>),
    Join(Box<Join<T, T, T, Mono<T>, Mono<T>, T>>),
    View(Box<View<T, Mono<T>>>),
}

impl<T: Tuple + 'static> Mono<T> {
    /// Wraps the receiver in a `Box`.
    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }
}

impl<T: Tuple> From<Full<T>> for Mono<T> {
    fn from(full: Full<T>) -> Self {
        Self::Full(full)
    }
}

impl<T: Tuple> From<Empty<T>> for Mono<T> {
    fn from(empty: Empty<T>) -> Self {
        Self::Empty(empty)
    }
}

impl<T: Tuple> From<Singleton<T>> for Mono<T> {
    fn from(singleton: Singleton<T>) -> Self {
        Self::Singleton(singleton)
    }
}

impl<T: Tuple> From<Relation<T>> for Mono<T> {
    fn from(relation: Relation<T>) -> Self {
        Self::Relation(relation)
    }
}

impl<T: Tuple> From<Select<T, Mono<T>>> for Mono<T> {
    fn from(select: Select<T, Mono<T>>) -> Self {
        Self::Select(Box::new(select))
    }
}

impl<T: Tuple> From<Project<T, T, Mono<T>>> for Mono<T> {
    fn from(project: Project<T, T, Mono<T>>) -> Self {
        Self::Project(Box::new(project))
    }
}

impl<T: Tuple> From<Union<T, Mono<T>, Mono<T>>> for Mono<T> {
    fn from(union: Union<T, Mono<T>, Mono<T>>) -> Self {
        Self::Union(Box::new(union))
    }
}

impl<T: Tuple> From<Intersect<T, Mono<T>, Mono<T>>> for Mono<T> {
    fn from(intersect: Intersect<T, Mono<T>, Mono<T>>) -> Self {
        Self::Intersect(Box::new(intersect))
    }
}

impl<T: Tuple> From<Difference<T, Mono<T>, Mono<T>>> for Mono<T> {
    fn from(difference: Difference<T, Mono<T>, Mono<T>>) -> Self {
        Self::Difference(Box::new(difference))
    }
}

impl<T: Tuple> From<Product<T, T, Mono<T>, Mono<T>, T>> for Mono<T> {
    fn from(product: Product<T, T, Mono<T>, Mono<T>, T>) -> Self {
        Self::Product(Box::new(product))
    }
}

impl<T: Tuple> From<Join<T, T, T, Mono<T>, Mono<T>, T>> for Mono<T> {
    fn from(join: Join<T, T, T, Mono<T>, Mono<T>, T>) -> Self {
        Self::Join(Box::new(join))
    }
}

impl<T: Tuple> From<View<T, Mono<T>>> for Mono<T> {
    fn from(view: View<T, Mono<T>>) -> Self {
        Self::View(Box::new(view))
    }
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
}
