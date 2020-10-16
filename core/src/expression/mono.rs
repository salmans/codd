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
pub enum Mono<T>
where
    T: Tuple + 'static,
{
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

impl<T: Tuple + 'static> Mono<T> {
    /// Wraps the receiver in a `Box`.
    pub fn boxed(self) -> Box<Self> {
        Box::new(self)
    }

    pub fn builder(&self) -> Builder<T, Self> {
        Builder::from(self.clone())
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

impl<T: Tuple> From<Select<T, Box<Mono<T>>>> for Mono<T> {
    fn from(select: Select<T, Box<Mono<T>>>) -> Self {
        Self::Select(select)
    }
}

impl<T: Tuple> From<Project<T, T, Box<Mono<T>>>> for Mono<T> {
    fn from(project: Project<T, T, Box<Mono<T>>>) -> Self {
        Self::Project(project)
    }
}

impl<T: Tuple> From<Union<T, Box<Mono<T>>, Box<Mono<T>>>> for Mono<T> {
    fn from(union: Union<T, Box<Mono<T>>, Box<Mono<T>>>) -> Self {
        Self::Union(union)
    }
}

impl<T: Tuple> From<Intersect<T, Box<Mono<T>>, Box<Mono<T>>>> for Mono<T> {
    fn from(intersect: Intersect<T, Box<Mono<T>>, Box<Mono<T>>>) -> Self {
        Self::Intersect(intersect)
    }
}

impl<T: Tuple> From<Difference<T, Box<Mono<T>>, Box<Mono<T>>>> for Mono<T> {
    fn from(difference: Difference<T, Box<Mono<T>>, Box<Mono<T>>>) -> Self {
        Self::Difference(difference)
    }
}

impl<T: Tuple> From<Product<T, T, Box<Mono<T>>, Box<Mono<T>>, T>> for Mono<T> {
    fn from(product: Product<T, T, Box<Mono<T>>, Box<Mono<T>>, T>) -> Self {
        Self::Product(product)
    }
}

impl<T: Tuple> From<Join<T, T, T, Box<Mono<T>>, Box<Mono<T>>, T>> for Mono<T> {
    fn from(join: Join<T, T, T, Box<Mono<T>>, Box<Mono<T>>, T>) -> Self {
        Self::Join(join)
    }
}

impl<T: Tuple> From<View<T, Box<Mono<T>>>> for Mono<T> {
    fn from(view: View<T, Box<Mono<T>>>) -> Self {
        Self::View(view)
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
