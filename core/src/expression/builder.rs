use super::*;
use crate::Tuple;
use std::marker::PhantomData;

pub trait IntoExpression<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
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

pub struct Builder<L, Left>
where
    L: Tuple,
    Left: Expression<L>,
{
    expression: Left,
    _marker: PhantomData<L>,
}

impl<L, Left> Builder<L, Left>
where
    L: Tuple,
    Left: Expression<L>,
{
    pub fn project<T>(self, f: impl FnMut(&L) -> T + 'static) -> Builder<T, Project<L, T, Left>>
    where
        T: Tuple,
    {
        Builder {
            expression: Project::new(&self.expression, f),
            _marker: PhantomData,
        }
    }

    pub fn select(self, f: impl FnMut(&L) -> bool + 'static) -> Builder<L, Select<L, Left>> {
        Builder {
            expression: Select::new(&self.expression, f),
            _marker: PhantomData,
        }
    }

    pub fn intersect<Right, I>(self, other: I) -> Builder<L, Intersect<L, Left, Right>>
    where
        Right: Expression<L>,
        I: IntoExpression<L, Right>,
    {
        Builder {
            expression: Intersect::new(&self.expression, &other.into_expression()),
            _marker: PhantomData,
        }
    }

    pub fn difference<Right, I>(self, other: I) -> Builder<L, Difference<L, Left, Right>>
    where
        Right: Expression<L>,
        I: IntoExpression<L, Right>,
    {
        Builder {
            expression: Difference::new(&self.expression, &other.into_expression()),
            _marker: PhantomData,
        }
    }

    pub fn union<Right, I>(self, other: I) -> Builder<L, Union<L, Left, Right>>
    where
        Right: Expression<L>,
        I: IntoExpression<L, Right>,
    {
        Builder {
            expression: Union::new(&self.expression, &other.into_expression()),
            _marker: PhantomData,
        }
    }

    pub fn product<R, Right, I>(self, other: I) -> ProductBuilder<L, R, Left, Right>
    where
        R: Tuple,
        Right: Expression<R>,
        I: IntoExpression<R, Right>,
    {
        ProductBuilder {
            left: self.expression,
            right: other.into_expression(),
            _marker: PhantomData,
        }
    }

    pub fn with_key<'k, K>(self, f: impl FnMut(&L) -> K + 'static) -> WithKeyBuilder<K, L, Left>
    where
        K: Tuple,
    {
        WithKeyBuilder {
            expression: self.expression,
            key: Box::new(f),
        }
    }

    pub fn build(self) -> Left {
        self.into_expression()
    }
}

impl<T, E> IntoExpression<T, E> for Builder<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    fn into_expression(self) -> E {
        self.expression
    }
}

impl<T, E> From<E> for Builder<T, E>
where
    T: Tuple,
    E: Expression<T>,
{
    fn from(expression: E) -> Self {
        Builder {
            expression,
            _marker: PhantomData,
        }
    }
}

pub struct ProductBuilder<L, R, Left, Right>
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

impl<L, R, Left, Right> ProductBuilder<L, R, Left, Right>
where
    L: Tuple,
    R: Tuple,
    Left: Expression<L>,
    Right: Expression<R>,
{
    pub fn on<T: Tuple>(
        self,
        f: impl FnMut(&L, &R) -> T + 'static,
    ) -> Builder<T, Product<L, R, Left, Right, T>> {
        Builder {
            expression: Product::new(&self.left, &self.right, f),
            _marker: PhantomData,
        }
    }
}

pub struct WithKeyBuilder<K, L, Left>
where
    K: Tuple + 'static,
    L: Tuple + 'static,
    Left: Expression<L>,
{
    expression: Left,
    key: Box<dyn FnMut(&L) -> K>,
}

impl<K, L, Left> WithKeyBuilder<K, L, Left>
where
    K: Tuple,
    L: Tuple,
    Left: Expression<L>,
{
    pub fn join<R, Right>(
        self,
        other: WithKeyBuilder<K, R, Right>,
    ) -> JoinBuilder<K, L, R, Left, Right>
    where
        R: Tuple,
        Right: Expression<R>,
    {
        JoinBuilder {
            left: self,
            right: other,
        }
    }
}

pub struct JoinBuilder<K, L, R, Left, Right>
where
    K: Tuple + 'static,
    L: Tuple + 'static,
    R: Tuple + 'static,
    Left: Expression<L>,
    Right: Expression<R>,
{
    left: WithKeyBuilder<K, L, Left>,
    right: WithKeyBuilder<K, R, Right>,
}

impl<K, L, R, Left, Right> JoinBuilder<K, L, R, Left, Right>
where
    K: Tuple + 'static,
    L: Tuple + 'static,
    R: Tuple + 'static,
    Left: Expression<L>,
    Right: Expression<R>,
{
    pub fn on<T: Tuple>(
        self,
        f: impl FnMut(&K, &L, &R) -> T + 'static,
    ) -> Builder<T, Join<K, L, R, Left, Right, T>> {
        Builder {
            expression: Join::new(
                &self.left.expression,
                &self.right.expression,
                self.left.key,
                self.right.key,
                f,
            ),
            _marker: PhantomData,
        }
    }
}
