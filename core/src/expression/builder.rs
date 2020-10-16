use super::*;
use crate::Tuple;
use std::marker::PhantomData;

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

/// Is a builder for building [`Expression`]s.
///
/// [`Expression`]: ./trait.Expression.html
pub struct Builder<L, Left>
where
    L: Tuple,
    Left: Expression<L>,
{
    /// Is the expression constructed by this builder.
    expression: Left,
    _marker: PhantomData<L>,
}

impl<L, Left> Builder<L, Left>
where
    L: Tuple,
    Left: Expression<L>,
{
    /// Builds a [`Project`] expression over the receiver's expression.
    ///
    /// [`Project`]: ./struct.Project.html
    ///
    /// **Example**:
    /// ```rust
    /// use codd::Database;
    ///
    /// let mut db = Database::new();
    /// let fruit = db.add_relation::<String>("R").unwrap();
    ///
    /// db.insert(&fruit, vec!["Apple".to_string(), "BANANA".into(), "cherry".into()].into());
    ///
    /// let lower = fruit.builder().project(|t| t.to_lowercase()).build();
    ///
    /// assert_eq!(vec!["apple", "banana", "cherry"], db.evaluate(&lower).unwrap().into_tuples());
    /// ```
    pub fn project<T>(self, f: impl FnMut(&L) -> T + 'static) -> Builder<T, Project<L, T, Left>>
    where
        T: Tuple,
    {
        Builder {
            expression: Project::new(&self.expression, f),
            _marker: PhantomData,
        }
    }

    /// Builds a [`Select`] expression over the receiver's expression.
    ///
    /// [`Select`]: ./struct.Select.html
    ///    
    /// **Example**:
    /// ```rust
    /// use codd::Database;
    ///
    /// let mut db = Database::new();
    /// let fruit = db.add_relation::<String>("Fruit").unwrap();
    ///
    /// db.insert(&fruit, vec!["Apple".to_string(), "BANANA".to_string(), "cherry".to_string()].into());
    ///
    /// let select = fruit.builder().select(|t| t.contains('A')).build();
    ///
    /// assert_eq!(vec!["Apple", "BANANA"], db.evaluate(&select).unwrap().into_tuples());
    /// ```
    pub fn select(self, f: impl FnMut(&L) -> bool + 'static) -> Builder<L, Select<L, Left>> {
        Builder {
            expression: Select::new(&self.expression, f),
            _marker: PhantomData,
        }
    }

    /// Builds an [`Intersect`] expression with the receiver's expression on left and `other` on right.
    ///
    /// [`Intersect`]: ./struct.Intersect.html
    ///
    /// **Example**:
    /// ```rust
    /// use codd::Database;
    ///
    /// let mut db = Database::new();
    /// let r = db.add_relation::<i32>("R").unwrap();
    /// let s = db.add_relation::<i32>("S").unwrap();
    ///
    /// db.insert(&r, vec![0, 1, 2].into());
    /// db.insert(&s, vec![2, 4].into());
    ///
    /// let intersect = r.builder().intersect(s).build();
    ///
    /// assert_eq!(vec![2], db.evaluate(&intersect).unwrap().into_tuples());
    /// ```
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

    /// Builds a [`Difference`] expression with the receiver's expression on left and `other` on right.
    ///
    /// [`Difference`]: ./struct.Difference.html
    ///
    /// **Example**:
    /// ```rust
    /// use codd::{Database, expression::Difference};
    ///
    /// let mut db = Database::new();
    /// let r = db.add_relation::<i32>("R").unwrap();
    /// let s = db.add_relation::<i32>("S").unwrap();
    ///
    /// db.insert(&r, vec![0, 1, 2].into());
    /// db.insert(&s, vec![2, 4].into());
    ///
    /// let r_s = r.builder().difference(&s).build();
    /// let s_r = s.builder().difference(r).build();
    ///
    /// assert_eq!(vec![0, 1], db.evaluate(&r_s).unwrap().into_tuples());
    /// assert_eq!(vec![4], db.evaluate(&s_r).unwrap().into_tuples());
    /// ```
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

    /// Builds a [`Union`] expression with the receiver's expression on left and `other` on right.
    ///
    /// [`Union`]: ./struct.Union.html
    ///
    /// **Example**:
    /// ```rust
    /// use codd::Database;
    ///
    /// let mut db = Database::new();
    /// let r = db.add_relation::<i32>("R").unwrap();
    /// let s = db.add_relation::<i32>("S").unwrap();
    ///
    /// db.insert(&r, vec![0, 1, 2].into());
    /// db.insert(&s, vec![2, 4].into());
    ///
    /// let union = r.builder().union(s).build();
    ///
    /// assert_eq!(vec![0, 1, 2, 4], db.evaluate(&union).unwrap().into_tuples());
    /// ```
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

    /// Combines the receiver's expression with `other` in a temporary builder, which then can be turned into
    /// a [`Product`] expression using a combining closure provided by method `on`.
    ///
    /// [`Product`]: ./struct.Product.html
    ///
    /// **Example**:
    /// ```rust
    /// use codd::Database;
    ///
    /// let mut db = Database::new();
    /// let r = db.add_relation::<i32>("R").unwrap();
    /// let s = db.add_relation::<i32>("S").unwrap();
    ///
    /// db.insert(&r, vec![0, 1, 2].into());
    /// db.insert(&s, vec![2, 4].into());
    ///
    /// let prod = r.builder().product(s).on(|l, r| l*r).build();
    ///
    /// assert_eq!(vec![0, 2, 4, 8], db.evaluate(&prod).unwrap().into_tuples());
    /// ```
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

    /// Combines the receiver's expression with closure `f` as the join key. This value can then be joined with
    /// another expression and it's key to create a temporary join builder. Finally, the temporary builder
    /// can be turned into a [`Join`] expression using a combining closure provided by method `on`.
    ///
    /// [`Join`]: ./struct.Join.html
    ///
    /// **Example**:
    /// ```rust
    /// use codd::Database;
    ///
    /// let mut db = Database::new();
    /// let fruit = db.add_relation::<(i32, String)>("R").unwrap();
    /// let numbers = db.add_relation::<i32>("S").unwrap();
    ///
    /// db.insert(&fruit, vec![
    ///    (0, "Apple".to_string()),
    ///    (1, "Banana".to_string()),
    ///    (2, "Cherry".to_string())
    /// ].into());
    /// db.insert(&numbers, vec![0, 2].into());
    ///
    /// let join = fruit
    ///     .builder()
    ///     .with_key(|t| t.0) // first element of tuples in `r` is the key for join
    ///     .join(numbers.builder().with_key(|&t| t))
    ///     .on(|k, l, r| format!("{}{}", l.1, k + r))
    ///         // combine the key `k`, left tuple `l` and right tuple `r`:    
    ///     .build();
    ///     
    /// assert_eq!(vec!["Apple0", "Cherry4"], db.evaluate(&join).unwrap().into_tuples());
    /// ```
    pub fn with_key<'k, K>(self, f: impl FnMut(&L) -> K + 'static) -> WithKeyBuilder<K, L, Left>
    where
        K: Tuple,
    {
        WithKeyBuilder {
            expression: self.expression,
            key: Box::new(f),
        }
    }

    /// Builds an expression from the receiver.
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
