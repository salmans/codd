mod database;
mod expression;
mod macros;

pub use database::{Database, Tuples};
pub use expression::{
    Difference, Expression, Intersect, Join, Product, Project, Relation, Select, Singleton, Union,
    View,
};

pub trait Tuple: Ord + Clone + 'static {}
impl<T: Ord + Clone + 'static> Tuple for T {}
