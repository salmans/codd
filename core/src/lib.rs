mod database;
mod expression;
mod macros;

pub use database::{Database, Tuples};
pub use expression::{
    Difference, Expression, Intersect, Join, Product, Project, Relation, Select, Singleton, Union,
    View,
};
use thiserror::Error;

pub trait Tuple: Ord + Clone {}
impl<T: Ord + Clone> Tuple for T {}

#[derive(Error, Debug)]
pub enum Error {
    #[error("unsopported operation `{operation:?}` on expression `{name:?}`")]
    UnsupportedExpression { name: String, operation: String },

    #[error("database instance `{name:?}` not found")]
    InstanceNotFound { name: String },

    #[error("database instance `{name:?}` already exists")]
    InstanceExists { name: String },
}
