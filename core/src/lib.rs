/*! Implements a minimal [database] and relational [expressions] for evaluating queries
in the database.

[database]: ./struct.Database.html
[expressions]: ./expression/index.html
*/
mod database;
pub mod expression;
mod macros;

pub use database::{Database, Tuples};
pub use expression::{
    Difference, Empty, Expression, Full, Intersect, Join, Product, Project, Relation, Select,
    Singleton, Union, View,
};
use thiserror::Error;

/// Is the trait of tuples (analogous to the rows of a relational table).
pub trait Tuple: Ord + Clone + std::fmt::Debug {}
impl<T: Ord + Clone + std::fmt::Debug> Tuple for T {}

/// Is the type of errors returned by `codd`.
#[derive(Error, Debug)]
pub enum Error {
    /// Is returned when an unsupported operation is performed on an expression.
    #[error("unsopported operation `{operation:?}` on expression `{name:?}`")]
    UnsupportedExpression { name: String, operation: String },

    /// Is returned when a given relation instance doesn't exist.
    #[error("database instance `{name:?}` not found")]
    InstanceNotFound { name: String },

    /// Is returned when attempting to re-define an existing instance in a database.
    #[error("database instance `{name:?}` already exists")]
    InstanceExists { name: String },
}
