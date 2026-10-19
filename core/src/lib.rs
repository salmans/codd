/*! Implements a minimal [database][Database] and relational algebraic [expressions][expression] for evaluating queries in the database.
 */
mod database;
pub mod expression;

#[cfg(feature = "unstable")]
mod macros;

pub use database::{Database, Tuples};
pub use expression::Expression;
use thiserror::Error;

/// Is the trait of tuples. Tuples are the smallest unit of data stored in databases.
///
/// **Note**: Tuples are analogous to the rows of a table in a conventional database.
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
