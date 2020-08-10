mod database;
mod expression;
mod macros;
mod tools;

pub use database::{Database, Tuples};
pub use expression::{Expression, Join, Project, Relation, Select, View};

pub trait Tuple: Ord + Clone + 'static {}
impl<T: Ord + Clone + 'static> Tuple for T {}
