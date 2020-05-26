use std::fmt::Debug;

mod database;
mod expression;
mod tools;

pub use database::Database;
pub use expression::Join;

pub trait Tuple: Ord + Clone + Debug {}
impl<T: Ord + Clone + Debug> Tuple for T {}

#[cfg(test)]
mod tests {}
