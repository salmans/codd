use crate::{expression::Visitor, Error, Tuple};

pub(super) struct Validator(pub(super) Option<Error>);

impl Validator {
    pub(super) fn new() -> Self {
        Self(None)
    }
}

impl Visitor for Validator {
    fn visit_difference<T, L, R>(&mut self, _: &crate::Difference<T, L, R>)
    where
        T: Tuple,
        L: crate::Expression<T>,
        R: crate::Expression<T>,
    {
        self.0 = Some(Error::UnsupportedExpression {
            name: "Difference".to_string(),
            operation: "Create View".to_string(),
        })
    }
}
