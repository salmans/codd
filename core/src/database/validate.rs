use crate::{
    expression::{Expression, Full, Visitor},
    Error, Tuple,
};

/// Is a [`Visitor`] that validates if an expression can be turned into a [`View`].
/// Expressions containing `Full` cannot be materialized and are rejected.
pub(crate) struct ViewExpressionValidator(Option<Error>);

impl ViewExpressionValidator {
    pub fn new() -> Self {
        Self(None)
    }

    #[inline]
    pub fn into_error(self) -> Option<Error> {
        self.0
    }
}

impl Visitor for ViewExpressionValidator {
    fn visit_full<T>(&mut self, _: &Full<T>)
    where
        T: Tuple,
    {
        self.0 = Some(Error::UnsupportedExpression {
            name: "Full".to_string(),
            operation: "Create View".to_string(),
        })
    }
}

/// Validates `expression` and returns an error if it cannot be turned into a [`View`].
pub(super) fn validate_view_expression<T, E>(expression: &E) -> Result<(), Error>
where
    T: Tuple,
    E: Expression<T>,
{
    let mut validator = ViewExpressionValidator::new();
    expression.visit(&mut validator);
    if let Some(e) = validator.into_error() {
        Err(e)
    } else {
        Ok(())
    }
}
