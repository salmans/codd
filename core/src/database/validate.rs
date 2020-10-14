use crate::{
    expression::{Expression, Visitor},
    Error, Tuple,
};

/// Is a `crate::expression::Visitor` that validates if an expression can be turned into
/// a [`View`]. Currently, expressions containing `Difference` are not supported.
///
/// [`View`]: ../../struct.View.html
/// [`Difference`]: ../../struct.Difference.html
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
