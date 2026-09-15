//! Pure-test helpers that preserve Bombay's exact static contracts.

use core::convert::Infallible;

use behavior::Never;

/// Extract the value from a result whose error is statically uninhabited.
///
/// This is intentionally absent from [`crate::prelude`]. Import it in a test
/// module when a Behavior's error is exactly [`Never`] or [`Infallible`]. If
/// that Behavior later gains an inhabited domain error, the method stops
/// compiling instead of becoming a latent panic.
pub trait InfallibleResultExt {
    /// Successful value carried by the result.
    type Output;

    /// Return the successful value without a panic path.
    #[must_use]
    fn infallible(self) -> Self::Output;
}

impl<T> InfallibleResultExt for Result<T, Never> {
    type Output = T;

    fn infallible(self) -> Self::Output {
        match self {
            Ok(value) => value,
            Err(error) => match error {},
        }
    }
}

impl<T> InfallibleResultExt for Result<T, Infallible> {
    type Output = T;

    fn infallible(self) -> Self::Output {
        match self {
            Ok(value) => value,
            Err(error) => match error {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn move_only_value_keeps_its_exact_allocation() {
        let value = String::from("owned actions");
        let allocation = value.as_ptr();

        let moved = Result::<_, Never>::Ok(value).infallible();

        assert_eq!(moved.as_ptr(), allocation);
    }

    #[test]
    fn core_infallible_result_uses_the_same_spelling() {
        let value = Result::<_, Infallible>::Ok(41_u64).infallible();

        assert_eq!(value, 41);
    }
}
