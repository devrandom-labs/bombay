use core::num::NonZeroUsize;

/// A validated bounded user-mailbox size.
///
/// Communication rounds a positive requested minimum up to a power of two,
/// with a floor of two. This value stores that effective capacity once.
///
/// The validated representation cannot be forged:
///
/// ```compile_fail,E0423
/// let capacity = bombay::MailboxCapacity(core::num::NonZeroUsize::MIN);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MailboxCapacity(NonZeroUsize);

impl MailboxCapacity {
    /// Validate the requested minimum before transferring or allocating a mailbox.
    ///
    /// # Errors
    /// Returns the original minimum if it is zero or rounding would overflow.
    pub fn new(minimum: usize) -> Result<Self, MailboxCapacityError> {
        if minimum == 0 {
            return Err(MailboxCapacityError { minimum });
        }
        let effective = minimum
            .max(2)
            .checked_next_power_of_two()
            .and_then(NonZeroUsize::new)
            .ok_or(MailboxCapacityError { minimum })?;
        Ok(Self(effective))
    }

    /// The effective number of user-message slots.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }
}

impl Default for MailboxCapacity {
    fn default() -> Self {
        Self(NonZeroUsize::new(1_024).expect("the existing bounded default is nonzero"))
    }
}

/// Rejected sizing with the original requested minimum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("mailbox minimum {minimum} must be positive and round to a representable power of two")]
pub struct MailboxCapacityError {
    /// The complete original requested minimum.
    pub minimum: usize,
}
