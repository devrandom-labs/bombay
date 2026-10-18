use core::num::NonZeroUsize;

// Communication requires the rounded power of two to be below 2^31 slots.
// A larger minimum would round past that native limit before allocation.
const MAX_CAPACITY: usize = 1 << 30;

/// A validated bounded user-mailbox size.
///
/// Communication rounds a positive requested minimum up to a power of two,
/// with a floor of two. The largest supported capacity is 2^30 slots.
/// This value stores that effective capacity once; it does not allocate slots.
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
    /// Returns the original minimum if it is zero or exceeds the largest
    /// supported capacity. No mailbox is allocated during validation.
    pub fn new(minimum: usize) -> Result<Self, MailboxCapacityError> {
        if minimum == 0 || minimum > MAX_CAPACITY {
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
#[error("mailbox minimum {minimum} must be between 1 and {}", MAX_CAPACITY)]
pub struct MailboxCapacityError {
    /// The complete original requested minimum.
    pub minimum: usize,
}
