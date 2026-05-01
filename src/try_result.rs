//! Non-blocking lookup result type.

/// Represents the result of a non-blocking read.
#[derive(Debug)]
pub enum TryResult<R> {
    /// The value was present and the lock was obtained.
    Present(R),
    /// The shard wasn't locked and the value wasn't present.
    Absent,
    /// The shard was locked.
    Locked,
}

impl<R> TryResult<R> {
    /// Returns `true` if the entry was found and the lock was acquired.
    pub fn is_present(&self) -> bool {
        matches!(self, TryResult::Present(_))
    }

    /// Returns `true` if the shard was accessible but the key was not found.
    pub fn is_absent(&self) -> bool {
        matches!(self, TryResult::Absent)
    }

    /// Returns `true` if the shard was locked and the attempt was aborted.
    pub fn is_locked(&self) -> bool {
        matches!(self, TryResult::Locked)
    }

    /// Panics if the result is not `Present`, otherwise returns the inner value.
    pub fn unwrap(self) -> R {
        match self {
            TryResult::Present(r) => r,
            TryResult::Locked => panic!("Called unwrap() on TryResult::Locked"),
            TryResult::Absent => panic!("Called unwrap() on TryResult::Absent"),
        }
    }

    /// Returns `Some(r)` if `Present`, otherwise `None`.
    pub fn try_unwrap(self) -> Option<R> {
        match self {
            TryResult::Present(r) => Some(r),
            _ => None,
        }
    }
}
