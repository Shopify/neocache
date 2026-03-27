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
    pub fn is_present(&self) -> bool {
        matches!(self, TryResult::Present(_))
    }

    pub fn is_absent(&self) -> bool {
        matches!(self, TryResult::Absent)
    }

    pub fn is_locked(&self) -> bool {
        matches!(self, TryResult::Locked)
    }

    pub fn unwrap(self) -> R {
        match self {
            TryResult::Present(r) => r,
            TryResult::Locked => panic!("Called unwrap() on TryResult::Locked"),
            TryResult::Absent => panic!("Called unwrap() on TryResult::Absent"),
        }
    }

    pub fn try_unwrap(self) -> Option<R> {
        match self {
            TryResult::Present(r) => Some(r),
            _ => None,
        }
    }
}
