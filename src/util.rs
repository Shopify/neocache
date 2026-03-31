use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicU8, Ordering};
use core::{mem, ptr};

/// Returns the number of bits in a pointer-sized integer.
///
/// Used by `NeoCache` to compute the bit-shift for shard selection.
pub const fn ptr_size_bits() -> usize {
    mem::size_of::<usize>() * 8
}

/// Replaces `*v` with `f(k, old_v)` using raw pointer manipulation.
///
/// The [`AbortOnPanic`] guard ensures that if `f` panics, the process aborts
/// rather than leaving a shard entry pointing to a partially-replaced value.
/// Called from `_alter` and `_alter_all` in the `Map` trait implementation.
pub fn map_in_place_2<T, U, F: FnOnce(U, T) -> T>((k, v): (U, &mut T), f: F) {
    unsafe {
        let promote_panic_to_abort = AbortOnPanic;
        ptr::write(v, f(k, ptr::read(v)));
        std::mem::forget(promote_panic_to_abort);
    }
}

/// Interior-mutable value wrapper used by the raw hashbrown table.
#[repr(transparent)]
pub struct SharedValue<T> {
    value: UnsafeCell<T>,
}

impl<T: Clone> Clone for SharedValue<T> {
    fn clone(&self) -> Self {
        Self {
            value: UnsafeCell::new(self.get().clone()),
        }
    }
}

unsafe impl<T: Send> Send for SharedValue<T> {}
unsafe impl<T: Sync> Sync for SharedValue<T> {}

impl<T> SharedValue<T> {
    /// Creates a new `SharedValue` wrapping `value`.
    pub const fn new(value: T) -> Self {
        Self {
            value: UnsafeCell::new(value),
        }
    }

    /// Returns a shared reference to the inner value.
    ///
    /// Callers must hold at least a shard read lock to ensure no concurrent
    /// mutable access is in flight.
    pub fn get(&self) -> &T {
        unsafe { &*self.value.get() }
    }

    /// Returns a mutable reference to the inner value.
    ///
    /// Callers must hold a shard write lock.
    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.value.get() }
    }

    /// Consumes the wrapper and returns the inner value.
    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }

    /// Returns a raw mutable pointer to the inner value.
    ///
    /// Used by [`Ref`](crate::mapref::one::Ref) and [`RefMut`](crate::mapref::one::RefMut)
    /// to store a raw pointer alongside the owning lock guard.
    pub(crate) fn as_ptr(&self) -> *mut T {
        self.value.get()
    }
}

/// Per-entry metadata for S3-FIFO eviction.
///
/// Stored inline in the hashbrown table alongside each key-value pair.
/// `freq` is an `AtomicU8` so it can be incremented under a read lock (in `get`).
/// `loc` is a plain `u8` since it's only written under a write lock (in eviction).
pub(crate) struct CacheEntry<V> {
    pub(crate) value: SharedValue<V>,
    pub(crate) freq: AtomicU8,
    pub(crate) loc: u8,
}

impl<V> CacheEntry<V> {
    /// Creates a new entry with `freq = 0` in the given eviction queue location.
    #[inline]
    pub(crate) fn new(value: V, loc: u8) -> Self {
        Self {
            value: SharedValue::new(value),
            freq: AtomicU8::new(0),
            loc,
        }
    }

    /// Increments the frequency counter, saturating at [`MAX_FREQ`](crate::shard::MAX_FREQ).
    ///
    /// Uses `Relaxed` ordering because the eviction decision only reads `freq`
    /// under a write lock, which establishes the necessary happens-before
    /// relationship with all prior `bump_freq` calls.
    #[inline]
    pub(crate) fn bump_freq(&self) {
        let _ = self
            .freq
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |f| {
                if f < crate::shard::MAX_FREQ {
                    Some(f + 1)
                } else {
                    None
                }
            });
    }
}

impl<V: Clone> Clone for CacheEntry<V> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            freq: AtomicU8::new(self.freq.load(Ordering::Relaxed)),
            loc: self.loc,
        }
    }
}

unsafe impl<V: Send> Send for CacheEntry<V> {}
unsafe impl<V: Sync> Sync for CacheEntry<V> {}

/// Guard that aborts the process if dropped while a panic is in flight.
///
/// Used by [`map_in_place_2`] to protect against user-closure panics leaving a
/// shard entry in an inconsistent half-replaced state. Preferred over
/// `catch_unwind` because it does not require `V: UnwindSafe`.
struct AbortOnPanic;

impl Drop for AbortOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            std::process::abort()
        }
    }
}
