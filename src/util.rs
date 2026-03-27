use core::cell::UnsafeCell;
use core::sync::atomic::{AtomicU8, Ordering};
use core::{mem, ptr};

pub const fn ptr_size_bits() -> usize {
    mem::size_of::<usize>() * 8
}

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
    pub const fn new(value: T) -> Self {
        Self {
            value: UnsafeCell::new(value),
        }
    }

    pub fn get(&self) -> &T {
        unsafe { &*self.value.get() }
    }

    pub fn get_mut(&mut self) -> &mut T {
        unsafe { &mut *self.value.get() }
    }

    pub fn into_inner(self) -> T {
        self.value.into_inner()
    }

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
    /// 16-bit hash fingerprint for eviction queue lookup.
    /// Stored in padding bytes — CacheEntry stays at 16 bytes for V=pointer.
    pub(crate) hash_check: u16,
}

impl<V> CacheEntry<V> {
    #[inline]
    pub(crate) fn new(value: V, loc: u8, hash: u64) -> Self {
        Self {
            value: SharedValue::new(value),
            freq: AtomicU8::new(0),
            loc,
            hash_check: hash as u16,
        }
    }

    #[inline]
    pub(crate) fn bump_freq(&self) {
        // Load+store instead of CAS loop. Races may lose an increment but freq
        // is a heuristic (saturates at 3) — correctness only requires "was
        // accessed at least once", so a lost increment is harmless.
        let f = self.freq.load(Ordering::Relaxed);
        if f < crate::shard::MAX_FREQ {
            self.freq.store(f + 1, Ordering::Relaxed);
        }
    }
}

impl<V: Clone> Clone for CacheEntry<V> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            freq: AtomicU8::new(self.freq.load(Ordering::Relaxed)),
            loc: self.loc,
            hash_check: self.hash_check,
        }
    }
}

unsafe impl<V: Send> Send for CacheEntry<V> {}
unsafe impl<V: Sync> Sync for CacheEntry<V> {}

struct AbortOnPanic;

impl Drop for AbortOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() {
            std::process::abort()
        }
    }
}
