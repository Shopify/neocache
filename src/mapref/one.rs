//! Single-key reference types returned by `get` and `get_mut`.
use crate::HashMap;
use crate::lock::{RwLockReadGuard, RwLockWriteGuard};
use core::hash::Hash;
use core::ops::{Deref, DerefMut};
use std::fmt::{Debug, Formatter};

/// A shared reference to a single map entry, holding a per-shard read lock.
pub struct Ref<'a, K, V> {
    _guard: RwLockReadGuard<'a, HashMap<K, V>>,
    k: *const K,
    v: *const V,
}

// SAFETY: `Ref` carries a read guard and raw pointers into the locked
// shard. The read guard from our vendored `RawRwLock` does not require
// unlock-on-the-locking-thread, so it is safe to send across threads.
// `K`/`V` need only be `Sync` because access through `Ref` is read-only.
unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Send for Ref<'a, K, V> {}
// SAFETY: see the `Send` impl above.
unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Sync for Ref<'a, K, V> {}

impl<'a, K: Eq + Hash, V> Ref<'a, K, V> {
    /// # Safety
    ///
    /// `k` and `v` must point to a key/value pair stored inside the
    /// `HashMap<K, V>` (the per-shard `ShardData`) protected by `guard`,
    /// and the entry must not be removed or relocated for the lifetime
    /// `'a`. The lock guard is what keeps the storage alive and pinned.
    pub(crate) unsafe fn new(
        guard: RwLockReadGuard<'a, HashMap<K, V>>,
        k: *const K,
        v: *const V,
    ) -> Self {
        Self {
            _guard: guard,
            k,
            v,
        }
    }

    /// Returns a reference to the key.
    pub fn key(&self) -> &K {
        self.pair().0
    }

    /// Returns a reference to the value.
    pub fn value(&self) -> &V {
        self.pair().1
    }

    /// Returns a `(&key, &value)` tuple.
    pub fn pair(&self) -> (&K, &V) {
        // SAFETY: `Ref::new`'s contract requires `self.k`/`self.v` to be
        // valid for the lifetime of the held read guard `_guard`. The
        // shared borrow of `&self` keeps that lifetime active and the
        // read lock excludes any concurrent writer.
        unsafe { (&*self.k, &*self.v) }
    }

    /// Projects the reference onto a sub-field of the value, keeping the lock held.
    pub fn map<F, T>(self, f: F) -> MappedRef<'a, K, V, T>
    where
        F: FnOnce(&V) -> &T,
    {
        MappedRef {
            _guard: self._guard,
            k: self.k,
            // SAFETY: `self.v` upholds the `Ref::new` contract; the read
            // guard is moved into the resulting `MappedRef`, so the lock
            // remains held for the projected reference's lifetime.
            v: f(unsafe { &*self.v }),
        }
    }

    /// Like [`map`](Self::map) but returns `Err(self)` if the projection returns `None`.
    pub fn try_map<F, T>(self, f: F) -> Result<MappedRef<'a, K, V, T>, Self>
    where
        F: FnOnce(&V) -> Option<&T>,
    {
        // SAFETY: see the `map` impl above.
        if let Some(v) = f(unsafe { &*self.v }) {
            Ok(MappedRef {
                _guard: self._guard,
                k: self.k,
                v,
            })
        } else {
            Err(self)
        }
    }
}

impl<'a, K: Eq + Hash + Debug, V: Debug> Debug for Ref<'a, K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ref")
            .field("k", self.key())
            .field("v", self.value())
            .finish()
    }
}

impl<'a, K: Eq + Hash, V> Deref for Ref<'a, K, V> {
    type Target = V;

    fn deref(&self) -> &V {
        self.value()
    }
}

/// A mutable reference to a single map entry, holding a per-shard write lock.
pub struct RefMut<'a, K, V> {
    guard: RwLockWriteGuard<'a, HashMap<K, V>>,
    k: *const K,
    v: *mut V,
}

// SAFETY: see the `Send`/`Sync` argument on `Ref` above. `RefMut` differs
// only in the guard kind — a write guard excludes both readers and writers,
// so the bound is the same.
unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Send for RefMut<'a, K, V> {}
// SAFETY: see the `Send` impl above.
unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Sync for RefMut<'a, K, V> {}

impl<'a, K: Eq + Hash, V> RefMut<'a, K, V> {
    /// # Safety
    ///
    /// `k` and `v` must point to a key/value pair stored inside the
    /// `HashMap<K, V>` (the per-shard `ShardData`) protected by `guard`,
    /// and the entry must not be removed or relocated for the lifetime
    /// `'a`. The write guard is what keeps the storage alive, pinned, and
    /// exclusively borrowed.
    pub(crate) unsafe fn new(
        guard: RwLockWriteGuard<'a, HashMap<K, V>>,
        k: *const K,
        v: *mut V,
    ) -> Self {
        Self { guard, k, v }
    }

    /// Returns a reference to the key.
    pub fn key(&self) -> &K {
        self.pair().0
    }

    /// Returns a shared reference to the value.
    pub fn value(&self) -> &V {
        self.pair().1
    }

    /// Returns a mutable reference to the value.
    pub fn value_mut(&mut self) -> &mut V {
        self.pair_mut().1
    }

    /// Returns a `(&key, &value)` tuple.
    pub fn pair(&self) -> (&K, &V) {
        // SAFETY: see `RefMut::new`. `&self` excludes any concurrent
        // `pair_mut`/`value_mut` call on this `RefMut`.
        unsafe { (&*self.k, &*self.v) }
    }

    /// Returns a `(&key, &mut value)` tuple.
    pub fn pair_mut(&mut self) -> (&K, &mut V) {
        // SAFETY: `&mut self` is unique by the borrow checker; the held
        // write guard excludes every other thread; together these give
        // exclusive access to the entry.
        unsafe { (&*self.k, &mut *self.v) }
    }

    /// Atomically downgrades this write guard to a shared read guard.
    pub fn downgrade(self) -> Ref<'a, K, V> {
        // SAFETY: `RwLockWriteGuard::downgrade` atomically converts the
        // exclusive lock to a shared one without releasing it. The
        // pointers `self.k`/`self.v` continue to satisfy `Ref::new`'s
        // contract under the (now shared) lock.
        unsafe { Ref::new(RwLockWriteGuard::downgrade(self.guard), self.k, self.v) }
    }

    /// Projects the mutable reference onto a sub-field of the value.
    pub fn map<F, T>(self, f: F) -> MappedRefMut<'a, K, V, T>
    where
        F: FnOnce(&mut V) -> &mut T,
    {
        MappedRefMut {
            _guard: self.guard,
            k: self.k,
            // SAFETY: `self` is consumed, so the `&mut V` we hand to `f`
            // is the only live reference to the value. The write guard
            // moves into the returned `MappedRefMut`, keeping the lock
            // held for the projected reference's lifetime.
            v: f(unsafe { &mut *self.v }),
        }
    }

    /// Like [`map`](Self::map) but returns `Err(self)` if the projection returns `None`.
    pub fn try_map<F, T>(self, f: F) -> Result<MappedRefMut<'a, K, V, T>, Self>
    where
        F: FnOnce(&mut V) -> Option<&mut T>,
    {
        // SAFETY: same as the `map` impl above. The cast `*mut V as
        // *mut _` only changes the inferred pointee type; the underlying
        // address remains the value pointer and is reborrowed exclusively.
        let v = match f(unsafe { &mut *(self.v as *mut _) }) {
            Some(v) => v,
            None => return Err(self),
        };
        let guard = self.guard;
        let k = self.k;
        Ok(MappedRefMut {
            _guard: guard,
            k,
            v,
        })
    }
}

impl<'a, K: Eq + Hash + Debug, V: Debug> Debug for RefMut<'a, K, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefMut")
            .field("k", self.key())
            .field("v", self.value())
            .finish()
    }
}

impl<'a, K: Eq + Hash, V> Deref for RefMut<'a, K, V> {
    type Target = V;

    fn deref(&self) -> &V {
        self.value()
    }
}

impl<'a, K: Eq + Hash, V> DerefMut for RefMut<'a, K, V> {
    fn deref_mut(&mut self) -> &mut V {
        self.value_mut()
    }
}

/// A reference to a projected sub-field of a map value, holding a read lock.
pub struct MappedRef<'a, K, V, T> {
    _guard: RwLockReadGuard<'a, HashMap<K, V>>,
    k: *const K,
    v: *const T,
}

impl<'a, K: Eq + Hash, V, T> MappedRef<'a, K, V, T> {
    /// Returns a reference to the key.
    pub fn key(&self) -> &K {
        self.pair().0
    }

    /// Returns a reference to the projected value.
    pub fn value(&self) -> &T {
        self.pair().1
    }

    /// Returns a `(&key, &projected_value)` tuple.
    pub fn pair(&self) -> (&K, &T) {
        // SAFETY: `MappedRef` was constructed from a `Ref` whose pointers
        // satisfied `Ref::new`'s contract; the projection function returned
        // a sub-reference of the value, which is also valid for the same
        // read-lock lifetime `'a`.
        unsafe { (&*self.k, &*self.v) }
    }

    /// Further projects onto a sub-field.
    pub fn map<F, T2>(self, f: F) -> MappedRef<'a, K, V, T2>
    where
        F: FnOnce(&T) -> &T2,
    {
        MappedRef {
            _guard: self._guard,
            k: self.k,
            // SAFETY: see `pair` above. The read guard is moved into the
            // new `MappedRef` so the projected reference outlives the
            // call.
            v: f(unsafe { &*self.v }),
        }
    }

    /// Like [`map`](Self::map) but returns `Err(self)` if the projection returns `None`.
    pub fn try_map<F, T2>(self, f: F) -> Result<MappedRef<'a, K, V, T2>, Self>
    where
        F: FnOnce(&T) -> Option<&T2>,
    {
        // SAFETY: see the `map` impl above.
        let v = match f(unsafe { &*self.v }) {
            Some(v) => v,
            None => return Err(self),
        };
        let guard = self._guard;
        Ok(MappedRef {
            _guard: guard,
            k: self.k,
            v,
        })
    }
}

impl<'a, K: Eq + Hash + Debug, V, T: Debug> Debug for MappedRef<'a, K, V, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedRef")
            .field("k", self.key())
            .field("v", self.value())
            .finish()
    }
}

impl<'a, K: Eq + Hash, V, T> Deref for MappedRef<'a, K, V, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.value()
    }
}

impl<'a, K: Eq + Hash, V, T: std::fmt::Display> std::fmt::Display for MappedRef<'a, K, V, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.value(), f)
    }
}

impl<'a, K: Eq + Hash, V, T: AsRef<TDeref>, TDeref: ?Sized> AsRef<TDeref>
    for MappedRef<'a, K, V, T>
{
    fn as_ref(&self) -> &TDeref {
        self.value().as_ref()
    }
}

/// A mutable reference to a projected sub-field of a map value, holding a write lock.
pub struct MappedRefMut<'a, K, V, T> {
    _guard: RwLockWriteGuard<'a, HashMap<K, V>>,
    k: *const K,
    v: *mut T,
}

impl<'a, K: Eq + Hash, V, T> MappedRefMut<'a, K, V, T> {
    /// Returns a reference to the key.
    pub fn key(&self) -> &K {
        self.pair().0
    }

    /// Returns a shared reference to the projected value.
    pub fn value(&self) -> &T {
        self.pair().1
    }

    /// Returns a mutable reference to the projected value.
    pub fn value_mut(&mut self) -> &mut T {
        self.pair_mut().1
    }

    /// Returns a `(&key, &projected_value)` tuple.
    pub fn pair(&self) -> (&K, &T) {
        // SAFETY: see the corresponding `MappedRef::pair`. The held write
        // guard excludes other threads; `&self` excludes other accesses
        // through this `MappedRefMut`.
        unsafe { (&*self.k, &*self.v) }
    }

    /// Returns a `(&key, &mut projected_value)` tuple.
    pub fn pair_mut(&mut self) -> (&K, &mut T) {
        // SAFETY: `&mut self` is unique by the borrow checker; the held
        // write guard excludes every other thread.
        unsafe { (&*self.k, &mut *self.v) }
    }

    /// Further projects onto a sub-field mutably.
    pub fn map<F, T2>(self, f: F) -> MappedRefMut<'a, K, V, T2>
    where
        F: FnOnce(&mut T) -> &mut T2,
    {
        MappedRefMut {
            _guard: self._guard,
            k: self.k,
            // SAFETY: `self` is consumed; the projection produces the only
            // live `&mut T` to the entry. The write guard moves into the
            // returned `MappedRefMut`.
            v: f(unsafe { &mut *self.v }),
        }
    }

    /// Like [`map`](Self::map) but returns `Err(self)` if the projection returns `None`.
    pub fn try_map<F, T2>(self, f: F) -> Result<MappedRefMut<'a, K, V, T2>, Self>
    where
        F: FnOnce(&mut T) -> Option<&mut T2>,
    {
        // SAFETY: see the `map` impl above; the cast only changes the
        // inferred pointee type.
        let v = match f(unsafe { &mut *(self.v as *mut _) }) {
            Some(v) => v,
            None => return Err(self),
        };
        let guard = self._guard;
        let k = self.k;
        Ok(MappedRefMut {
            _guard: guard,
            k,
            v,
        })
    }
}

impl<'a, K: Eq + Hash + Debug, V, T: Debug> Debug for MappedRefMut<'a, K, V, T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MappedRefMut")
            .field("k", self.key())
            .field("v", self.value())
            .finish()
    }
}

impl<'a, K: Eq + Hash, V, T> Deref for MappedRefMut<'a, K, V, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.value()
    }
}

impl<'a, K: Eq + Hash, V, T> DerefMut for MappedRefMut<'a, K, V, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.value_mut()
    }
}
