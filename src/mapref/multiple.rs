//! Reference types yielded by multi-shard iterators.
use crate::HashMap;
use crate::lock::{RwLockReadGuard, RwLockWriteGuard};
use core::hash::Hash;
use core::ops::{Deref, DerefMut};
use std::sync::Arc;

/// A shared reference to a map entry obtained during multi-shard iteration.
pub struct RefMulti<'a, K, V> {
    _guard: Arc<RwLockReadGuard<'a, HashMap<K, V>>>,
    k: *const K,
    v: *const V,
}

// SAFETY: `RefMulti` holds an `Arc<RwLockReadGuard>` and raw pointers
// into the locked shard. Crossing thread boundaries is sound because the
// `Arc<...>` keeps the lock alive on the receiving thread, and the read
// guard does not require unlock-on-the-locking-thread (parking_lot_core
// idiom). `K`/`V` need only be `Sync` because access is read-only.
unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Send for RefMulti<'a, K, V> {}
// SAFETY: see the `Send` impl above; `&RefMulti` exposes `&K` and `&V`
// which require `K: Sync` / `V: Sync`.
unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Sync for RefMulti<'a, K, V> {}

impl<'a, K: Eq + Hash, V> RefMulti<'a, K, V> {
    pub(crate) unsafe fn new(
        guard: Arc<RwLockReadGuard<'a, HashMap<K, V>>>,
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
        // SAFETY: `self.k` and `self.v` were stored by `RefMulti::new`
        // pointing into the locked shard. The `Arc<RwLockReadGuard>` keeps
        // the read lock held for the whole lifetime of `&self`, so no
        // writer can be mutating the entry.
        unsafe { (&*self.k, &*self.v) }
    }
}

impl<'a, K: Eq + Hash, V> Deref for RefMulti<'a, K, V> {
    type Target = V;

    fn deref(&self) -> &V {
        self.value()
    }
}

/// A mutable reference to a map entry obtained during multi-shard iteration.
pub struct RefMutMulti<'a, K, V> {
    _guard: Arc<RwLockWriteGuard<'a, HashMap<K, V>>>,
    k: *const K,
    v: *mut V,
}

// SAFETY: see the `RefMulti` impls above. `RefMutMulti` differs only in
// holding a write guard; mutation is gated on a unique `&mut
// RefMutMulti`, so multiple `RefMutMulti` clones from the same iterator
// cannot alias the same entry's `&mut V`.
unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Send for RefMutMulti<'a, K, V> {}
// SAFETY: see the `Send` impl above.
unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Sync for RefMutMulti<'a, K, V> {}

impl<'a, K: Eq + Hash, V> RefMutMulti<'a, K, V> {
    pub(crate) unsafe fn new(
        guard: Arc<RwLockWriteGuard<'a, HashMap<K, V>>>,
        k: *const K,
        v: *mut V,
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
        // SAFETY: `self.k`/`self.v` point into the locked shard; the
        // `Arc<RwLockWriteGuard>` keeps the lock held, and `&self` keeps
        // any concurrent `pair_mut` call on the same `RefMutMulti` out.
        unsafe { (&*self.k, &*self.v) }
    }

    /// Returns a `(&key, &mut value)` tuple.
    pub fn pair_mut(&mut self) -> (&K, &mut V) {
        // SAFETY: `&mut self` is unique by Rust's borrow rules; the write
        // guard inside the `Arc` excludes every other thread; and the
        // backing `RawIter` produced this entry exactly once, so no other
        // live `RefMutMulti` aliases this `*mut V`.
        unsafe { (&*self.k, &mut *self.v) }
    }
}

impl<'a, K: Eq + Hash, V> Deref for RefMutMulti<'a, K, V> {
    type Target = V;

    fn deref(&self) -> &V {
        self.value()
    }
}

impl<'a, K: Eq + Hash, V> DerefMut for RefMutMulti<'a, K, V> {
    fn deref_mut(&mut self) -> &mut V {
        self.value_mut()
    }
}
