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

unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Send for RefMulti<'a, K, V> {}
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

unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Send for RefMutMulti<'a, K, V> {}
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
        unsafe { (&*self.k, &*self.v) }
    }

    /// Returns a `(&key, &mut value)` tuple.
    pub fn pair_mut(&mut self) -> (&K, &mut V) {
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
