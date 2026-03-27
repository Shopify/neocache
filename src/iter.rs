//! Iterator types for [`S3DashMap`](crate::S3DashMap).
use super::mapref::multiple::{RefMulti, RefMutMulti};
use crate::lock::{RwLockReadGuard, RwLockWriteGuard};
use crate::t::Map;
use crate::util::CacheEntry;
use crate::{HashMap, S3DashMap};
use ahash::RandomState;
use core::hash::{BuildHasher, Hash};
use core::mem;
use std::marker::PhantomData;
use std::sync::Arc;

/// Iterator that consumes the map and yields `(K, V)` pairs.
pub struct OwningIter<K, V, S = RandomState> {
    map: S3DashMap<K, V, S>,
    shard_i: usize,
    current: Option<GuardOwningIter<K, V>>,
}

impl<K: Eq + Hash + Clone, V, S: BuildHasher + Clone> OwningIter<K, V, S> {
    pub(crate) fn new(map: S3DashMap<K, V, S>) -> Self {
        Self {
            map,
            shard_i: 0,
            current: None,
        }
    }
}

type GuardOwningIter<K, V> = hashbrown::raw::RawIntoIter<(K, CacheEntry<V>)>;

impl<K: Eq + Hash + Clone, V, S: BuildHasher + Clone> Iterator for OwningIter<K, V, S> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = self.current.as_mut()
                && let Some((k, entry)) = current.next()
            {
                return Some((k, entry.value.into_inner()));
            }

            if self.shard_i == self.map._shard_count() {
                return None;
            }

            let mut shard_wl = unsafe { self.map._yield_write_shard(self.shard_i) };

            // Take just the raw table; the S3 state (queues) is dropped with the guard.
            let raw_table = mem::take(&mut shard_wl.map);

            drop(shard_wl);

            self.current = Some(raw_table.into_iter());
            self.shard_i += 1;
        }
    }
}

unsafe impl<K, V, S> Send for OwningIter<K, V, S>
where
    K: Eq + Hash + Clone + Send,
    V: Send,
    S: BuildHasher + Clone + Send,
{
}

unsafe impl<K, V, S> Sync for OwningIter<K, V, S>
where
    K: Eq + Hash + Clone + Sync,
    V: Sync,
    S: BuildHasher + Clone + Sync,
{
}

type GuardIter<'a, K, V> = (
    Arc<RwLockReadGuard<'a, HashMap<K, V>>>,
    hashbrown::raw::RawIter<(K, CacheEntry<V>)>,
);

type GuardIterMut<'a, K, V> = (
    Arc<RwLockWriteGuard<'a, HashMap<K, V>>>,
    hashbrown::raw::RawIter<(K, CacheEntry<V>)>,
);

/// Iterator over a map yielding immutable references.
pub struct Iter<'a, K, V, S = RandomState, M = S3DashMap<K, V, S>> {
    map: &'a M,
    shard_i: usize,
    current: Option<GuardIter<'a, K, V>>,
    marker: PhantomData<S>,
}

impl<'i, K: Clone + Hash + Eq, V: Clone, S: Clone + BuildHasher> Clone for Iter<'i, K, V, S> {
    fn clone(&self) -> Self {
        Iter::new(self.map)
    }
}

unsafe impl<'a, 'i, K, V, S, M> Send for Iter<'i, K, V, S, M>
where
    K: 'a + Eq + Hash + Clone + Send,
    V: 'a + Send,
    S: 'a + BuildHasher + Clone,
    M: Map<'a, K, V, S>,
{
}

unsafe impl<'a, 'i, K, V, S, M> Sync for Iter<'i, K, V, S, M>
where
    K: 'a + Eq + Hash + Clone + Sync,
    V: 'a + Sync,
    S: 'a + BuildHasher + Clone,
    M: Map<'a, K, V, S>,
{
}

impl<'a, K: Eq + Hash + Clone, V, S: 'a + BuildHasher + Clone, M: Map<'a, K, V, S>>
    Iter<'a, K, V, S, M>
{
    pub(crate) fn new(map: &'a M) -> Self {
        Self {
            map,
            shard_i: 0,
            current: None,
            marker: PhantomData,
        }
    }
}

impl<'a, K: Eq + Hash + Clone, V, S: 'a + BuildHasher + Clone, M: Map<'a, K, V, S>> Iterator
    for Iter<'a, K, V, S, M>
{
    type Item = RefMulti<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = self.current.as_mut()
                && let Some(b) = current.1.next()
            {
                return unsafe {
                    let (k, entry) = b.as_ref();
                    let guard = current.0.clone();
                    Some(RefMulti::new(guard, k, entry.value.as_ptr()))
                };
            }

            if self.shard_i == self.map._shard_count() {
                return None;
            }

            let guard = unsafe { self.map._yield_read_shard(self.shard_i) };
            let iter = unsafe { guard.map.iter() };
            self.current = Some((Arc::new(guard), iter));
            self.shard_i += 1;
        }
    }
}

/// Iterator over a map yielding mutable references.
pub struct IterMut<'a, K, V, S = RandomState, M = S3DashMap<K, V, S>> {
    map: &'a M,
    shard_i: usize,
    current: Option<GuardIterMut<'a, K, V>>,
    marker: PhantomData<S>,
}

unsafe impl<'a, 'i, K, V, S, M> Send for IterMut<'i, K, V, S, M>
where
    K: 'a + Eq + Hash + Clone + Send,
    V: 'a + Send,
    S: 'a + BuildHasher + Clone,
    M: Map<'a, K, V, S>,
{
}

unsafe impl<'a, 'i, K, V, S, M> Sync for IterMut<'i, K, V, S, M>
where
    K: 'a + Eq + Hash + Clone + Sync,
    V: 'a + Sync,
    S: 'a + BuildHasher + Clone,
    M: Map<'a, K, V, S>,
{
}

impl<'a, K: Eq + Hash + Clone, V, S: 'a + BuildHasher + Clone, M: Map<'a, K, V, S>>
    IterMut<'a, K, V, S, M>
{
    pub(crate) fn new(map: &'a M) -> Self {
        Self {
            map,
            shard_i: 0,
            current: None,
            marker: PhantomData,
        }
    }
}

impl<'a, K: Eq + Hash + Clone, V, S: 'a + BuildHasher + Clone, M: Map<'a, K, V, S>> Iterator
    for IterMut<'a, K, V, S, M>
{
    type Item = RefMutMulti<'a, K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = self.current.as_mut()
                && let Some(b) = current.1.next()
            {
                return unsafe {
                    let (k, entry) = b.as_mut();
                    let guard = current.0.clone();
                    Some(RefMutMulti::new(guard, k, entry.value.as_ptr()))
                };
            }

            if self.shard_i == self.map._shard_count() {
                return None;
            }

            let guard = unsafe { self.map._yield_write_shard(self.shard_i) };
            let iter = unsafe { guard.map.iter() };
            self.current = Some((Arc::new(guard), iter));
            self.shard_i += 1;
        }
    }
}
