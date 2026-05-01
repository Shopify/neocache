//! Iterator types for [`NeoCache`].
use super::mapref::multiple::{RefMulti, RefMutMulti};
use crate::lock::{RwLockReadGuard, RwLockWriteGuard};
use crate::t::Map;
use crate::util::CacheEntry;
use crate::{HashMap, NeoCache};
use ahash::RandomState;
use core::hash::{BuildHasher, Hash};
use core::mem;
use std::marker::PhantomData;
use std::sync::Arc;

/// Iterator that consumes the map and yields `(K, V)` pairs.
pub struct OwningIter<K, V, S = RandomState> {
    map: NeoCache<K, V, S>,
    shard_i: usize,
    current: Option<GuardOwningIter<K, V>>,
}

impl<K: Eq + Hash + Clone, V, S: BuildHasher + Clone> OwningIter<K, V, S> {
    pub(crate) fn new(map: NeoCache<K, V, S>) -> Self {
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

            // SAFETY: `shard_i < shard_count` is guaranteed by the early
            // return above, satisfying the unsafe contract on
            // `_yield_write_shard`.
            let mut shard_wl = unsafe { self.map._yield_write_shard(self.shard_i) };

            // Take just the raw table; the S3 state (queues) is dropped with the guard.
            // Replacing the shard's `map` with an empty `RawTable` while we
            // still hold the write lock means that any later eviction sweep
            // on this shard finds every queue entry stale (its `find` will
            // return `None`), keeping `ShardData` self-consistent. We then
            // release the lock and consume the extracted table without any
            // further synchronisation — sound because `OwningIter` owns the
            // `NeoCache` and no external alias to the shard exists.
            let raw_table = mem::take(&mut shard_wl.map);

            drop(shard_wl);

            self.current = Some(raw_table.into_iter());
            self.shard_i += 1;
        }
    }
}

// SAFETY: `OwningIter` owns its `NeoCache` and a partially-drained
// `RawIntoIter`. `Send` of an owned cache plus an iterator over `(K, V)`
// pairs requires `K: Send` and `V: Send`; `S` is sent because the cache
// owns the hasher.
unsafe impl<K, V, S> Send for OwningIter<K, V, S>
where
    K: Eq + Hash + Clone + Send,
    V: Send,
    S: BuildHasher + Clone + Send,
{
}

// SAFETY: `Sync` of `OwningIter` is sound under stricter bounds than
// `Send`: sharing `&OwningIter` does not give callers any way to mutate
// the inner cache (`next` requires `&mut self`), but a thread reading the
// owned cache via `Sync` can observe `K` and `V`, which therefore must be
// `Sync` themselves.
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
pub struct Iter<'a, K, V, S = RandomState, M = NeoCache<K, V, S>> {
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

// SAFETY: `Iter` holds an `&M` borrow plus a stashed read guard over a
// shard. Sending the iterator across threads is sound because the borrow
// outlives `'a` and the read guard, when present, is a shared lock that
// `parking_lot_core` permits to be released from any thread.
unsafe impl<'a, 'i, K, V, S, M> Send for Iter<'i, K, V, S, M>
where
    K: 'a + Eq + Hash + Clone + Send,
    V: 'a + Send,
    S: 'a + BuildHasher + Clone,
    M: Map<'a, K, V, S>,
{
}

// SAFETY: `Sync` is sound for the same reason `Send` is, with `Sync`
// bounds on `K`/`V` because shared access through `&Iter` produces
// references to the entries.
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
                // SAFETY: `b` was produced by `current.1.next()`, a
                // `RawIter` over the table inside the read guard
                // `current.0`. The guard is `Arc`-cloned into the new
                // `RefMulti` so the lock outlives the returned reference;
                // no concurrent writer can mutate the entry while any
                // clone of the read guard is alive.
                return unsafe {
                    let (k, entry) = b.as_ref();
                    let guard = current.0.clone();
                    Some(RefMulti::new(guard, k, entry.value.as_ptr()))
                };
            }

            if self.shard_i == self.map._shard_count() {
                return None;
            }

            // SAFETY: `shard_i < shard_count` is guaranteed by the early
            // return above, satisfying the unsafe contract on
            // `_yield_read_shard`.
            let guard = unsafe { self.map._yield_read_shard(self.shard_i) };
            // SAFETY: `iter` is a `RawIter` cursor into `guard.map`. It is
            // stored alongside `Arc::new(guard)`, so the read lock is held
            // for the cursor's full lifetime.
            let iter = unsafe { guard.map.iter() };
            self.current = Some((Arc::new(guard), iter));
            self.shard_i += 1;
        }
    }
}

/// Iterator over a map yielding mutable references.
pub struct IterMut<'a, K, V, S = RandomState, M = NeoCache<K, V, S>> {
    map: &'a M,
    shard_i: usize,
    current: Option<GuardIterMut<'a, K, V>>,
    marker: PhantomData<S>,
}

// SAFETY: see the `Send`/`Sync` justifications on `Iter` above; this
// variant differs only in holding a write guard rather than a read guard.
unsafe impl<'a, 'i, K, V, S, M> Send for IterMut<'i, K, V, S, M>
where
    K: 'a + Eq + Hash + Clone + Send,
    V: 'a + Send,
    S: 'a + BuildHasher + Clone,
    M: Map<'a, K, V, S>,
{
}

// SAFETY: see the `Send` impl above.
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
                // SAFETY: `b` is produced by `RawIter::next` on the table
                // inside the write guard `current.0`. We hold the write
                // lock, and `RawIter` yields each bucket at most once — so
                // the `&mut`-projected pointer cannot alias another live
                // `RefMutMulti` from the same iteration.
                return unsafe {
                    let (k, entry) = b.as_mut();
                    let guard = current.0.clone();
                    Some(RefMutMulti::new(guard, k, entry.value.as_ptr()))
                };
            }

            if self.shard_i == self.map._shard_count() {
                return None;
            }

            // SAFETY: `shard_i < shard_count` is guaranteed by the early
            // return above, satisfying the unsafe contract on
            // `_yield_write_shard`.
            let guard = unsafe { self.map._yield_write_shard(self.shard_i) };
            // SAFETY: `iter` is a `RawIter` cursor into `guard.map`. The
            // write guard is `Arc`-shared with each yielded `RefMutMulti`,
            // so the lock outlives the cursor.
            let iter = unsafe { guard.map.iter() };
            self.current = Some((Arc::new(guard), iter));
            self.shard_i += 1;
        }
    }
}
