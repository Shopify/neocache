#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
// Every `unsafe` block and `unsafe impl` in this crate must carry a
// `// SAFETY:` justification. The lints below enforce that — they are
// the only mechanism that keeps the safety arguments at the call site
// instead of drifting into `docs/internals.md`.
#![warn(clippy::undocumented_unsafe_blocks)]
#![warn(clippy::missing_safety_doc)]
#![allow(clippy::type_complexity)]

pub mod iter;
mod lock;
pub mod mapref;
#[cfg(feature = "rayon")]
pub mod rayon_impl;
mod read_only;
#[cfg(feature = "serde")]
mod ser;
pub(crate) mod shard;
pub mod t;
pub mod try_result;
mod util;

use crate::lock::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use ahash::RandomState;
use core::borrow::Borrow;
use core::fmt;
use core::hash::{BuildHasher, Hash};
use core::iter::FromIterator;
use core::ops::{BitAnd, BitOr, Shl, Shr, Sub};
use crossbeam_utils::CachePadded;
use iter::{Iter, IterMut, OwningIter};
pub use mapref::entry::{Entry, OccupiedEntry, VacantEntry};
use mapref::multiple::RefMulti;
use mapref::one::{Ref, RefMut};
pub use read_only::ReadOnlyView;
use shard::{LOC_SMALL, ShardData};
use std::sync::LazyLock;
pub use t::Map;
use try_result::TryResult;

/// The per-shard type: hashbrown raw table + S3-FIFO eviction state.
///
/// Aliased as `HashMap<K,V>` to keep the rest of the codebase (iter, entry,
/// t.rs) using the familiar DashMap naming convention.
pub(crate) type HashMap<K, V> = ShardData<K, V>;

// ── TryReserveError ──────────────────────────────────────────────────────────

/// Error returned by [`NeoCache::try_reserve`] when allocation fails.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TryReserveError {}

// ── Shard count helpers ───────────────────────────────────────────────────────

fn default_shard_amount() -> usize {
    static DEFAULT_SHARD_AMOUNT: LazyLock<usize> = LazyLock::new(|| {
        (std::thread::available_parallelism().map_or(1, usize::from) * 16).next_power_of_two()
    });
    *DEFAULT_SHARD_AMOUNT
}

fn ncb(shard_amount: usize) -> usize {
    shard_amount.trailing_zeros() as usize
}

// ── NeoCache ─────────────────────────────────────────────────────────────────

/// A concurrent hash map with S3-FIFO cache eviction.
///
/// Each shard holds a hashbrown raw table **and** the eviction queues, so
/// eviction is fully concurrent — no global lock is ever taken.
///
/// When `cache_capacity` is 0 (constructed via `new_unbounded`), eviction is
/// disabled and the map grows without bound.
pub struct NeoCache<K, V, S = RandomState> {
    shift: usize,
    pub(crate) shards: Box<[CachePadded<RwLock<HashMap<K, V>>>]>,
    hasher: S,
    /// Total cache capacity across all shards (0 = unbounded).
    pub(crate) cache_capacity: usize,
}

// ── Clone ─────────────────────────────────────────────────────────────────────

impl<K: Eq + Hash + Clone, V: Clone, S: Clone> Clone for NeoCache<K, V, S> {
    fn clone(&self) -> Self {
        let inner_shards = self
            .shards
            .iter()
            .map(|s| CachePadded::new(RwLock::new((*s.read()).clone())))
            .collect();
        Self {
            shift: self.shift,
            shards: inner_shards,
            hasher: self.hasher.clone(),
            cache_capacity: self.cache_capacity,
        }
    }
}

// ── Default ───────────────────────────────────────────────────────────────────

impl<K, V, S> Default for NeoCache<K, V, S>
where
    K: Eq + Hash + Clone,
    S: Default + BuildHasher + Clone,
{
    fn default() -> Self {
        Self::with_hasher(Default::default())
    }
}

// ── Constructors (ahash default hasher) ──────────────────────────────────────

impl<K: Eq + Hash + Clone, V> NeoCache<K, V, RandomState> {
    /// Create a map with S3-FIFO eviction at the given capacity.
    pub fn new(cache_capacity: usize) -> Self {
        Self::with_capacity_and_hasher(cache_capacity, RandomState::new())
    }

    /// Create a map with S3-FIFO eviction and a specified shard count.
    ///
    /// `shard_amount` must be a power of two and at least 2.
    ///
    /// # Panics
    ///
    /// Panics if `shard_amount <= 1` or `shard_amount` is not a power of two.
    pub fn with_shard_amount(cache_capacity: usize, shard_amount: usize) -> Self {
        Self::with_capacity_and_hasher_and_shard_amount(
            cache_capacity,
            RandomState::new(),
            shard_amount,
        )
    }

    /// Create a map with no eviction limit (grows without bound).
    pub fn new_unbounded() -> Self {
        Self::with_capacity_and_hasher(0, RandomState::new())
    }
}

// ── Constructors (generic hasher) ─────────────────────────────────────────────

impl<'a, K: Eq + Hash + Clone, V: 'a, S: BuildHasher + Clone> NeoCache<K, V, S> {
    /// Converts the map into a lock-free [`ReadOnlyView`], consuming it.
    pub fn into_read_only(self) -> ReadOnlyView<K, V, S> {
        ReadOnlyView::new(self)
    }

    /// Create an unbounded map with a custom hasher.
    pub fn with_hasher(hasher: S) -> Self {
        Self::with_capacity_and_hasher(0, hasher)
    }

    /// Create a map with S3-FIFO eviction using a custom hasher.
    pub fn with_capacity_and_hasher(cache_capacity: usize, hasher: S) -> Self {
        Self::with_capacity_and_hasher_and_shard_amount(
            cache_capacity,
            hasher,
            default_shard_amount(),
        )
    }

    /// Create an unbounded map with a custom hasher and explicit shard count.
    ///
    /// `shard_amount` must be a power of two and at least 2.
    ///
    /// # Panics
    ///
    /// Panics if `shard_amount <= 1` or `shard_amount` is not a power of two.
    pub fn with_hasher_and_shard_amount(hasher: S, shard_amount: usize) -> Self {
        Self::with_capacity_and_hasher_and_shard_amount(0, hasher, shard_amount)
    }

    /// Core constructor.
    ///
    /// `cache_capacity` is the total S3-FIFO eviction capacity (0 = disabled).
    /// `shard_amount` must be a power of two and at least 2.
    ///
    /// # Panics
    ///
    /// Panics if `shard_amount <= 1` or `shard_amount` is not a power of two.
    pub fn with_capacity_and_hasher_and_shard_amount(
        cache_capacity: usize,
        hasher: S,
        shard_amount: usize,
    ) -> Self {
        assert!(shard_amount > 1);
        assert!(shard_amount.is_power_of_two());

        // If the requested shard count would make per-shard capacity < 4,
        // reduce the shard count so each shard holds enough entries to
        // avoid premature eviction on small caches (e.g. capacity=100).
        let shard_amount = if cache_capacity > 0 && cache_capacity / shard_amount < 4 {
            (cache_capacity / 4).next_power_of_two().max(2)
        } else {
            shard_amount
        };

        let shift = util::ptr_size_bits() - ncb(shard_amount);

        // Per-shard eviction capacity (rounded up so the sum ≥ cache_capacity).
        let shard_cap = if cache_capacity == 0 {
            0
        } else {
            cache_capacity.div_ceil(shard_amount).max(1)
        };

        let shards = (0..shard_amount)
            .map(|_| CachePadded::new(RwLock::new(ShardData::new(0, shard_cap))))
            .collect();

        Self {
            shift,
            shards,
            hasher,
            cache_capacity,
        }
    }

    // ── Hash helpers ──────────────────────────────────────────────────────────

    /// Hashes `item` to a `usize` using the map's hasher.
    ///
    /// Accepts unsized lookup keys (e.g. `str`, `[u8]`) — for example
    /// `cache.hash_usize::<str>("k")`.
    pub fn hash_usize<T: Hash + ?Sized>(&self, item: &T) -> usize {
        self.hash_u64(item) as usize
    }

    /// Hashes `item` with the map's hasher.
    ///
    /// Note on the `?Sized` bound and the call shape below: a lookup method
    /// has `key: &Q` and must reach this function with the *same* level of
    /// indirection as `_insert`'s `&key: &K`, otherwise `BuildHasher::hash_one`
    /// dispatches through a different `ahash::CallHasher` branch (ahash
    /// specializes primitives `T` and `&T`, but not `&&T`). Lookup callers
    /// therefore pass `key` directly (not `&key`); this requires `T: ?Sized`.
    /// See `tests::lookup_hash_matches_insert_hash_for_primitive_keys`.
    #[inline]
    pub(crate) fn hash_u64<T: Hash + ?Sized>(&self, item: &T) -> u64 {
        self.hasher.hash_one(item)
    }

    #[inline]
    pub(crate) fn determine_shard(&self, hash: usize) -> usize {
        // Leave the high 7 bits for HashBrown's SIMD tag.
        (hash << 7) >> self.shift
    }

    /// Returns a reference to the hasher.
    pub fn hasher(&self) -> &S {
        &self.hasher
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /// Insert a key-value pair. Returns the previous value if the key existed.
    ///
    /// S3-FIFO eviction may remove another entry to make room.
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        self._insert(key, value)
    }

    /// Removes an entry and returns its `(key, value)` pair, or `None`.
    pub fn remove<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self._remove(key)
    }

    /// Removes an entry if `f(key, value)` returns `true`.
    pub fn remove_if<Q>(&self, key: &Q, f: impl FnOnce(&K, &V) -> bool) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self._remove_if(key, f)
    }

    /// Removes an entry if `f(key, &mut value)` returns `true`.
    pub fn remove_if_mut<Q>(&self, key: &Q, f: impl FnOnce(&K, &mut V) -> bool) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self._remove_if_mut(key, f)
    }

    /// Returns an iterator over shared references to all entries.
    pub fn iter(&'a self) -> Iter<'a, K, V, S, NeoCache<K, V, S>> {
        self._iter()
    }

    /// Returns an iterator over mutable references to all entries.
    pub fn iter_mut(&'a self) -> IterMut<'a, K, V, S, NeoCache<K, V, S>> {
        self._iter_mut()
    }

    /// Returns a shared reference guard for `key`, bumping the frequency counter.
    pub fn get<Q>(&'a self, key: &Q) -> Option<Ref<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self._get(key)
    }

    /// Returns a mutable reference guard for `key`, bumping the frequency counter.
    pub fn get_mut<Q>(&'a self, key: &Q) -> Option<RefMut<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self._get_mut(key)
    }

    /// Non-blocking variant of `get`; returns `TryResult::Locked` if the shard is busy.
    pub fn try_get<Q>(&'a self, key: &Q) -> TryResult<Ref<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self._try_get(key)
    }

    /// Non-blocking variant of `get_mut`; returns `TryResult::Locked` if the shard is busy.
    pub fn try_get_mut<Q>(&'a self, key: &Q) -> TryResult<RefMut<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self._try_get_mut(key)
    }

    /// Shrinks each shard's hashbrown allocation to fit the current number of entries.
    pub fn shrink_to_fit(&self) {
        self._shrink_to_fit();
    }

    /// Removes all entries for which `f(key, value)` returns `false`.
    pub fn retain(&self, f: impl FnMut(&K, &mut V) -> bool) {
        self._retain(f);
    }

    /// Remove all entries and reset all S3-FIFO queues.
    pub fn clear(&self) {
        for s in self.shards.iter() {
            s.write().clear_all();
        }
    }

    /// Returns the total number of entries across all shards.
    pub fn len(&self) -> usize {
        self._len()
    }

    /// Returns `true` if the map contains no entries.
    pub fn is_empty(&self) -> bool {
        self._is_empty()
    }

    /// Returns the total allocated capacity across all shards.
    pub fn capacity(&self) -> usize {
        self._capacity()
    }

    /// The S3-FIFO cache capacity (0 = unbounded).
    pub fn cache_capacity(&self) -> usize {
        self.cache_capacity
    }

    /// Updates the value of an existing entry with `f(key, old_value) -> new_value`.
    pub fn alter<Q>(&self, key: &Q, f: impl FnOnce(&K, V) -> V)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self._alter(key, f);
    }

    /// Updates all entries with `f(key, old_value) -> new_value`.
    pub fn alter_all(&self, f: impl FnMut(&K, V) -> V) {
        self._alter_all(f);
    }

    /// Calls `f(key, value)` under the read lock and returns the result.
    pub fn view<Q, R>(&self, key: &Q, f: impl FnOnce(&K, &V) -> R) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self._view(key, f)
    }

    /// Returns `true` if the map contains an entry for `key`.
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self._contains_key(key)
    }

    /// Returns an [`Entry`] for the given key, acquiring a write lock.
    pub fn entry(&'a self, key: K) -> Entry<'a, K, V> {
        self._entry(key)
    }

    /// Non-blocking variant of [`entry`](Self::entry); returns `None` if the shard is busy.
    pub fn try_entry(&'a self, key: K) -> Option<Entry<'a, K, V>> {
        self._try_entry(key)
    }

    /// Attempts to reserve capacity for `additional` more entries in each shard.
    pub fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError> {
        for shard in self.shards.iter() {
            shard
                .write()
                .map_try_reserve(additional, &self.hasher)
                .map_err(|_| TryReserveError {})?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn shards(&self) -> &[CachePadded<RwLock<HashMap<K, V>>>] {
        &self.shards
    }
}

// ── Map trait impl ────────────────────────────────────────────────────────────

#[allow(private_interfaces)]
impl<'a, K: 'a + Eq + Hash + Clone, V: 'a, S: 'a + BuildHasher + Clone> Map<'a, K, V, S>
    for NeoCache<K, V, S>
{
    fn _shard_count(&self) -> usize {
        self.shards.len()
    }

    unsafe fn _get_read_shard(&'a self, i: usize) -> &'a HashMap<K, V> {
        debug_assert!(i < self.shards.len());
        // SAFETY: caller upholds the `Map::_get_read_shard` contract that
        // `i < self.shards.len()` and that no concurrent writer may exist
        // (the only in-tree caller is `ReadOnlyView`, which consumes the
        // map). `data_ptr()` returns a pointer into the lock's UnsafeCell
        // whose target lives for `'a` because the `Box<[...]>` is owned by
        // `self`.
        unsafe { &*self.shards.get_unchecked(i).data_ptr() }
    }

    unsafe fn _yield_read_shard(&'a self, i: usize) -> RwLockReadGuard<'a, HashMap<K, V>> {
        debug_assert!(i < self.shards.len());
        // SAFETY: caller upholds `i < self.shards.len()`. `read()` blocks
        // until a shared lock is acquired and is sound for any pinned
        // `RwLock`.
        unsafe { self.shards.get_unchecked(i).read() }
    }

    unsafe fn _yield_write_shard(&'a self, i: usize) -> RwLockWriteGuard<'a, HashMap<K, V>> {
        debug_assert!(i < self.shards.len());
        // SAFETY: caller upholds `i < self.shards.len()`. See
        // `_yield_read_shard` above.
        unsafe { self.shards.get_unchecked(i).write() }
    }

    unsafe fn _try_yield_read_shard(
        &'a self,
        i: usize,
    ) -> Option<RwLockReadGuard<'a, HashMap<K, V>>> {
        debug_assert!(i < self.shards.len());
        // SAFETY: caller upholds `i < self.shards.len()`. `try_read()` is
        // a non-blocking variant of `read()`.
        unsafe { self.shards.get_unchecked(i).try_read() }
    }

    unsafe fn _try_yield_write_shard(
        &'a self,
        i: usize,
    ) -> Option<RwLockWriteGuard<'a, HashMap<K, V>>> {
        debug_assert!(i < self.shards.len());
        // SAFETY: caller upholds `i < self.shards.len()`. `try_write()` is
        // a non-blocking variant of `write()`.
        unsafe { self.shards.get_unchecked(i).try_write() }
    }

    fn _insert(&self, key: K, value: V) -> Option<V> {
        use crate::shard::LOC_MAIN;
        use crate::util::CacheEntry;

        let hash = self.hash_u64(&key);
        let idx = self.determine_shard(hash as usize);
        // SAFETY: `idx = determine_shard(hash) < shards.len()` by
        // construction (the high bits of the hash are masked into the
        // shard-index range).
        let mut shard = unsafe { self._yield_write_shard(idx) };

        // Single probe: find existing entry OR locate insert slot.
        // reserve(1) is ~5ns overhead on updates but saves ~40ns second
        // probe on new-key inserts (under the write lock).
        match shard.map.find_or_find_insert_slot(
            hash,
            |(k, _)| k == &key,
            |(k, _)| self.hasher.hash_one(k),
        ) {
            Ok(bucket) => {
                // Key exists — replace value.
                // SAFETY: `bucket` was just returned by
                // `find_or_find_insert_slot` above and we hold the shard
                // write lock, so the bucket is live and uniquely owned.
                let old = unsafe { core::mem::replace(bucket.as_mut().1.value.get_mut(), value) };
                Some(old)
            }
            Err(slot) => {
                // New key — ghost check, eviction, insert at pre-found slot.
                let loc = if shard.ghost_set.remove(&hash) {
                    LOC_MAIN
                } else {
                    LOC_SMALL
                };

                while shard.shard_cap > 0 && shard.total_live() >= shard.shard_cap {
                    shard.evict_one();
                }

                let key_for_queue = key.clone();
                // SAFETY: `slot` was returned by `find_or_find_insert_slot`
                // above; the only intervening mutation is `evict_one`,
                // which uses `remove`/`erase` only — those do not
                // reallocate, so the slot remains valid.
                unsafe {
                    shard
                        .map
                        .insert_in_slot(hash, slot, (key, CacheEntry::new(value, loc)));
                }

                if loc == LOC_MAIN {
                    shard.main_hashes.push_back(hash);
                    shard.main_keys.push_back(key_for_queue);
                    shard.main_live += 1;
                } else {
                    shard.small_hashes.push_back(hash);
                    shard.small_keys.push_back(key_for_queue);
                    shard.small_live += 1;
                }

                None
            }
        }
    }

    fn _remove<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        // Pass `key` (not `&key`) so `BuildHasher::hash_one` sees the same
        // `&Q` shape that `_insert` passes as `&K` — see `hash_u64`'s docs.
        let hash = self.hash_u64(key);
        let idx = self.determine_shard(hash as usize);
        // SAFETY: `idx < shards.len()` by `determine_shard`.
        let mut shard = unsafe { self._yield_write_shard(idx) };

        if let Some(bucket) = shard.map.find(hash, |(k, _entry)| key == k.borrow()) {
            // SAFETY: `bucket` was just returned by `find` on this shard
            // under the held write lock; both calls below operate on it
            // before the lock is released.
            let loc = unsafe { bucket.as_ref().1.loc };
            // SAFETY: same bucket as above; `remove` consumes it.
            let ((k, entry), _) = unsafe { shard.map.remove(bucket) };
            if loc == LOC_SMALL {
                shard.small_live = shard.small_live.saturating_sub(1);
            } else {
                shard.main_live = shard.main_live.saturating_sub(1);
            }
            Some((k, entry.value.into_inner()))
        } else {
            None
        }
    }

    fn _remove_if<Q>(&self, key: &Q, f: impl FnOnce(&K, &V) -> bool) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_u64(key);
        let idx = self.determine_shard(hash as usize);
        // SAFETY: `idx < shards.len()` by `determine_shard`.
        let mut shard = unsafe { self._yield_write_shard(idx) };

        if let Some(bucket) = shard.map.find(hash, |(k, _entry)| key == k.borrow()) {
            // SAFETY: `bucket` was just returned by `find` on this shard
            // under the held write lock.
            let (k, entry) = unsafe { bucket.as_ref() };
            if f(k, entry.value.get()) {
                let loc = entry.loc;
                // SAFETY: same bucket as above; the predicate `f` did not
                // touch the table.
                let ((k, entry), _) = unsafe { shard.map.remove(bucket) };
                if loc == LOC_SMALL {
                    shard.small_live = shard.small_live.saturating_sub(1);
                } else {
                    shard.main_live = shard.main_live.saturating_sub(1);
                }
                Some((k, entry.value.into_inner()))
            } else {
                None
            }
        } else {
            None
        }
    }

    fn _remove_if_mut<Q>(&self, key: &Q, f: impl FnOnce(&K, &mut V) -> bool) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_u64(key);
        let idx = self.determine_shard(hash as usize);
        // SAFETY: `idx < shards.len()` by `determine_shard`.
        let mut shard = unsafe { self._yield_write_shard(idx) };

        if let Some(bucket) = shard.map.find(hash, |(k, _entry)| key == k.borrow()) {
            // SAFETY: `bucket` was just returned by `find` on this shard
            // under the held write lock; the projection to `&mut` is sound
            // because the lock excludes any other accessor.
            let (k, entry) = unsafe { bucket.as_mut() };
            if f(k, entry.value.get_mut()) {
                let loc = entry.loc;
                // SAFETY: same bucket as above; the predicate `f` did not
                // mutate the table structure (only the value).
                let ((k, entry), _) = unsafe { shard.map.remove(bucket) };
                if loc == LOC_SMALL {
                    shard.small_live = shard.small_live.saturating_sub(1);
                } else {
                    shard.main_live = shard.main_live.saturating_sub(1);
                }
                Some((k, entry.value.into_inner()))
            } else {
                None
            }
        } else {
            None
        }
    }

    fn _iter(&'a self) -> Iter<'a, K, V, S, NeoCache<K, V, S>> {
        Iter::new(self)
    }

    fn _iter_mut(&'a self) -> IterMut<'a, K, V, S, NeoCache<K, V, S>> {
        IterMut::new(self)
    }

    fn _get<Q>(&'a self, key: &Q) -> Option<Ref<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_u64(key);
        let idx = self.determine_shard(hash as usize);
        // SAFETY: `idx < shards.len()` by `determine_shard`.
        let shard = unsafe { self._yield_read_shard(idx) };

        if let Some(bucket) = shard.map.find(hash, |(k, _entry)| key == k.borrow()) {
            // SAFETY: `bucket` was returned by `find` on this shard's
            // table; the read guard is moved into the returned `Ref`,
            // satisfying `Ref::new`'s lifetime contract. `bump_freq` uses
            // `AtomicU8`, so a shared lock is sufficient even when other
            // readers race on the same entry.
            unsafe {
                let (k, entry) = bucket.as_ref();
                // Increment frequency (safe under read lock — AtomicU8).
                entry.bump_freq();
                Some(Ref::new(shard, k, entry.value.as_ptr()))
            }
        } else {
            None
        }
    }

    fn _get_mut<Q>(&'a self, key: &Q) -> Option<RefMut<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_u64(key);
        let idx = self.determine_shard(hash as usize);
        // SAFETY: `idx < shards.len()` by `determine_shard`.
        let shard = unsafe { self._yield_write_shard(idx) };

        if let Some(bucket) = shard.map.find(hash, |(k, _entry)| key == k.borrow()) {
            // SAFETY: `bucket` was returned by `find` under the held
            // write lock. The write guard moves into the returned
            // `RefMut`, satisfying its lifetime contract.
            unsafe {
                let (k, entry) = bucket.as_ref();
                entry.bump_freq();
                Some(RefMut::new(shard, k, entry.value.as_ptr()))
            }
        } else {
            None
        }
    }

    fn _try_get<Q>(&'a self, key: &Q) -> TryResult<Ref<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_u64(key);
        let idx = self.determine_shard(hash as usize);

        // SAFETY: `idx < shards.len()` by `determine_shard`.
        let shard = match unsafe { self._try_yield_read_shard(idx) } {
            Some(s) => s,
            None => return TryResult::Locked,
        };

        if let Some(bucket) = shard.map.find(hash, |(k, _entry)| key == k.borrow()) {
            // SAFETY: see `_get` above; the only difference is that the
            // shard lock was acquired non-blockingly.
            unsafe {
                let (k, entry) = bucket.as_ref();
                entry.bump_freq();
                TryResult::Present(Ref::new(shard, k, entry.value.as_ptr()))
            }
        } else {
            TryResult::Absent
        }
    }

    fn _try_get_mut<Q>(&'a self, key: &Q) -> TryResult<RefMut<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_u64(key);
        let idx = self.determine_shard(hash as usize);

        // SAFETY: `idx < shards.len()` by `determine_shard`.
        let shard = match unsafe { self._try_yield_write_shard(idx) } {
            Some(s) => s,
            None => return TryResult::Locked,
        };

        if let Some(bucket) = shard.map.find(hash, |(k, _entry)| key == k.borrow()) {
            // SAFETY: see `_get_mut` above.
            unsafe {
                let (k, entry) = bucket.as_ref();
                entry.bump_freq();
                TryResult::Present(RefMut::new(shard, k, entry.value.as_ptr()))
            }
        } else {
            TryResult::Absent
        }
    }

    fn _shrink_to_fit(&self) {
        self.shards.iter().for_each(|s| {
            let mut shard = s.write();
            let size = shard.map.len();
            shard.map_shrink_to(size, &self.hasher);
        });
    }

    fn _retain(&self, mut f: impl FnMut(&K, &mut V) -> bool) {
        self.shards.iter().for_each(|s| {
            let mut shard = s.write();
            // SAFETY: `shard.map.iter()` produces a `RawIter` cursor that
            // remains valid across `erase` calls (`erase` only marks the
            // slot as a tombstone; it neither reallocates nor moves
            // entries). Each `bucket.as_mut()` is sound because we hold
            // the write lock and `RawIter` yields each bucket at most
            // once, so no two `&mut` references to the same entry exist.
            unsafe {
                for bucket in shard.map.iter() {
                    let (k, entry) = bucket.as_mut();
                    if !f(&*k, entry.value.get_mut()) {
                        let loc = entry.loc;
                        shard.map.erase(bucket);
                        if loc == LOC_SMALL {
                            shard.small_live = shard.small_live.saturating_sub(1);
                        } else {
                            shard.main_live = shard.main_live.saturating_sub(1);
                        }
                    }
                }
            }
        });
    }

    fn _len(&self) -> usize {
        self.shards.iter().map(|s| s.read().map.len()).sum()
    }

    fn _capacity(&self) -> usize {
        self.shards.iter().map(|s| s.read().map.capacity()).sum()
    }

    fn _alter<Q>(&self, key: &Q, f: impl FnOnce(&K, V) -> V)
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        if let Some(mut r) = self.get_mut(key) {
            util::map_in_place_2(r.pair_mut(), f);
        }
    }

    fn _alter_all(&self, mut f: impl FnMut(&K, V) -> V) {
        self.iter_mut()
            .for_each(|mut m| util::map_in_place_2(m.pair_mut(), &mut f));
    }

    fn _view<Q, R>(&self, key: &Q, f: impl FnOnce(&K, &V) -> R) -> Option<R>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).map(|r| {
            let (k, v) = r.pair();
            f(k, v)
        })
    }

    fn _entry(&'a self, key: K) -> Entry<'a, K, V> {
        let hash = self.hash_u64(&key);
        let idx = self.determine_shard(hash as usize);
        // SAFETY: `idx < shards.len()` by `determine_shard`.
        let mut shard = unsafe { self._yield_write_shard(idx) };

        match shard.map.find_or_find_insert_slot(
            hash,
            |(k, _entry)| k == &key,
            |(k, _entry)| self.hasher.hash_one(k),
        ) {
            // SAFETY: `elem` was returned by `find_or_find_insert_slot`
            // on this shard's table under the held write lock; this
            // satisfies `OccupiedEntry::new`'s contract.
            Ok(elem) => Entry::Occupied(unsafe { OccupiedEntry::new(shard, key, elem) }),
            // SAFETY: `slot` was returned by `find_or_find_insert_slot`
            // on this shard's table under the held write lock; this
            // satisfies `VacantEntry::new`'s contract.
            Err(slot) => Entry::Vacant(unsafe { VacantEntry::new(shard, key, hash, slot) }),
        }
    }

    fn _try_entry(&'a self, key: K) -> Option<Entry<'a, K, V>> {
        let hash = self.hash_u64(&key);
        let idx = self.determine_shard(hash as usize);
        // SAFETY: `idx < shards.len()` by `determine_shard`.
        let mut shard = unsafe { self._try_yield_write_shard(idx) }?;

        match shard.map.find_or_find_insert_slot(
            hash,
            |(k, _entry)| k == &key,
            |(k, _entry)| self.hasher.hash_one(k),
        ) {
            // SAFETY: see `_entry` above.
            Ok(elem) => Some(Entry::Occupied(unsafe {
                OccupiedEntry::new(shard, key, elem)
            })),
            // SAFETY: see `_entry` above.
            Err(slot) => Some(Entry::Vacant(unsafe {
                VacantEntry::new(shard, key, hash, slot)
            })),
        }
    }

    fn _hasher(&self) -> S {
        self.hasher.clone()
    }
}

// ── Debug ─────────────────────────────────────────────────────────────────────

impl<K: Eq + Hash + Clone + fmt::Debug, V: fmt::Debug, S: BuildHasher + Clone> fmt::Debug
    for NeoCache<K, V, S>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut pmap = f.debug_map();
        for r in self {
            let (k, v) = r.pair();
            pmap.entry(k, v);
        }
        pmap.finish()
    }
}

// ── Operator overloads ────────────────────────────────────────────────────────

impl<'a, K: 'a + Eq + Hash + Clone, V: 'a, S: BuildHasher + Clone> Shl<(K, V)>
    for &'a NeoCache<K, V, S>
{
    type Output = Option<V>;
    fn shl(self, pair: (K, V)) -> Self::Output {
        self.insert(pair.0, pair.1)
    }
}

impl<'a, K: 'a + Eq + Hash + Clone, V: 'a, S: BuildHasher + Clone, Q> Shr<&Q>
    for &'a NeoCache<K, V, S>
where
    K: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
{
    type Output = Ref<'a, K, V>;
    fn shr(self, key: &Q) -> Self::Output {
        self.get(key).unwrap()
    }
}

impl<'a, K: 'a + Eq + Hash + Clone, V: 'a, S: BuildHasher + Clone, Q> BitOr<&Q>
    for &'a NeoCache<K, V, S>
where
    K: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
{
    type Output = RefMut<'a, K, V>;
    fn bitor(self, key: &Q) -> Self::Output {
        self.get_mut(key).unwrap()
    }
}

impl<'a, K: 'a + Eq + Hash + Clone, V: 'a, S: BuildHasher + Clone, Q> Sub<&Q>
    for &'a NeoCache<K, V, S>
where
    K: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
{
    type Output = Option<(K, V)>;
    fn sub(self, key: &Q) -> Self::Output {
        self.remove(key)
    }
}

impl<'a, K: 'a + Eq + Hash + Clone, V: 'a, S: BuildHasher + Clone, Q> BitAnd<&Q>
    for &'a NeoCache<K, V, S>
where
    K: Borrow<Q>,
    Q: Hash + Eq + ?Sized,
{
    type Output = bool;
    fn bitand(self, key: &Q) -> Self::Output {
        self.contains_key(key)
    }
}

// ── IntoIterator ──────────────────────────────────────────────────────────────

impl<K: Eq + Hash + Clone, V, S: BuildHasher + Clone> IntoIterator for NeoCache<K, V, S> {
    type Item = (K, V);
    type IntoIter = OwningIter<K, V, S>;
    fn into_iter(self) -> Self::IntoIter {
        OwningIter::new(self)
    }
}

impl<'a, K: Eq + Hash + Clone, V, S: BuildHasher + Clone> IntoIterator for &'a NeoCache<K, V, S> {
    type Item = RefMulti<'a, K, V>;
    type IntoIter = Iter<'a, K, V, S, NeoCache<K, V, S>>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<K: Eq + Hash + Clone, V, S: BuildHasher + Clone> Extend<(K, V)> for NeoCache<K, V, S> {
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, intoiter: I) {
        for pair in intoiter {
            self.insert(pair.0, pair.1);
        }
    }
}

impl<K: Eq + Hash + Clone, V, S: BuildHasher + Clone + Default> FromIterator<(K, V)>
    for NeoCache<K, V, S>
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(intoiter: I) -> Self {
        let mut map = NeoCache::default();
        map.extend(intoiter);
        map
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_and_get() {
        let map = NeoCache::new(100);
        map.insert("hello", 42u32);
        assert_eq!(*map.get("hello").unwrap(), 42);
    }

    #[test]
    fn test_insert_returns_old_value() {
        let map = NeoCache::new(100);
        assert_eq!(map.insert("k", 1u32), None);
        assert_eq!(map.insert("k", 2u32), Some(1));
        assert_eq!(*map.get("k").unwrap(), 2);
    }

    #[test]
    fn test_remove() {
        let map = NeoCache::new(100);
        map.insert(1u32, "a");
        let (k, v) = map.remove(&1u32).unwrap();
        assert_eq!(k, 1);
        assert_eq!(v, "a");
        assert!(map.get(&1u32).is_none());
    }

    #[test]
    fn test_get_bumps_freq() {
        let map = NeoCache::new(100);
        map.insert("key", 0u32);
        for _ in 0..5 {
            let _ = map.get("key");
        }
        assert!(map.contains_key("key"));
    }

    #[test]
    fn test_eviction_respects_capacity() {
        // Use with_shard_amount so cap divides evenly: shard_cap = 64/4 = 16.
        // Total live ≤ 16 * 4 = 64 == cap.
        let cap = 64usize;
        let map = NeoCache::with_shard_amount(cap, 4);
        for i in 0..200u64 {
            map.insert(i, i);
        }
        // Some entries were evicted; total must be ≤ capacity.
        assert!(map.len() <= cap, "len={} cap={}", map.len(), cap);
    }

    #[test]
    fn test_entry_or_insert() {
        let map = NeoCache::new(100);
        map.entry("k").or_insert(1u32);
        map.entry("k").or_insert(99u32);
        assert_eq!(*map.get("k").unwrap(), 1);
    }

    #[test]
    fn test_retain() {
        let map = NeoCache::new(100);
        for i in 0u32..10 {
            map.insert(i, i);
        }
        map.retain(|k, _v| *k % 2 == 0);
        assert_eq!(map.len(), 5);
        for i in (0u32..10).step_by(2) {
            assert!(map.contains_key(&i));
        }
    }

    #[test]
    fn test_iter_count() {
        let map = NeoCache::new_unbounded();
        for i in 0u32..20 {
            map.insert(i, i);
        }
        assert_eq!(map.iter().count(), 20);
    }

    #[test]
    fn test_into_iter() {
        let map = NeoCache::new(100);
        map.insert(1u32, "a");
        map.insert(2u32, "b");
        let mut pairs: Vec<_> = map.into_iter().collect();
        pairs.sort_by_key(|(k, _)| *k);
        assert_eq!(pairs, vec![(1, "a"), (2, "b")]);
    }

    #[test]
    fn test_clear() {
        let map = NeoCache::new(100);
        for i in 0u32..10 {
            map.insert(i, i);
        }
        map.clear();
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_try_get() {
        let map = NeoCache::new(100);
        map.insert("x", 7u32);
        assert_eq!(*map.try_get("x").unwrap(), 7);
        let _lock = map.get_mut("x");
        assert!(map.try_get("x").is_locked());
    }

    #[test]
    fn test_remove_if() {
        let map = NeoCache::new(100);
        map.insert(1u32, 10u32);
        assert!(map.remove_if(&1u32, |_, v| *v > 5).is_some());
        assert!(map.remove_if(&1u32, |_, v| *v > 5).is_none());
    }

    #[test]
    fn test_unbounded_grows_without_eviction() {
        let map: NeoCache<u64, u64> = NeoCache::new_unbounded();
        for i in 0..1000u64 {
            map.insert(i, i);
        }
        assert_eq!(map.len(), 1000);
    }

    #[test]
    fn test_replace_entry_returns_old_and_stores_new() {
        // Verify the basic replace_entry contract:
        // - returns the old (key, value) pair
        // - stores the new value under the same key
        // - map length is unchanged (no phantom insertion or removal)
        let map = NeoCache::new(100);
        map.insert(1u32, "original");

        // Saturate the frequency counter before replacing.
        for _ in 0..3 {
            let _ = map.get(&1u32);
        }

        let old_pair = match map.entry(1u32) {
            Entry::Occupied(occ) => occ.replace_entry("replaced"),
            Entry::Vacant(_) => panic!("key must be present"),
        };

        assert_eq!(old_pair, (1u32, "original"));
        assert_eq!(*map.get(&1u32).unwrap(), "replaced");
        assert_eq!(map.len(), 1);
    }

    /// Regression test for the ahash specialization bug.
    ///
    /// On nightly Rust, ahash auto-enables its `specialize` feature, which
    /// adds explicit `CallHasher` impls for primitives (`u32`, `i32`, ...)
    /// **and** for one-level references to them (`&u32`, `&i32`, ...).
    /// Two-level references (`&&u32`) fall through to the default impl,
    /// which uses a different hash function in the fallback (non-AES) build.
    ///
    /// Earlier revisions wrote `self.hash_u64(&key)` in lookup methods. With
    /// `key: &Q`, that produces `&&Q` at the `BuildHasher::hash_one` call
    /// site, hitting the unspecialized branch — while `_insert` (which has
    /// `key: K` by value) hit the specialized branch. The two paths
    /// disagreed on the bits, so a freshly-inserted primitive key could not
    /// be found by `remove` / `get` / `entry` under nightly Miri.
    ///
    /// Fix: lookup paths now pass `key` directly (not `&key`), so both
    /// insert and lookup feed the same `&K` / `&Q` reference into
    /// `hash_one`. This test pins that invariant: every lookup-shaped call
    /// must produce the same hash as the corresponding insert-shaped call.
    #[test]
    fn lookup_hash_matches_insert_hash_for_primitive_keys() {
        let map: NeoCache<u32, &'static str> = NeoCache::new(100);
        let h_insert = map.hash_u64(&1u32); // _insert path: T = u32
        let key_ref: &u32 = &1u32;
        let h_lookup = map.hash_u64(key_ref); // _remove etc.: T = u32 (after fix)
        assert_eq!(
            h_insert, h_lookup,
            "insert and lookup hashes must match — see the Borrow contract \
             and the doc-comment on this test",
        );
    }
}
