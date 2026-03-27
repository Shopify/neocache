#![doc = include_str!("../README.md")]
#![warn(missing_docs)]
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
use once_cell::sync::OnceCell;
pub use read_only::ReadOnlyView;
use shard::{LOC_SMALL, ShardData};
pub use t::Map;
use try_result::TryResult;
use ahash::RandomState;

/// The per-shard type: hashbrown raw table + S3-FIFO eviction state.
///
/// Aliased as `HashMap<K,V>` to keep the rest of the codebase (iter, entry,
/// t.rs) using the familiar DashMap naming convention.
pub(crate) type HashMap<K, V> = ShardData<K, V>;

// ── TryReserveError ──────────────────────────────────────────────────────────

/// Error returned by [`S3DashMap::try_reserve`] when allocation fails.
#[non_exhaustive]
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TryReserveError {}

// ── Shard count helpers ───────────────────────────────────────────────────────

fn default_shard_amount() -> usize {
    static DEFAULT_SHARD_AMOUNT: OnceCell<usize> = OnceCell::new();
    *DEFAULT_SHARD_AMOUNT.get_or_init(|| {
        (std::thread::available_parallelism().map_or(1, usize::from) * 4).next_power_of_two()
    })
}

fn ncb(shard_amount: usize) -> usize {
    shard_amount.trailing_zeros() as usize
}

// ── S3DashMap ─────────────────────────────────────────────────────────────────

/// A concurrent hash map with S3-FIFO cache eviction.
///
/// Each shard holds a hashbrown raw table **and** the eviction queues, so
/// eviction is fully concurrent — no global lock is ever taken.
///
/// When `cache_capacity` is 0 (constructed via `new_unbounded`), eviction is
/// disabled and the map grows without bound.
pub struct S3DashMap<K, V, S = RandomState> {
    shift: usize,
    pub(crate) shards: Box<[CachePadded<RwLock<HashMap<K, V>>>]>,
    hasher: S,
    /// Total cache capacity across all shards (0 = unbounded).
    pub(crate) cache_capacity: usize,
}

// ── Clone ─────────────────────────────────────────────────────────────────────

impl<K: Eq + Hash + Clone, V: Clone, S: Clone> Clone for S3DashMap<K, V, S> {
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

impl<K, V, S> Default for S3DashMap<K, V, S>
where
    K: Eq + Hash + Clone,
    S: Default + BuildHasher + Clone,
{
    fn default() -> Self {
        Self::with_hasher(Default::default())
    }
}

// ── Constructors (ahash default hasher) ──────────────────────────────────────

impl<K: Eq + Hash + Clone, V> S3DashMap<K, V, RandomState> {
    /// Create a map with S3-FIFO eviction at the given capacity.
    pub fn new(cache_capacity: usize) -> Self {
        Self::with_capacity_and_hasher(cache_capacity, RandomState::new())
    }

    /// Create a map with S3-FIFO eviction and a specified shard count.
    ///
    /// `shard_amount` must be a power of two greater than 1.
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

impl<'a, K: Eq + Hash + Clone, V: 'a, S: BuildHasher + Clone> S3DashMap<K, V, S> {
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
    pub fn with_hasher_and_shard_amount(hasher: S, shard_amount: usize) -> Self {
        Self::with_capacity_and_hasher_and_shard_amount(0, hasher, shard_amount)
    }

    /// Core constructor.
    ///
    /// `cache_capacity` is the total S3-FIFO eviction capacity (0 = disabled).
    pub fn with_capacity_and_hasher_and_shard_amount(
        cache_capacity: usize,
        hasher: S,
        shard_amount: usize,
    ) -> Self {
        assert!(shard_amount > 1);
        assert!(shard_amount.is_power_of_two());

        let shift = util::ptr_size_bits() - ncb(shard_amount);

        // Per-shard eviction capacity (rounded up so the sum ≥ cache_capacity).
        let shard_cap = if cache_capacity == 0 {
            0
        } else {
            cache_capacity.div_ceil(shard_amount).max(1)
        };

        let shards = (0..shard_amount)
            .map(|_| CachePadded::new(RwLock::new(ShardData::new(shard_cap, shard_cap))))
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
    pub fn hash_usize<T: Hash>(&self, item: &T) -> usize {
        self.hash_u64(item) as usize
    }

    #[inline]
    pub(crate) fn hash_u64<T: Hash>(&self, item: &T) -> u64 {
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
    pub fn remove_if_mut<Q>(
        &self,
        key: &Q,
        f: impl FnOnce(&K, &mut V) -> bool,
    ) -> Option<(K, V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self._remove_if_mut(key, f)
    }

    /// Returns an iterator over shared references to all entries.
    pub fn iter(&'a self) -> Iter<'a, K, V, S, S3DashMap<K, V, S>> {
        self._iter()
    }

    /// Returns an iterator over mutable references to all entries.
    pub fn iter_mut(&'a self) -> IterMut<'a, K, V, S, S3DashMap<K, V, S>> {
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
    for S3DashMap<K, V, S>
{
    fn _shard_count(&self) -> usize {
        self.shards.len()
    }

    unsafe fn _get_read_shard(&'a self, i: usize) -> &'a HashMap<K, V> {
        debug_assert!(i < self.shards.len());
        unsafe { &*self.shards.get_unchecked(i).data_ptr() }
    }

    unsafe fn _yield_read_shard(&'a self, i: usize) -> RwLockReadGuard<'a, HashMap<K, V>> {
        debug_assert!(i < self.shards.len());
        unsafe { self.shards.get_unchecked(i).read() }
    }

    unsafe fn _yield_write_shard(&'a self, i: usize) -> RwLockWriteGuard<'a, HashMap<K, V>> {
        debug_assert!(i < self.shards.len());
        unsafe { self.shards.get_unchecked(i).write() }
    }

    unsafe fn _try_yield_read_shard(
        &'a self,
        i: usize,
    ) -> Option<RwLockReadGuard<'a, HashMap<K, V>>> {
        debug_assert!(i < self.shards.len());
        unsafe { self.shards.get_unchecked(i).try_read() }
    }

    unsafe fn _try_yield_write_shard(
        &'a self,
        i: usize,
    ) -> Option<RwLockWriteGuard<'a, HashMap<K, V>>> {
        debug_assert!(i < self.shards.len());
        unsafe { self.shards.get_unchecked(i).try_write() }
    }

    fn _insert(&self, key: K, value: V) -> Option<V> {
        use crate::shard::{LOC_SMALL, LOC_MAIN};
        use crate::util::CacheEntry;

        let hash = self.hash_u64(&key);
        let idx = self.determine_shard(hash as usize);
        let mut shard = unsafe { self._yield_write_shard(idx) };

        // Single probe: find existing or get insert slot.
        match shard.map.find_or_find_insert_slot(
            hash,
            |(k, _)| k == &key,
            |(k, _)| self.hasher.hash_one(k),
        ) {
            Ok(bucket) => {
                // Fast path: update in-place, no eviction needed.
                let old = unsafe {
                    core::mem::replace(bucket.as_mut().1.value.get_mut(), value)
                };
                Some(old)
            }
            Err(slot) => {
                // Vacant: ghost check → evict → insert → register in queue.
                let loc = if shard.ghost_set.remove(&hash) {
                    LOC_MAIN
                } else {
                    LOC_SMALL
                };

                while shard.shard_cap > 0 && shard.total_live() >= shard.shard_cap {
                    shard.evict_one();
                }

                let key_for_queue = key.clone();

                unsafe {
                    let occupied = shard.map.insert_in_slot(
                        hash,
                        slot,
                        (key, CacheEntry::new(value, loc)),
                    );

                    if loc == LOC_MAIN {
                        shard.main.push_back((hash, key_for_queue));
                        shard.main_live += 1;
                    } else {
                        shard.small.push_back((hash, key_for_queue));
                        shard.small_live += 1;
                    }
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
        let hash = self.hash_u64(&key);
        let idx = self.determine_shard(hash as usize);
        let mut shard = unsafe { self._yield_write_shard(idx) };

        if let Some(bucket) = shard.map.find(hash, |(k, _entry)| key == k.borrow()) {
            let loc = unsafe { bucket.as_ref().1.loc };
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
        let hash = self.hash_u64(&key);
        let idx = self.determine_shard(hash as usize);
        let mut shard = unsafe { self._yield_write_shard(idx) };

        if let Some(bucket) = shard.map.find(hash, |(k, _entry)| key == k.borrow()) {
            let (k, entry) = unsafe { bucket.as_ref() };
            if f(k, entry.value.get()) {
                let loc = entry.loc;
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
        let hash = self.hash_u64(&key);
        let idx = self.determine_shard(hash as usize);
        let mut shard = unsafe { self._yield_write_shard(idx) };

        if let Some(bucket) = shard.map.find(hash, |(k, _entry)| key == k.borrow()) {
            let (k, entry) = unsafe { bucket.as_mut() };
            if f(k, entry.value.get_mut()) {
                let loc = entry.loc;
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

    fn _iter(&'a self) -> Iter<'a, K, V, S, S3DashMap<K, V, S>> {
        Iter::new(self)
    }

    fn _iter_mut(&'a self) -> IterMut<'a, K, V, S, S3DashMap<K, V, S>> {
        IterMut::new(self)
    }

    fn _get<Q>(&'a self, key: &Q) -> Option<Ref<'a, K, V>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let hash = self.hash_u64(&key);
        let idx = self.determine_shard(hash as usize);
        let shard = unsafe { self._yield_read_shard(idx) };

        if let Some(bucket) = shard.map.find(hash, |(k, _entry)| key == k.borrow()) {
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
        let hash = self.hash_u64(&key);
        let idx = self.determine_shard(hash as usize);
        let shard = unsafe { self._yield_write_shard(idx) };

        if let Some(bucket) = shard.map.find(hash, |(k, _entry)| key == k.borrow()) {
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
        let hash = self.hash_u64(&key);
        let idx = self.determine_shard(hash as usize);

        let shard = match unsafe { self._try_yield_read_shard(idx) } {
            Some(s) => s,
            None => return TryResult::Locked,
        };

        if let Some(bucket) = shard.map.find(hash, |(k, _entry)| key == k.borrow()) {
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
        let hash = self.hash_u64(&key);
        let idx = self.determine_shard(hash as usize);

        let shard = match unsafe { self._try_yield_write_shard(idx) } {
            Some(s) => s,
            None => return TryResult::Locked,
        };

        if let Some(bucket) = shard.map.find(hash, |(k, _entry)| key == k.borrow()) {
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
            unsafe {
                let mut shard = s.write();
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
        let mut shard = unsafe { self._yield_write_shard(idx) };

        match shard.map.find_or_find_insert_slot(
            hash,
            |(k, _entry)| k == &key,
            |(k, _entry)| self.hasher.hash_one(k),
        ) {
            Ok(elem) => Entry::Occupied(unsafe { OccupiedEntry::new(shard, key, elem) }),
            Err(slot) => Entry::Vacant(unsafe { VacantEntry::new(shard, key, hash, slot) }),
        }
    }

    fn _try_entry(&'a self, key: K) -> Option<Entry<'a, K, V>> {
        let hash = self.hash_u64(&key);
        let idx = self.determine_shard(hash as usize);
        let mut shard = unsafe { self._try_yield_write_shard(idx) }?;

        match shard.map.find_or_find_insert_slot(
            hash,
            |(k, _entry)| k == &key,
            |(k, _entry)| self.hasher.hash_one(k),
        ) {
            Ok(elem) => Some(Entry::Occupied(unsafe {
                OccupiedEntry::new(shard, key, elem)
            })),
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
    for S3DashMap<K, V, S>
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
    for &'a S3DashMap<K, V, S>
{
    type Output = Option<V>;
    fn shl(self, pair: (K, V)) -> Self::Output {
        self.insert(pair.0, pair.1)
    }
}

impl<'a, K: 'a + Eq + Hash + Clone, V: 'a, S: BuildHasher + Clone, Q> Shr<&Q>
    for &'a S3DashMap<K, V, S>
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
    for &'a S3DashMap<K, V, S>
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
    for &'a S3DashMap<K, V, S>
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
    for &'a S3DashMap<K, V, S>
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

impl<K: Eq + Hash + Clone, V, S: BuildHasher + Clone> IntoIterator for S3DashMap<K, V, S> {
    type Item = (K, V);
    type IntoIter = OwningIter<K, V, S>;
    fn into_iter(self) -> Self::IntoIter {
        OwningIter::new(self)
    }
}

impl<'a, K: Eq + Hash + Clone, V, S: BuildHasher + Clone> IntoIterator for &'a S3DashMap<K, V, S> {
    type Item = RefMulti<'a, K, V>;
    type IntoIter = Iter<'a, K, V, S, S3DashMap<K, V, S>>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<K: Eq + Hash + Clone, V, S: BuildHasher + Clone> Extend<(K, V)> for S3DashMap<K, V, S> {
    fn extend<I: IntoIterator<Item = (K, V)>>(&mut self, intoiter: I) {
        for pair in intoiter {
            self.insert(pair.0, pair.1);
        }
    }
}

impl<K: Eq + Hash + Clone, V, S: BuildHasher + Clone + Default> FromIterator<(K, V)>
    for S3DashMap<K, V, S>
{
    fn from_iter<I: IntoIterator<Item = (K, V)>>(intoiter: I) -> Self {
        let mut map = S3DashMap::default();
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
        let map = S3DashMap::new(100);
        map.insert("hello", 42u32);
        assert_eq!(*map.get("hello").unwrap(), 42);
    }

    #[test]
    fn test_insert_returns_old_value() {
        let map = S3DashMap::new(100);
        assert_eq!(map.insert("k", 1u32), None);
        assert_eq!(map.insert("k", 2u32), Some(1));
        assert_eq!(*map.get("k").unwrap(), 2);
    }

    #[test]
    fn test_remove() {
        let map = S3DashMap::new(100);
        map.insert(1u32, "a");
        let (k, v) = map.remove(&1u32).unwrap();
        assert_eq!(k, 1);
        assert_eq!(v, "a");
        assert!(map.get(&1u32).is_none());
    }

    #[test]
    fn test_get_bumps_freq() {
        let map = S3DashMap::new(100);
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
        let map = S3DashMap::with_shard_amount(cap, 4);
        for i in 0..200u64 {
            map.insert(i, i);
        }
        // Some entries were evicted; total must be ≤ capacity.
        assert!(map.len() <= cap, "len={} cap={}", map.len(), cap);
    }

    #[test]
    fn test_entry_or_insert() {
        let map = S3DashMap::new(100);
        map.entry("k").or_insert(1u32);
        map.entry("k").or_insert(99u32);
        assert_eq!(*map.get("k").unwrap(), 1);
    }

    #[test]
    fn test_retain() {
        let map = S3DashMap::new(100);
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
        let map = S3DashMap::new_unbounded();
        for i in 0u32..20 {
            map.insert(i, i);
        }
        assert_eq!(map.iter().count(), 20);
    }

    #[test]
    fn test_into_iter() {
        let map = S3DashMap::new(100);
        map.insert(1u32, "a");
        map.insert(2u32, "b");
        let mut pairs: Vec<_> = map.into_iter().collect();
        pairs.sort_by_key(|(k, _)| *k);
        assert_eq!(pairs, vec![(1, "a"), (2, "b")]);
    }

    #[test]
    fn test_clear() {
        let map = S3DashMap::new(100);
        for i in 0u32..10 {
            map.insert(i, i);
        }
        map.clear();
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_try_get() {
        let map = S3DashMap::new(100);
        map.insert("x", 7u32);
        assert_eq!(*map.try_get("x").unwrap(), 7);
        let _lock = map.get_mut("x");
        assert!(map.try_get("x").is_locked());
    }

    #[test]
    fn test_remove_if() {
        let map = S3DashMap::new(100);
        map.insert(1u32, 10u32);
        assert!(map.remove_if(&1u32, |_, v| *v > 5).is_some());
        assert!(map.remove_if(&1u32, |_, v| *v > 5).is_none());
    }

    #[test]
    fn test_unbounded_grows_without_eviction() {
        let map: S3DashMap<u64, u64> = S3DashMap::new_unbounded();
        for i in 0..1000u64 {
            map.insert(i, i);
        }
        assert_eq!(map.len(), 1000);
    }
}
