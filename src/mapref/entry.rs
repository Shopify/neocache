//! Entry API — occupied and vacant entry types.
use super::one::RefMut;
use crate::HashMap;
use crate::lock::RwLockWriteGuard;
use crate::shard::{LOC_MAIN, LOC_SMALL};
use crate::util::CacheEntry;
use core::hash::Hash;
use core::mem;

/// A view into a single entry in the map, which may be occupied or vacant.
pub enum Entry<'a, K, V> {
    /// An occupied entry.
    Occupied(OccupiedEntry<'a, K, V>),
    /// A vacant entry.
    Vacant(VacantEntry<'a, K, V>),
}

impl<'a, K: Eq + Hash, V> Entry<'a, K, V> {
    /// Provides in-place mutable access to an occupied entry before any potential inserts.
    pub fn and_modify(self, f: impl FnOnce(&mut V)) -> Self {
        match self {
            Entry::Occupied(mut entry) => {
                f(entry.get_mut());
                Entry::Occupied(entry)
            }
            Entry::Vacant(entry) => Entry::Vacant(entry),
        }
    }

    /// Returns a reference to the key of the entry.
    pub fn key(&self) -> &K {
        match *self {
            Entry::Occupied(ref entry) => entry.key(),
            Entry::Vacant(ref entry) => entry.key(),
        }
    }

    /// Consumes the entry and returns the key.
    pub fn into_key(self) -> K {
        match self {
            Entry::Occupied(entry) => entry.into_key(),
            Entry::Vacant(entry) => entry.into_key(),
        }
    }

    /// Ensures a value is in the entry by inserting `V::default()` if vacant.
    pub fn or_default(self) -> RefMut<'a, K, V>
    where
        K: Clone,
        V: Default,
    {
        match self {
            Entry::Occupied(entry) => entry.into_ref(),
            Entry::Vacant(entry) => entry.insert(V::default()),
        }
    }

    /// Ensures a value is in the entry by inserting `value` if vacant.
    pub fn or_insert(self, value: V) -> RefMut<'a, K, V>
    where
        K: Clone,
    {
        match self {
            Entry::Occupied(entry) => entry.into_ref(),
            Entry::Vacant(entry) => entry.insert(value),
        }
    }

    /// Ensures a value is in the entry by inserting the result of `value()` if vacant.
    pub fn or_insert_with(self, value: impl FnOnce() -> V) -> RefMut<'a, K, V>
    where
        K: Clone,
    {
        match self {
            Entry::Occupied(entry) => entry.into_ref(),
            Entry::Vacant(entry) => entry.insert(value()),
        }
    }

    /// Ensures a value is in the entry by inserting the result of `value()` if vacant,
    /// propagating any error from the fallible closure.
    pub fn or_try_insert_with<E>(
        self,
        value: impl FnOnce() -> Result<V, E>,
    ) -> Result<RefMut<'a, K, V>, E>
    where
        K: Clone,
    {
        match self {
            Entry::Occupied(entry) => Ok(entry.into_ref()),
            Entry::Vacant(entry) => Ok(entry.insert(value()?)),
        }
    }

    /// Inserts `value` into the entry regardless of whether it was occupied.
    pub fn insert(self, value: V) -> RefMut<'a, K, V>
    where
        K: Clone,
    {
        match self {
            Entry::Occupied(mut entry) => {
                entry.insert(value);
                entry.into_ref()
            }
            Entry::Vacant(entry) => entry.insert(value),
        }
    }

    /// Inserts `value` and returns an `OccupiedEntry` regardless of prior occupancy.
    pub fn insert_entry(self, value: V) -> OccupiedEntry<'a, K, V>
    where
        K: Clone,
    {
        match self {
            Entry::Occupied(mut entry) => {
                entry.insert(value);
                entry
            }
            Entry::Vacant(entry) => entry.insert_entry(value),
        }
    }
}

/// A view into a vacant entry in the map.
pub struct VacantEntry<'a, K, V> {
    shard: RwLockWriteGuard<'a, HashMap<K, V>>,
    key: K,
    hash: u64,
    slot: hashbrown::raw::InsertSlot,
}

// SAFETY: `VacantEntry` holds a write guard plus an `InsertSlot` and a
// pre-computed key/hash. The guard supplies exclusive access; the
// `InsertSlot` is only used inside `insert`/`insert_entry` while the
// write guard is still held, so it cannot race with other shards.
unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Send for VacantEntry<'a, K, V> {}
// SAFETY: see the `Send` impl above.
unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Sync for VacantEntry<'a, K, V> {}

impl<'a, K: Eq + Hash, V> VacantEntry<'a, K, V> {
    /// # Safety
    ///
    /// `slot` must have been returned by a *recent* call to
    /// `find_or_find_insert_slot(hash, ...)` on the same `RawTable` that
    /// `shard` protects, with no intervening operation that could cause a
    /// reallocation (eviction `remove`/`erase` are fine; `try_reserve`,
    /// `shrink_to`, or another `insert` invalidate the slot).
    pub(crate) unsafe fn new(
        shard: RwLockWriteGuard<'a, HashMap<K, V>>,
        key: K,
        hash: u64,
        slot: hashbrown::raw::InsertSlot,
    ) -> Self {
        Self {
            shard,
            key,
            hash,
            slot,
        }
    }

    /// Insert `value` at this vacant slot, running S3-FIFO eviction as needed.
    ///
    /// Requires `K: Clone` because the key must be stored both in the hashbrown
    /// table and in the eviction queue.
    pub fn insert(mut self, value: V) -> RefMut<'a, K, V>
    where
        K: Clone,
    {
        // Determine target location: ghost hit → main, otherwise → small.
        let loc = if self.shard.ghost_set.remove(&self.hash) {
            LOC_MAIN
        } else {
            LOC_SMALL
        };

        // Evict entries until we have capacity (no-op when shard_cap == 0).
        while self.shard.shard_cap > 0 && self.shard.total_live() >= self.shard.shard_cap {
            self.shard.evict_one();
        }

        let key_for_queue = self.key.clone();

        // SAFETY: `self.slot` was produced by `find_or_find_insert_slot`
        // when the `VacantEntry` was constructed (see `VacantEntry::new`'s
        // contract). Eviction — the only mutation between then and now —
        // calls only `remove`/`erase`, which do not reallocate the table
        // and therefore do not invalidate the slot. `as_ref` and
        // `RefMut::new` see a freshly-inserted entry under the held write
        // guard, satisfying their respective contracts.
        unsafe {
            let occupied = self.shard.map.insert_in_slot(
                self.hash,
                self.slot,
                (self.key, CacheEntry::new(value, loc)),
            );

            let (k, entry) = occupied.as_ref();

            // Register with the appropriate eviction queue.
            if loc == LOC_MAIN {
                self.shard.main_hashes.push_back(self.hash);
                self.shard.main_keys.push_back(key_for_queue);
                self.shard.main_live += 1;
            } else {
                self.shard.small_hashes.push_back(self.hash);
                self.shard.small_keys.push_back(key_for_queue);
                self.shard.small_live += 1;
            }

            RefMut::new(self.shard, k, entry.value.as_ptr())
        }
    }

    /// Insert `value` and return an `OccupiedEntry`, running eviction as needed.
    pub fn insert_entry(mut self, value: V) -> OccupiedEntry<'a, K, V>
    where
        K: Clone,
    {
        let loc = if self.shard.ghost_set.remove(&self.hash) {
            LOC_MAIN
        } else {
            LOC_SMALL
        };

        while self.shard.shard_cap > 0 && self.shard.total_live() >= self.shard.shard_cap {
            self.shard.evict_one();
        }

        let key_for_queue = self.key.clone();

        // SAFETY: see `VacantEntry::insert` above. The slot is still valid
        // because eviction only does `remove`/`erase`, which never
        // reallocate. `OccupiedEntry::new` is fed the bucket returned by
        // `insert_in_slot` and the same write guard, satisfying its
        // contract.
        unsafe {
            let bucket = self.shard.map.insert_in_slot(
                self.hash,
                self.slot,
                (self.key.clone(), CacheEntry::new(value, loc)),
            );

            if loc == LOC_MAIN {
                self.shard.main_hashes.push_back(self.hash);
                self.shard.main_keys.push_back(key_for_queue);
                self.shard.main_live += 1;
            } else {
                self.shard.small_hashes.push_back(self.hash);
                self.shard.small_keys.push_back(key_for_queue);
                self.shard.small_live += 1;
            }

            OccupiedEntry::new(self.shard, self.key, bucket)
        }
    }

    /// Consumes the entry and returns the key.
    pub fn into_key(self) -> K {
        self.key
    }

    /// Returns a reference to the key of the vacant entry.
    pub fn key(&self) -> &K {
        &self.key
    }
}

/// A view into an occupied entry in the map.
pub struct OccupiedEntry<'a, K, V> {
    shard: RwLockWriteGuard<'a, HashMap<K, V>>,
    bucket: hashbrown::raw::Bucket<(K, CacheEntry<V>)>,
    key: K,
}

// SAFETY: `OccupiedEntry` holds a write guard and a `Bucket` pointing
// into the locked shard's table. The write guard supplies exclusive
// access; the bucket is invalidated only by table operations that we do
// not perform between construction and consumption.
unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Send for OccupiedEntry<'a, K, V> {}
// SAFETY: see the `Send` impl above.
unsafe impl<'a, K: Eq + Hash + Sync, V: Sync> Sync for OccupiedEntry<'a, K, V> {}

impl<'a, K: Eq + Hash, V> OccupiedEntry<'a, K, V> {
    /// # Safety
    ///
    /// `bucket` must have been returned by a recent `find` /
    /// `find_or_find_insert_slot` / `insert_in_slot` call on the same
    /// `RawTable` that `shard` protects, and the underlying entry must
    /// not have been removed since.
    pub(crate) unsafe fn new(
        shard: RwLockWriteGuard<'a, HashMap<K, V>>,
        key: K,
        bucket: hashbrown::raw::Bucket<(K, CacheEntry<V>)>,
    ) -> Self {
        Self { shard, bucket, key }
    }

    /// Returns a shared reference to the value of the entry.
    pub fn get(&self) -> &V {
        // SAFETY: `self.bucket` is valid per `OccupiedEntry::new`'s
        // contract; the held write guard excludes every other accessor.
        unsafe { self.bucket.as_ref().1.value.get() }
    }

    /// Returns a mutable reference to the value of the entry.
    pub fn get_mut(&mut self) -> &mut V {
        // SAFETY: `&mut self` is unique by the borrow checker; the held
        // write guard excludes every other thread; together these provide
        // exclusive access to the entry.
        unsafe { self.bucket.as_mut().1.value.get_mut() }
    }

    /// Replaces the value of the entry and returns the old value.
    pub fn insert(&mut self, value: V) -> V {
        mem::replace(self.get_mut(), value)
    }

    /// Converts into a `RefMut` that holds the shard lock.
    pub fn into_ref(self) -> RefMut<'a, K, V> {
        // SAFETY: `self.bucket` is valid per `OccupiedEntry::new`'s
        // contract; the write guard moves into the returned `RefMut`,
        // satisfying `RefMut::new`'s contract that the pointers remain
        // valid for the guard's lifetime.
        unsafe {
            let (k, entry) = self.bucket.as_ref();
            RefMut::new(self.shard, k, entry.value.as_ptr())
        }
    }

    /// Consumes the entry and returns the key.
    pub fn into_key(self) -> K {
        self.key
    }

    /// Returns a reference to the key of the entry.
    pub fn key(&self) -> &K {
        // SAFETY: see `OccupiedEntry::get` above.
        unsafe { &self.bucket.as_ref().0 }
    }

    /// Removes the entry from the map and returns the value.
    pub fn remove(mut self) -> V {
        // SAFETY: `self.bucket` is valid (contract of `OccupiedEntry::new`)
        // and we hold the write lock; `as_ref` is a read of the entry's
        // metadata under exclusive access. `map.remove(bucket)` then
        // consumes the bucket; nothing else aliases it.
        let loc = unsafe { self.bucket.as_ref().1.loc };
        // SAFETY: same justification as the `as_ref` call above.
        let ((_k, entry), _) = unsafe { self.shard.map.remove(self.bucket) };
        // Update live counts for lazy-removal consistency.
        if loc == crate::shard::LOC_SMALL {
            self.shard.small_live = self.shard.small_live.saturating_sub(1);
        } else {
            self.shard.main_live = self.shard.main_live.saturating_sub(1);
        }
        entry.value.into_inner()
    }

    /// Removes the entry from the map and returns the `(key, value)` pair.
    pub fn remove_entry(mut self) -> (K, V) {
        // SAFETY: see `OccupiedEntry::remove` above; this method differs
        // only in returning the key as well.
        let loc = unsafe { self.bucket.as_ref().1.loc };
        // SAFETY: same as the `as_ref` call above.
        let ((k, entry), _) = unsafe { self.shard.map.remove(self.bucket) };
        if loc == crate::shard::LOC_SMALL {
            self.shard.small_live = self.shard.small_live.saturating_sub(1);
        } else {
            self.shard.main_live = self.shard.main_live.saturating_sub(1);
        }
        (k, entry.value.into_inner())
    }

    /// Replaces the value in-place and returns the old `(key, value)` pair.
    ///
    /// The eviction queue location (`small` or `main`) and frequency counter are
    /// preserved from the existing entry, so a hot entry that was promoted to the
    /// main queue stays there after replacement.
    pub fn replace_entry(self, value: V) -> (K, V) {
        // SAFETY: `self.bucket` is valid per `OccupiedEntry::new`'s
        // contract; the held write lock excludes any other accessor of
        // the entry, including any concurrent `bump_freq` from the read
        // path — so the `freq.load(Relaxed)` observes the latest value.
        let (orig_loc, orig_freq) = unsafe {
            let e = &self.bucket.as_ref().1;
            (e.loc, e.freq.load(core::sync::atomic::Ordering::Relaxed))
        };
        let new_entry = CacheEntry::new(value, orig_loc);
        new_entry
            .freq
            .store(orig_freq, core::sync::atomic::Ordering::Relaxed);
        // SAFETY: under the write lock no other thread can hold a
        // reference into the bucket; `bucket.as_mut()` is the only live
        // reference to the entry, so `mem::replace` swaps the pair
        // atomically with respect to any future reader.
        let (k, entry) = mem::replace(unsafe { self.bucket.as_mut() }, (self.key, new_entry));
        (k, entry.value.into_inner())
    }
}
