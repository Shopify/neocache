use crate::util::CacheEntry;
use core::hash::{BuildHasher, Hash, Hasher};
use core::sync::atomic::Ordering;
use std::collections::VecDeque;

/// Identity hasher for u64 values that are already well-distributed hash outputs.
/// Avoids the overhead of re-hashing through ahash/SipHash.
pub(crate) struct IdentityHasher(u64);

impl Hasher for IdentityHasher {
    #[inline]
    fn finish(&self) -> u64 { self.0 }
    fn write(&mut self, _: &[u8]) { unreachable!("IdentityHasher only supports u64") }
    #[inline]
    fn write_u64(&mut self, n: u64) { self.0 = n; }
}

#[derive(Clone)]
pub(crate) struct IdentityBuildHasher;

impl BuildHasher for IdentityBuildHasher {
    type Hasher = IdentityHasher;
    #[inline]
    fn build_hasher(&self) -> IdentityHasher { IdentityHasher(0) }
}

pub(crate) const MAX_FREQ: u8 = 7;
pub(crate) const LOC_SMALL: u8 = 0;
pub(crate) const LOC_MAIN: u8 = 1;

/// The type stored inside each shard's `RwLock`. Contains the raw hashbrown
/// table plus all S3-FIFO eviction state for this shard.
///
/// Using `type HashMap<K, V> = ShardData<K, V>` lets the rest of the codebase
/// (iter, entry, t.rs) keep DashMap-style type names while operating on our
/// extended shard struct.
pub(crate) struct ShardData<K, V> {
    /// The actual key-value store (hashbrown raw table).
    pub(crate) map: hashbrown::raw::RawTable<(K, CacheEntry<V>)>,

    // ---- S3-FIFO eviction state ----
    /// FIFO queue of newly inserted entries (~10% of capacity).
    /// Stores only hash values — entries are found via hash_check fingerprint.
    pub(crate) small: VecDeque<u64>,
    /// FIFO queue with second-chance eviction (~90% of capacity).
    /// Stores only hash values — entries are found via hash_check fingerprint.
    pub(crate) main: VecDeque<u64>,
    /// FIFO queue of recently evicted hash values (ghost set).
    pub(crate) ghost: VecDeque<u64>,
    /// Hash set for O(1) ghost membership test (stores hash values, not keys).
    /// Uses identity hasher since values are already well-distributed ahash outputs.
    pub(crate) ghost_set: hashbrown::HashSet<u64, IdentityBuildHasher>,

    /// Number of live entries currently in `small` queue.
    pub(crate) small_live: usize,
    /// Number of live entries currently in `main` queue.
    pub(crate) main_live: usize,

    /// Per-shard eviction threshold (0 = no eviction).
    pub(crate) shard_cap: usize,
    pub(crate) small_cap: usize,
    pub(crate) main_cap: usize,
    pub(crate) ghost_cap: usize,
}

impl<K, V> ShardData<K, V> {
    /// Construct an empty shard.
    ///
    /// `map_cap` is the initial hashbrown pre-allocation.
    /// `shard_cap` is the S3-FIFO eviction capacity for this shard (0 = disabled).
    pub(crate) fn new(map_cap: usize, shard_cap: usize) -> Self {
        let (small_cap, main_cap, ghost_cap) = if shard_cap == 0 {
            (0, 0, 0)
        } else {
            let s = shard_cap.div_ceil(10).max(1);
            let m = shard_cap.saturating_sub(s).max(1);
            (s, m, shard_cap)
        };
        Self {
            map: hashbrown::raw::RawTable::with_capacity(map_cap),
            // Pre-allocate queues to avoid reallocation during steady-state.
            // Over-allocate by 2x to account for stale entries from lazy removal.
            small: VecDeque::with_capacity(small_cap.saturating_mul(2)),
            main: VecDeque::with_capacity(main_cap.saturating_mul(2)),
            ghost: VecDeque::with_capacity(ghost_cap),
            ghost_set: hashbrown::HashSet::with_capacity_and_hasher(ghost_cap, IdentityBuildHasher),
            small_live: 0,
            main_live: 0,
            shard_cap,
            small_cap,
            main_cap,
            ghost_cap,
        }
    }

    /// Total live entries (small + main queues).
    #[inline]
    pub(crate) fn total_live(&self) -> usize {
        self.small_live + self.main_live
    }

    /// Proxy for the raw table's `len()`.
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.map.len()
    }

    /// Proxy for the raw table's `capacity()`.
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.map.capacity()
    }
}

impl<K, V> Default for ShardData<K, V> {
    fn default() -> Self {
        Self {
            map: hashbrown::raw::RawTable::new(),
            small: VecDeque::new(),
            main: VecDeque::new(),
            ghost: VecDeque::new(),
            ghost_set: hashbrown::HashSet::with_hasher(IdentityBuildHasher),
            small_live: 0,
            main_live: 0,
            shard_cap: 0,
            small_cap: 0,
            main_cap: 0,
            ghost_cap: 0,
        }
    }
}

impl<K: Clone + Eq + Hash, V: Clone> Clone for ShardData<K, V> {
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
            small: self.small.clone(),
            main: self.main.clone(),
            ghost: self.ghost.clone(),
            ghost_set: self.ghost_set.clone(),
            small_live: self.small_live,
            main_live: self.main_live,
            shard_cap: self.shard_cap,
            small_cap: self.small_cap,
            main_cap: self.main_cap,
            ghost_cap: self.ghost_cap,
        }
    }
}

// S3-FIFO eviction methods — only available when K can be used as a map key.
impl<K: Clone + Eq + Hash, V> ShardData<K, V> {
    /// Evict a single logical slot:
    ///
    /// * If small is at or above its capacity, process one entry from small.
    /// * Otherwise, process one entry from main.
    ///
    /// Note: this may promote (small→main) without reducing `total_live`.
    /// The caller must loop (`while total_live >= shard_cap { evict_one() }`)
    /// to guarantee a free slot.
    #[inline]
    pub(crate) fn evict_one(&mut self) {
        if self.small_live >= self.small_cap {
            self.evict_from_small();
        } else {
            self.evict_from_main();
        }
    }

    /// Process one entry from the small queue.
    ///
    /// * `freq > 0` → promote to main (total_live unchanged; outer while loop
    ///   will call evict_from_main next time if main is now over capacity).
    /// * `freq == 0` → evict to ghost (total_live -= 1).
    ///
    /// Falls through to `evict_from_main` if small is empty.
    fn evict_from_small(&mut self) {
        // Hash-only queues: find entries via hash_check fingerprint in CacheEntry.
        // No key clones needed. Fingerprint collisions are <0.1% per lookup.
        loop {
            let hash = match self.small.pop_front() {
                None => {
                    self.evict_from_main();
                    return;
                }
                Some(h) => h,
            };

            let hc = hash as u16;
            let bucket = match self.map.find(hash, |(_, e)| e.hash_check == hc && e.loc == LOC_SMALL) {
                Some(b) => b,
                None => continue, // Stale or fingerprint miss, skip.
            };

            let freq = unsafe { bucket.as_ref().1.freq.load(Ordering::Relaxed) };

            if freq > 0 {
                // Promote to main queue.
                unsafe { bucket.as_mut().1.loc = LOC_MAIN; }
                self.small_live -= 1;
                self.main.push_back(hash);
                self.main_live += 1;
            } else {
                // Evict: remove from map, add hash to ghost.
                self.small_live -= 1;
                unsafe { self.map.remove(bucket); }
                self.add_to_ghost(hash);
            }
            return;
        }
    }

    /// Process one entry from the main queue.
    ///
    /// * `freq > 0` → decrement freq by 1 and re-enqueue (second chance).
    /// * `freq == 0` → evict (total_live -= 1).
    fn evict_from_main(&mut self) {
        loop {
            let hash = match self.main.pop_front() {
                None => return,
                Some(h) => h,
            };

            let hc = hash as u16;
            let bucket = match self.map.find(hash, |(_, e)| e.hash_check == hc && e.loc == LOC_MAIN) {
                Some(b) => b,
                None => continue, // Stale or fingerprint miss, skip.
            };

            let freq = unsafe { bucket.as_ref().1.freq.load(Ordering::Relaxed) };

            if freq > 0 {
                // Second chance: decrement freq and re-enqueue at the back.
                unsafe { bucket.as_ref().1.freq.store(freq - 1, Ordering::Relaxed); }
                self.main.push_back(hash);
            } else {
                // Evict.
                self.main_live -= 1;
                unsafe { self.map.remove(bucket); }
                return;
            }
        }
    }

    /// Add a hash to the ghost set, trimming the oldest if at capacity.
    /// Stores only the hash value (u64) instead of the full key, eliminating
    /// key cloning. False positives from hash collisions are benign — they
    /// only affect whether an entry starts in small vs main queue.
    pub(crate) fn add_to_ghost(&mut self, hash: u64) {
        while self.ghost.len() >= self.ghost_cap {
            if let Some(old) = self.ghost.pop_front() {
                self.ghost_set.remove(&old);
            }
        }
        self.ghost_set.insert(hash);
        self.ghost.push_back(hash);
    }

    /// Remove all cache entries and clear all eviction queues.
    pub(crate) fn clear_all(&mut self) {
        // Safety: we own the write lock; no other references exist.
        unsafe {
            for bucket in self.map.iter() {
                self.map.erase(bucket);
            }
        }
        self.small.clear();
        self.main.clear();
        self.ghost.clear();
        self.ghost_set.clear();
        self.small_live = 0;
        self.main_live = 0;
    }
}

// Implement `try_reserve` and `shrink_to` as pass-throughs with the correct
// hasher type. These are called from `S3DashMap` impl blocks.
impl<K: Eq + Hash, V> ShardData<K, V> {
    pub(crate) fn map_try_reserve<S: BuildHasher>(
        &mut self,
        additional: usize,
        hasher: &S,
    ) -> Result<(), hashbrown::TryReserveError> {
        self.map.try_reserve(additional, |(k, _v)| hasher.hash_one(k))
    }

    pub(crate) fn map_shrink_to<S: BuildHasher>(&mut self, min_capacity: usize, hasher: &S) {
        self.map.shrink_to(min_capacity, |(k, _v)| hasher.hash_one(k));
    }
}
