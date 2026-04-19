use crate::util::CacheEntry;
use ahash::RandomState;
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::Ordering;
use std::collections::HashSet;
use std::collections::VecDeque;

/// Maximum value the `freq` counter saturates at.
///
/// Capped at 3 so that entries with a burst of popularity don't become
/// permanently immune to eviction once their access pattern changes.
pub(crate) const MAX_FREQ: u8 = 3;

/// [`CacheEntry::loc`](crate::util::CacheEntry) value: entry is in the small queue.
pub(crate) const LOC_SMALL: u8 = 0;

/// [`CacheEntry::loc`](crate::util::CacheEntry) value: entry is in the main queue (promoted from small).
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
    // Split into hash and key arrays for cache locality during stale-entry scanning.
    /// FIFO queue hashes (small queue, ~10% of capacity).
    pub(crate) small_hashes: VecDeque<u64>,
    /// FIFO queue keys (small queue).
    pub(crate) small_keys: VecDeque<K>,
    /// FIFO queue hashes (main queue, ~90% of capacity).
    pub(crate) main_hashes: VecDeque<u64>,
    /// FIFO queue keys (main queue).
    pub(crate) main_keys: VecDeque<K>,
    /// Hash set for O(1) ghost membership test (stores hashes, not keys).
    /// Cleared entirely when it exceeds `ghost_cap` instead of FIFO trimming.
    pub(crate) ghost_set: HashSet<u64, RandomState>,

    /// Number of live entries currently in `small` queue.
    pub(crate) small_live: usize,
    /// Number of live entries currently in `main` queue.
    pub(crate) main_live: usize,

    /// Per-shard eviction threshold (0 = no eviction).
    /// Per-shard eviction threshold (0 = no eviction).
    pub(crate) shard_cap: usize,
    /// Hard limit for the small queue (~10 % of `shard_cap`, minimum 1).
    pub(crate) small_cap: usize,
    /// Hard limit for the main queue (`shard_cap - small_cap`, minimum 1).
    pub(crate) main_cap: usize,
    /// Hard limit for the ghost set (`shard_cap` entries).
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
            small_hashes: VecDeque::new(),
            small_keys: VecDeque::new(),
            main_hashes: VecDeque::new(),
            main_keys: VecDeque::new(),
            ghost_set: HashSet::with_hasher(RandomState::new()),
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
            small_hashes: VecDeque::new(),
            small_keys: VecDeque::new(),
            main_hashes: VecDeque::new(),
            main_keys: VecDeque::new(),
            ghost_set: HashSet::with_hasher(RandomState::new()),
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
            small_hashes: self.small_hashes.clone(),
            small_keys: self.small_keys.clone(),
            main_hashes: self.main_hashes.clone(),
            main_keys: self.main_keys.clone(),
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
    #[cold]
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
        // Find the next live entry in the small queue (skip lazily-removed keys).
        // We capture the bucket from the first find to avoid a redundant second lookup.
        let (hash, key, bucket) = loop {
            match (self.small_hashes.pop_front(), self.small_keys.pop_front()) {
                (None, _) | (_, None) => {
                    self.evict_from_main();
                    return;
                }
                (Some(h), Some(k)) => {
                    if let Some(b) = self.map.find(h, |(mk, _)| mk == &k) {
                        break (h, k, b);
                    }
                }
            }
        };

        let freq = unsafe { bucket.as_ref().1.freq.load(Ordering::Relaxed) };

        if freq > 0 {
            unsafe {
                bucket.as_mut().1.loc = LOC_MAIN;
            }
            self.small_live -= 1;
            self.main_hashes.push_back(hash);
            self.main_keys.push_back(key);
            self.main_live += 1;
            // total_live is unchanged; the while loop handles main overflow.
        } else {
            // Evict: remove from map, add key to ghost.
            self.small_live -= 1;
            unsafe {
                self.map.remove(bucket);
            }
            self.add_to_ghost(hash);
        }
    }

    /// Process one entry from the main queue.
    ///
    /// * `freq > 0` → decrement freq by 1 and re-enqueue (second chance).
    /// * `freq == 0` → evict (total_live -= 1).
    fn evict_from_main(&mut self) {
        loop {
            // Capture the bucket from the first find to avoid a redundant second lookup.
            let (hash, key, bucket) = loop {
                match (self.main_hashes.pop_front(), self.main_keys.pop_front()) {
                    (None, _) | (_, None) => return,
                    (Some(h), Some(k)) => {
                        if let Some(b) = self.map.find(h, |(mk, _)| mk == &k) {
                            break (h, k, b);
                        }
                    }
                }
            };

            let freq = unsafe { bucket.as_ref().1.freq.load(Ordering::Relaxed) };

            if freq > 0 {
                unsafe {
                    bucket.as_ref().1.freq.store(freq - 1, Ordering::Relaxed);
                }
                self.main_hashes.push_back(hash);
                self.main_keys.push_back(key);
                // Continue the loop to find the next candidate.
            } else {
                // Evict.
                self.main_live -= 1;
                unsafe {
                    self.map.remove(bucket);
                }
                return;
            }
        }
    }

    /// Add a key hash to the ghost set, clearing it entirely if at capacity.
    #[cold]
    pub(crate) fn add_to_ghost(&mut self, hash: u64) {
        if self.ghost_set.len() >= self.ghost_cap {
            self.ghost_set.clear();
        }
        self.ghost_set.insert(hash);
    }

    /// Remove all cache entries and clear all eviction queues.
    #[cold]
    pub(crate) fn clear_all(&mut self) {
        // Safety: we own the write lock; no other references exist.
        unsafe {
            for bucket in self.map.iter() {
                self.map.erase(bucket);
            }
        }
        self.small_hashes.clear();
        self.small_keys.clear();
        self.main_hashes.clear();
        self.main_keys.clear();
        self.ghost_set.clear();
        self.small_live = 0;
        self.main_live = 0;
    }
}

// Implement `try_reserve` and `shrink_to` as pass-throughs with the correct
// hasher type. These are called from `NeoCache` impl blocks.
impl<K: Eq + Hash, V> ShardData<K, V> {
    pub(crate) fn map_try_reserve<S: BuildHasher>(
        &mut self,
        additional: usize,
        hasher: &S,
    ) -> Result<(), hashbrown::TryReserveError> {
        self.map
            .try_reserve(additional, |(k, _v)| hasher.hash_one(k))
    }

    pub(crate) fn map_shrink_to<S: BuildHasher>(&mut self, min_capacity: usize, hasher: &S) {
        self.map
            .shrink_to(min_capacity, |(k, _v)| hasher.hash_one(k));
    }
}
