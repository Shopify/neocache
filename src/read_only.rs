use crate::lock::RwLock;
use crate::t::Map;
use crate::{HashMap, NeoCache};
use ahash::RandomState;
use core::borrow::Borrow;
use core::fmt;
use core::hash::{BuildHasher, Hash};
use crossbeam_utils::CachePadded;

/// A read-only view into a `NeoCache`.
///
/// Obtained via [`NeoCache::into_read_only`]. All reads are lock-free once
/// the view is constructed, because the underlying map can no longer be written.
pub struct ReadOnlyView<K, V, S = RandomState> {
    pub(crate) map: NeoCache<K, V, S>,
}

impl<K: Eq + Hash + Clone, V: Clone, S: Clone> Clone for ReadOnlyView<K, V, S> {
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
        }
    }
}

impl<K: Eq + Hash + Clone + fmt::Debug, V: fmt::Debug, S: BuildHasher + Clone> fmt::Debug
    for ReadOnlyView<K, V, S>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.map.fmt(f)
    }
}

impl<K, V, S> ReadOnlyView<K, V, S> {
    pub(crate) fn new(map: NeoCache<K, V, S>) -> Self {
        Self { map }
    }

    /// Recovers the inner `NeoCache`.
    pub fn into_inner(self) -> NeoCache<K, V, S> {
        self.map
    }
}

impl<'a, K: 'a + Eq + Hash + Clone, V: 'a, S: BuildHasher + Clone> ReadOnlyView<K, V, S> {
    /// Returns the number of entries in the view.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if the view contains no entries.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns the current hashbrown allocation capacity.
    pub fn capacity(&self) -> usize {
        self.map.capacity()
    }

    /// Returns `true` if the view contains an entry for `key`.
    pub fn contains_key<Q>(&'a self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key).is_some()
    }

    /// Returns a shared reference to the value for `key`, or `None`.
    pub fn get<Q>(&'a self, key: &Q) -> Option<&'a V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get_key_value(key).map(|(_k, v)| v)
    }

    /// Returns a `(&key, &value)` pair for `key`, or `None`.
    pub fn get_key_value<Q>(&'a self, key: &Q) -> Option<(&'a K, &'a V)>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        // Pass `key` (not `&key`) so the lookup hash matches what `_insert`
        // produced for the same logical key. See `NeoCache::hash_u64`.
        let hash = self.map.hash_u64(key);
        let idx = self.map.determine_shard(hash as usize);
        // SAFETY: `idx < shards.len()` by construction — `determine_shard`
        // produces an index in `[0, shard_count)`. `ReadOnlyView` consumes
        // the underlying `NeoCache`, so no writer to this shard can exist;
        // the lock-free `_get_read_shard` is therefore sound.
        let shard = unsafe { self.map._get_read_shard(idx) };

        shard
            .map
            .find(hash, |(k, _entry)| key == k.borrow())
            .map(|b| {
                // SAFETY: `b` was just returned by `find` on this same
                // table; no other code is mutating the table because
                // `ReadOnlyView` does not expose any writer API.
                let (k, entry) = unsafe { b.as_ref() };
                (k, entry.value.get())
            })
    }

    /// Iterates over all `(&key, &value)` pairs without acquiring any locks.
    pub fn iter(&'a self) -> impl Iterator<Item = (&'a K, &'a V)> + 'a {
        // SAFETY: shard indices `0..shard_count` are valid by construction;
        // `ReadOnlyView` precludes any writer, so the lock-free shard access
        // and the raw `RawTable::iter()` cursor are not racing with any
        // mutation. Each `Bucket::as_ref` call dereferences a bucket that
        // came directly from `iter()` on the same (unmodified) table.
        unsafe {
            (0..self.map._shard_count())
                .map(move |shard_i| self.map._get_read_shard(shard_i))
                .flat_map(|shard| shard.map.iter())
                .map(|b| {
                    let (k, entry) = b.as_ref();
                    (k, entry.value.get())
                })
        }
    }

    /// Iterates over all keys without acquiring any locks.
    pub fn keys(&'a self) -> impl Iterator<Item = &'a K> + 'a {
        self.iter().map(|(k, _v)| k)
    }

    /// Iterates over all values without acquiring any locks.
    pub fn values(&'a self) -> impl Iterator<Item = &'a V> + 'a {
        self.iter().map(|(_k, v)| v)
    }

    #[allow(dead_code)]
    pub(crate) fn shards(&self) -> &[CachePadded<RwLock<HashMap<K, V>>>] {
        &self.map.shards
    }
}
