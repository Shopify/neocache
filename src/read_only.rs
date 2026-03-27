use crate::lock::RwLock;
use crate::t::Map;
use crate::{HashMap, S3DashMap};
use ahash::RandomState;
use core::borrow::Borrow;
use core::fmt;
use core::hash::{BuildHasher, Hash};
use crossbeam_utils::CachePadded;

/// A read-only view into a `S3DashMap`.
///
/// Obtained via [`S3DashMap::into_read_only`]. All reads are lock-free once
/// the view is constructed, because the underlying map can no longer be written.
pub struct ReadOnlyView<K, V, S = RandomState> {
    pub(crate) map: S3DashMap<K, V, S>,
}

impl<K: Eq + Hash + Clone + Clone, V: Clone, S: Clone> Clone for ReadOnlyView<K, V, S> {
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
    pub(crate) fn new(map: S3DashMap<K, V, S>) -> Self {
        Self { map }
    }

    /// Recovers the inner `S3DashMap`.
    pub fn into_inner(self) -> S3DashMap<K, V, S> {
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
        let hash = self.map.hash_u64(&key);
        let idx = self.map.determine_shard(hash as usize);
        let shard = unsafe { self.map._get_read_shard(idx) };

        shard
            .map
            .find(hash, |(k, _entry)| key == k.borrow())
            .map(|b| {
                let (k, entry) = unsafe { b.as_ref() };
                (k, entry.value.get())
            })
    }

    /// Iterates over all `(&key, &value)` pairs without acquiring any locks.
    pub fn iter(&'a self) -> impl Iterator<Item = (&'a K, &'a V)> + 'a {
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
