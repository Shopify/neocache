//! Optional `rayon` parallel iteration support for [`NeoCache`].
//! Enable with the `rayon` feature flag.
use crate::NeoCache;
use core::hash::{BuildHasher, Hash};
use rayon::iter::{IntoParallelIterator, ParallelBridge};

impl<'a, K, V, S> IntoParallelIterator for &'a NeoCache<K, V, S>
where
    K: Eq + Hash + Clone + Send + Sync,
    V: Send + Sync,
    S: BuildHasher + Clone + Send + Sync,
{
    type Iter = rayon::iter::IterBridge<crate::iter::Iter<'a, K, V, S, NeoCache<K, V, S>>>;
    type Item = crate::mapref::multiple::RefMulti<'a, K, V>;

    fn into_par_iter(self) -> Self::Iter {
        self.iter().par_bridge()
    }
}
