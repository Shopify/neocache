//! Optional `serde` support for [`S3DashMap`](crate::S3DashMap).
//!
//! Enable with the `serde` feature flag.
use crate::S3DashMap;
use core::hash::{BuildHasher, Hash};
use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, Serializer};
use std::fmt;
use std::marker::PhantomData;

impl<K, V, S> Serialize for S3DashMap<K, V, S>
where
    K: Eq + Hash + Clone + Serialize,
    V: Serialize,
    S: BuildHasher + Clone,
{
    fn serialize<Ser: Serializer>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error> {
        let mut map = serializer.serialize_map(Some(self.len()))?;
        for r in self {
            let (k, v) = r.pair();
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}

struct S3DashMapVisitor<K, V, S> {
    marker: PhantomData<S3DashMap<K, V, S>>,
}

impl<'de, K, V, S> Visitor<'de> for S3DashMapVisitor<K, V, S>
where
    K: Eq + Hash + Clone + Deserialize<'de>,
    V: Deserialize<'de>,
    S: BuildHasher + Clone + Default,
{
    type Value = S3DashMap<K, V, S>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "a map")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut access: A) -> Result<Self::Value, A::Error> {
        let map = S3DashMap::with_hasher(S::default());
        while let Some((k, v)) = access.next_entry::<K, V>()? {
            map.insert(k, v);
        }
        Ok(map)
    }
}

impl<'de, K, V, S> Deserialize<'de> for S3DashMap<K, V, S>
where
    K: Eq + Hash + Clone + Deserialize<'de>,
    V: Deserialize<'de>,
    S: BuildHasher + Clone + Default,
{
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_map(S3DashMapVisitor {
            marker: PhantomData,
        })
    }
}
