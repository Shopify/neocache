# Architecture

## Module map

```
src/
├── lib.rs            NeoCache struct, public API, Map trait impl, IntoIterator, operators
├── shard.rs          ShardData<K,V>: hashbrown table + S3-FIFO queues + evict_one()
├── util.rs           SharedValue<T>, CacheEntry<V>, map_in_place_2
├── lock.rs           Custom RwLock built on parking_lot_core
├── try_result.rs     TryResult<R> enum (Present / Absent / Locked)
├── t.rs              Map trait (internal, mirrors DashMap's t.rs)
├── read_only.rs      ReadOnlyView<K,V,S>
└── mapref/
    ├── mod.rs        re-exports
    ├── one.rs        Ref, RefMut, MappedRef, MappedRefMut
    ├── multiple.rs   RefMulti, RefMutMulti (used by iterators)
    └── entry.rs      Entry, VacantEntry, OccupiedEntry
```

## Central type alias

```rust
// lib.rs
pub(crate) type HashMap<K, V> = ShardData<K, V>;
```

Every module in the codebase uses the name `HashMap<K, V>` in lock-guard signatures, iterator types, and trait definitions. This alias means none of that code needed structural changes from the DashMap original — they simply operate on `ShardData` without knowing it. The only module that knows about `ShardData` by name is `shard.rs` itself and the top-level `lib.rs`.

## NeoCache struct

```rust
pub struct NeoCache<K, V, S = RandomState> {
    shift:          usize,                              // for shard index calculation
    shards:         Box<[CachePadded<RwLock<HashMap<K, V>>>]>,
    hasher:         S,
    cache_capacity: usize,                              // 0 = unbounded
}
```

`shift` encodes the shard count as a bit-shift: `shard_index = hash >> shift`. With N shards (power of two), `shift = ptr_size_bits() - log2(N)`. This gives the top bits of the hash as the shard selector, which spreads keys more uniformly than a modulo.

`CachePadded` from `crossbeam-utils` adds padding so each shard's `RwLock` sits on its own cache line. Without this, writes to adjacent shards cause false sharing.

## ShardData struct

```rust
pub(crate) struct ShardData<K, V> {
    // hashbrown raw table
    pub(crate) map:        RawTable<(K, CacheEntry<V>)>,

    // S3-FIFO eviction queues
    pub(crate) small:      VecDeque<(u64, K)>,
    pub(crate) main:       VecDeque<(u64, K)>,
    pub(crate) ghost:      VecDeque<K>,
    pub(crate) ghost_set:  HashSet<K, RandomState>,

    // live counters (stale queue entries don't count)
    pub(crate) small_live: usize,
    pub(crate) main_live:  usize,

    // capacities set at construction time
    pub(crate) shard_cap:  usize,
    pub(crate) small_cap:  usize,
    pub(crate) main_cap:   usize,
    pub(crate) ghost_cap:  usize,
}
```

All S3-FIFO eviction state lives inside the same struct as the hashbrown table. This is the key architectural decision: it means the write lock that an insertion or removal already holds gives exclusive access to _both_ the table and the queues. No separate lock for the eviction queues is ever needed.

## CacheEntry

```rust
pub(crate) struct CacheEntry<V> {
    pub(crate) value: SharedValue<V>,   // UnsafeCell<V> wrapper
    pub(crate) freq:  AtomicU8,         // access counter, saturates at 3
    pub(crate) loc:   u8,               // LOC_SMALL or LOC_MAIN
}
```

This struct is stored inline in the hashbrown raw table as `(K, CacheEntry<V>)`. Storing eviction metadata inline rather than in a separate side-table means no secondary lookup is needed during eviction — the metadata is found in the same cache line as the key.

`freq` is `AtomicU8` because `get()` increments it under a read lock. Multiple concurrent readers may be incrementing `freq` on the same entry simultaneously. Relaxed ordering is sufficient: the exact value of `freq` is only decisive during eviction, which happens under a write lock when no readers are active.

`loc` is a plain `u8` because it is only written under a write lock. No atomic needed.

## Lock design

`lock.rs` contains a custom `RwLock` based on `parking_lot_core`. It exposes:

- `read()` → `RwLockReadGuard` — shared read access, blocks writers
- `write()` → `RwLockWriteGuard` — exclusive write access, blocks all others
- `try_read()` / `try_write()` → non-blocking variants returning `Option`
- `downgrade()` on `RwLockWriteGuard` → atomically converts to a read guard without releasing the lock (used in `RefMut::downgrade()`)

The `downgrade` operation is critical for the `RefMut::downgrade()` API: it allows a caller to atomically switch from holding a write lock to a read lock without ever dropping the shard lock, preventing any writer from sneaking in between.

## The Map trait

```rust
// t.rs
#[allow(private_interfaces)]
pub trait Map<'a, K: 'a + Eq + Hash + Clone, V: 'a, S: 'a + Clone + BuildHasher> {
    fn _shard_count(&self) -> usize;
    unsafe fn _get_read_shard(&'a self, i: usize) -> &'a HashMap<K, V>;
    unsafe fn _yield_read_shard(&'a self, i: usize) -> RwLockReadGuard<'a, HashMap<K, V>>;
    // ... 20+ methods
}
```

`Map` is an internal seam that lets `Iter`, `IterMut`, and `ReadOnlyView` be generic over any implementing type. This mirrors DashMap's design where `ReadOnlyView` is also generic over `Map`. The trait methods are prefixed with `_` and their names match DashMap's original — this makes diffs against the upstream trivial.

`#[allow(private_interfaces)]` suppresses the Rust lint that fires when a `pub` trait refers to a `pub(crate)` type (`ShardData`) in its method signatures. The trait is intentionally public (so `ReadOnlyView` can be generic over it) but callers outside the crate cannot name `ShardData`, so there is no practical API exposure.

## Shard selection

```rust
pub(crate) fn determine_shard(&self, hash: usize) -> usize {
    (hash << 7) >> self.shift
}
```

The `<< 7` rotation mixes the low bits of the hash before taking the top bits as the shard index. This prevents keys whose hashes differ only in low bits from all landing on shard 0. Combined with ahash's strong avalanche properties, it distributes keys uniformly across shards.

## Operator overloads

`NeoCache` implements four operators as shorthand:

| Operator | Equivalent |
|----------|-----------|
| `map >> key` (`Shr`) | `map.get(key).unwrap()` |
| `map \| key` (`BitOr`) | `map.get_mut(key).unwrap()` |
| `map - key` (`Sub`) | `map.remove(key)` |
| `map & key` (`BitAnd`) | `map.contains_key(key)` |

These mirror DashMap's operators and are included for compatibility. They panic on absent keys for the get variants.

## Iteration model

Three iterator types:

| Type | Borrows | Lock held per element |
|------|---------|----------------------|
| `Iter<'a, K, V>` | `&map` | `Arc<RwLockReadGuard>` for one shard at a time |
| `IterMut<'a, K, V>` | `&map` | `Arc<RwLockWriteGuard>` for one shard at a time |
| `OwningIter<K, V>` | consumes map | write lock taken, table extracted via `mem::take` |

The `Arc` wrapping of guards allows multiple `RefMulti`/`RefMutMulti` items from the same shard to coexist — each item holds a clone of the `Arc`, and the guard is dropped when the last reference is released.

`OwningIter` uses `mem::take(&mut shard_wl.map)` to extract just the raw hashbrown table from the shard. The S3-FIFO queues in `ShardData` are dropped with the write guard. The extracted `RawTable` is then consumed as a `RawIntoIter`, which moves `(K, CacheEntry<V>)` pairs out without any lock being held.

## ReadOnlyView

`ReadOnlyView<K, V, S>` wraps an `NeoCache` and exposes only read methods. It uses `_get_read_shard(i)` — which returns a raw pointer cast to `&'a ShardData` — rather than acquiring a lock, providing zero-lock-overhead reads when the caller can guarantee no concurrent mutations (e.g., after calling `into_read_only()` which consumes the map).
