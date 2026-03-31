# neocache

A low-latency concurrent hash map with [S3-FIFO](https://s3fifo.com/) cache eviction, forked from [DashMap](https://github.com/xacrimon/dashmap) 6.1.0.

S3-FIFO is a simple, scalable eviction policy that outperforms LRU in miss-ratio across a wide range of access patterns while being cheaper to implement: it uses only FIFO queues and a small ghost set.

## How it works

`NeoCache` is a sharded concurrent hash map. Each shard is an `RwLock<ShardData>` where `ShardData` holds both the raw hashbrown table and the complete S3-FIFO eviction state for that shard. There is **no global eviction lock** — eviction is concurrent with the same granularity as reads and writes.

### S3-FIFO algorithm

Each shard maintains three structures:

| Structure | Size | Purpose |
|-----------|------|---------|
| Small queue | ~10% of shard capacity | New entries enter here |
| Main queue | ~90% of shard capacity | Hot entries (second-chance eviction) |
| Ghost set | ~100% of shard capacity | Keys of recently evicted entries |

**Insertion:** new entries go to the Small queue. If the key was recently evicted (ghost hit), it goes directly to Main instead.

**Eviction from Small:** if the entry's frequency counter is > 0, it is promoted to Main (freq ≥ 1 indicates it was accessed at least once). If freq == 0, it is evicted and its key is added to the ghost set.

**Eviction from Main:** entries with freq > 0 get a second chance — freq is decremented and the entry is re-enqueued at the back of Main. Entries with freq == 0 are evicted.

**Frequency tracking:** reads call `bump_freq()` under the existing read lock, saturating at 3. No extra locking is needed.

## Usage

```toml
[dependencies]
neocache = { path = "..." }
```

```rust
use neocache::NeoCache;

// Create a map that evicts entries beyond 10_000 items.
let cache: NeoCache<String, Vec<u8>> = NeoCache::new(10_000);

cache.insert("key".to_string(), vec![1, 2, 3]);

if let Some(v) = cache.get("key") {
    println!("{:?}", *v);
}

cache.remove("key");
```

### Unbounded map (no eviction)

```rust
# use neocache::NeoCache;
let map: NeoCache<u64, u64> = NeoCache::new_unbounded();
```

### Custom shard count

The shard count must be a power of two greater than 1. Choosing a count that evenly divides your capacity gives a tighter bound on the maximum live entries.

```rust
# use neocache::NeoCache;
// 4 shards, 16 entries per shard, total capacity exactly 64.
let cache = NeoCache::<u64, u64>::with_shard_amount(64, 4);
```

### Custom hasher

```rust
use neocache::NeoCache;
use std::collections::hash_map::RandomState;

let cache: NeoCache<String, u64, RandomState> =
    NeoCache::with_capacity_and_hasher(1_000, RandomState::new());
```

### Entry API

```rust
# use neocache::NeoCache;
# let cache: NeoCache<String, u64> = NeoCache::new(100);
cache.entry("counter".to_string())
    .and_modify(|v| *v += 1)
    .or_insert(0);
```

### Iteration

```rust
# use neocache::NeoCache;
# let cache: NeoCache<String, u64> = NeoCache::new(100);
for r in &cache {
    let (k, v) = r.pair();
    println!("{k}: {v:?}");
}
```

## Design notes

### Keys must be `Clone`

`K: Clone` is required for `insert` and `entry(...).or_insert(...)` because each key must be stored in two places: the hashbrown table and the eviction queue (as a `(hash, K)` pair). The clone happens once per insertion.

### Capacity is approximate

The total capacity guarantee is `shard_cap * shard_count`, where `shard_cap = ceil(capacity / shard_count)`. With the default shard count (4 × logical CPUs, rounded to the next power of two) and a capacity of `N`, the map may hold up to `N + shard_count - 1` live entries. Use `with_shard_amount` with a power-of-two shard count that divides your capacity evenly for an exact bound.

### Lazy removal

`remove()` decrements the live counter for the appropriate queue but does not purge stale entries from the FIFO queues. The eviction sweep skips them via a hashbrown `find()` liveness check. This avoids the cost of scanning queues on every removal.

### No allocation on read

Reads (`get`, `contains_key`, iteration) hold the per-shard read lock and bump the frequency counter atomically. No allocation or queue modification occurs on the read path.

## API surface

`NeoCache` mirrors the DashMap 6.1.0 API. The main additions are the constructors that accept a `cache_capacity` argument:

| Constructor | Description |
|-------------|-------------|
| `new(capacity)` | Default shard count, eviction enabled |
| `new_unbounded()` | Default shard count, no eviction |
| `with_shard_amount(capacity, n)` | Custom shard count, eviction enabled |
| `with_hasher(hasher)` | Custom hasher, no eviction |
| `with_capacity_and_hasher(capacity, hasher)` | Custom hasher, eviction enabled |
| `with_hasher_and_shard_amount(hasher, n)` | Custom hasher and shard count, no eviction |
| `with_capacity_and_hasher_and_shard_amount(capacity, hasher, n)` | Full control |

All DashMap methods are available: `insert`, `get`, `get_mut`, `remove`, `remove_if`, `contains_key`, `entry`, `try_entry`, `iter`, `iter_mut`, `retain`, `alter`, `alter_all`, `len`, `is_empty`, `capacity`, `clear`, `shrink_to_fit`, `try_reserve`, `into_iter`, `into_read_only`, and the `try_get` / `try_get_mut` non-blocking variants.

## Dependencies

| Crate | Role |
|-------|------|
| `hashbrown` | Raw hash table (with `raw` feature) |
| `ahash` | Default hasher |
| `parking_lot_core` | Custom reader-writer lock |
| `crossbeam-utils` | Cache-line padding for shards |
| `lock_api` | Lock trait abstractions |
| `once_cell` | One-time initialization of default shard count |

## License

MIT
