# Usage Guide

Practical patterns, common pitfalls, and recipes for `s3dashmap`.

## Choosing a capacity

The `cache_capacity` argument is the approximate maximum number of entries across all shards. "Approximate" because the actual ceiling is `shard_cap * shard_count`, where `shard_cap = ceil(cache_capacity / shard_count)`.

**Rule of thumb**: set `cache_capacity` to the maximum memory budget divided by the average entry size. For exact capacity bounds, use `with_shard_amount` with a shard count that divides your capacity evenly:

```rust
// 1 million entries, 8 shards → shard_cap = 125_000 exactly
let cache = S3DashMap::<String, Vec<u8>>::with_shard_amount(1_000_000, 8);
```

For most applications the default shard count is fine. On a 16-core machine it will be 64 shards, so `new(1_000_000)` gives a shard_cap of 15_625 and a true ceiling of 1_000_000 entries — the overshoot is 0 when 64 divides evenly.

## Sharing across threads

`S3DashMap` is `Send + Sync`, so wrap it in an `Arc` to share between threads:

```rust
use std::sync::Arc;
use s3dashmap::S3DashMap;

let cache: Arc<S3DashMap<String, Vec<u8>>> = Arc::new(S3DashMap::new(100_000));

let c = Arc::clone(&cache);
std::thread::spawn(move || {
    c.insert("key".to_string(), vec![1, 2, 3]);
});
```

## Do not hold guards across await points

`Ref` and `RefMut` hold a shard lock. Holding them across an `.await` means the shard is locked for the duration of the suspension — a classic deadlock recipe in async code.

```rust
// BAD: shard lock held across await
let r = cache.get("key").unwrap();
some_async_fn().await;       // ← lock still held here
println!("{}", *r);

// GOOD: extract the value and drop the guard
let value = cache.get("key").map(|r| r.value().clone());
some_async_fn().await;
```

Or use `view` to scope the lock explicitly:

```rust
let result = cache.view("key", |_k, v| expensive_computation(v));
some_async_fn().await;
```

## Do not hold two guards from the same shard

Acquiring two write guards from the same shard deadlocks. `get_mut` followed by `entry` on a key in the same shard will deadlock. If you need to touch multiple keys in one operation, use `alter_all` or `retain`, which iterate within a single lock scope.

```rust
// BAD: may deadlock if both keys hash to the same shard
let a = cache.get_mut("a").unwrap();
let b = cache.get_mut("b").unwrap();  // ← may deadlock

// GOOD: use separate scopes
{
    let mut a = cache.get_mut("a").unwrap();
    *a += 1;
}
{
    let mut b = cache.get_mut("b").unwrap();
    *b += 1;
}
```

## Counter pattern

Atomically increment a counter, inserting 0 as the default:

```rust
cache.entry(key)
    .and_modify(|v| *v += 1)
    .or_insert(1u64);
```

Equivalent but slightly less ergonomic:

```rust
*cache.entry(key).or_insert(0u64) += 1;
```

## Conditional remove

Remove an entry only if a condition on the value is met:

```rust
// Remove sessions that have expired
cache.remove_if(&session_id, |_k, v| v.expires_at < Instant::now());
```

## Batch load

Use `extend` or `from_iter` to populate from an existing collection. Note that eviction runs on each insert, so if the source collection is larger than `cache_capacity`, some entries will be evicted:

```rust
let data: Vec<(String, u64)> = load_data();
let cache: S3DashMap<String, u64> = S3DashMap::new(50_000);
cache.extend(data);
```

## Read-only snapshot

After all writes are complete, convert to a `ReadOnlyView` for zero-overhead reads:

```rust
let map: S3DashMap<u32, String> = build_lookup_table();
let view = map.into_read_only();

// No lock acquired on read
let val = view.get(&42);

// Iterate without any lock
for (k, v) in view.iter() {
    println!("{k}: {v}");
}

// Recover the map if needed
let map = view.into_inner();
```

## Non-blocking reads in hot loops

Use `try_get` to avoid blocking when a shard is temporarily write-locked. This is useful in tight polling loops where occasional misses are acceptable:

```rust
match cache.try_get(&key) {
    TryResult::Present(r) => process(r.value()),
    TryResult::Absent    => {}
    TryResult::Locked    => {} // retry later or skip
}
```

## Projection with `map`

`Ref::map` and `RefMut::map` allow projecting a guard onto a sub-value while keeping the lock held:

```rust
struct Record { name: String, score: u32 }

// Get a reference to just the `name` field
let name_ref: MappedRef<'_, u64, Record, String> = cache
    .get(&id)
    .unwrap()
    .map(|r| &r.name);

println!("{}", *name_ref);  // lock still held
```

Use `try_map` for fallible projections (e.g., downcasting):

```rust
let int_ref = cache.get(&key).unwrap().try_map(|v| v.as_int());
```

## Downgrading a write guard

After a write, downgrade to a read guard atomically — other writers cannot sneak in between:

```rust
let mut write_guard = cache.get_mut(&key).unwrap();
*write_guard = new_value;
let read_guard = write_guard.downgrade();  // atomically becomes read guard
// Other readers can now enter, but writers are still blocked
println!("{}", *read_guard);
```

## Iterating and modifying

`iter_mut` gives mutable access to values while iterating. To remove entries during iteration, use `retain` instead — you cannot call `remove` while holding an `iter_mut` guard.

```rust
// Multiply every value by 2
for mut entry in cache.iter_mut() {
    *entry *= 2;
}

// Remove entries where value > 100
cache.retain(|_k, v| *v <= 100);
```

## Consuming iteration (into_iter)

When you want to drain the map entirely:

```rust
let totals: u64 = cache.into_iter().map(|(_k, v)| v).sum();
// `cache` is moved; cannot be used after this
```

## Custom hasher

For deterministic testing or when ahash is not suitable:

```rust
use std::collections::hash_map::RandomState;
use s3dashmap::S3DashMap;

let cache: S3DashMap<String, u64, RandomState> =
    S3DashMap::with_capacity_and_hasher(1_000, RandomState::new());
```

Or with a fixed seed for reproducible behavior in tests:

```rust
use ahash::RandomState;

let hasher = RandomState::with_seeds(1, 2, 3, 4);
let cache: S3DashMap<u64, u64, RandomState> =
    S3DashMap::with_capacity_and_hasher(1_000, hasher);
```

## Using borrowed keys

Like `HashMap`, lookup methods accept any `Q` where `K: Borrow<Q>`. This means you can look up `String` keys with `&str`:

```rust
let cache: S3DashMap<String, u64> = S3DashMap::new(1_000);
cache.insert("hello".to_string(), 1);

let v = cache.get("hello");   // &str works directly
assert!(cache.contains_key("hello"));
cache.remove("hello");
```

## Understanding eviction behavior

Eviction is triggered inside `VacantEntry::insert`. **Replacing an existing key does not trigger eviction** — the entry count stays the same.

```rust
// Eviction may occur (new key)
cache.insert("new_key".to_string(), value);

// No eviction (key already exists)
cache.insert("existing_key".to_string(), new_value);
```

Entries accessed frequently (via `get` or `get_mut`) accumulate a frequency counter capped at 3. These entries are promoted to or kept in the Main queue and receive second-chance eviction. Entries accessed only once (freq = 0 when eviction reaches them) are evicted quickly.

You can observe the effect:

```rust
// This entry will be promoted to main and survive longer
for _ in 0..5 { let _ = cache.get("hot_key"); }

// This entry (freq=0) is likely to be evicted first under pressure
cache.insert("cold_key".to_string(), data);
```

## `K: Clone` requirement

All insertion operations (`insert`, `entry(...).or_insert(...)`, etc.) require `K: Clone`. The key is cloned once per insert to be stored in the eviction queue alongside the hashbrown table entry.

If `K` clone is expensive (e.g., long strings), consider:
- Wrapping in `Arc<str>` or `Arc<String>` — the clone is then a reference-count bump
- Using integer IDs as keys with a side table for the string

```rust
use std::sync::Arc;

let cache: S3DashMap<Arc<str>, Vec<u8>> = S3DashMap::new(100_000);
let key: Arc<str> = Arc::from("some_long_key");
cache.insert(Arc::clone(&key), data);  // clone is O(1)
```

## Memory overhead per entry

Each entry in the hashbrown table stores:
- `K` — the key
- `CacheEntry<V>`:
  - `V` — the value (inside `UnsafeCell`)
  - `AtomicU8` — frequency counter (1 byte + alignment padding)
  - `u8` — location flag (1 byte)

Additionally, each live entry has a `(u64, K)` pair in either the small or main queue. This is the cost of the eviction metadata: one hash plus one key clone per entry.

Total overhead vs a plain `HashMap<K, V>`: approximately `sizeof(u64) + sizeof(K)` per live entry for the queue slot, plus the ghost set entries for recently evicted keys.
