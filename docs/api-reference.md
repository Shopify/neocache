# API Reference

## `NeoCache<K, V, S = RandomState>`

The main concurrent map type. `K` and `V` are the key and value types. `S` is the hasher (defaults to `ahash::RandomState`).

**Trait bounds required by all operations:**
- `K: Eq + Hash + Clone`
- `V` has no bounds beyond what individual operations need

---

### Constructors

#### `NeoCache::new(cache_capacity: usize) -> Self`

Creates a map with S3-FIFO eviction enabled. `cache_capacity` is the approximate maximum number of live entries across all shards. When the map is full, inserting a new key evicts an existing entry.

```rust
let cache: NeoCache<String, Vec<u8>> = NeoCache::new(50_000);
```

#### `NeoCache::new_unbounded() -> Self`

Creates a map with no eviction limit. The map grows without bound. Equivalent to `NeoCache::new(0)`.

```rust
let map: NeoCache<u64, String> = NeoCache::new_unbounded();
```

#### `NeoCache::with_shard_amount(cache_capacity: usize, shard_amount: usize) -> Self`

Creates a map with a specific shard count. `shard_amount` must be a power of two greater than 1. Use this to control the capacity overshoot from ceiling division or to tune lock contention.

```rust
// Exact capacity: shard_cap = 256/4 = 64, total = 4*64 = 256.
let cache = NeoCache::<u64, u64>::with_shard_amount(256, 4);
```

#### `NeoCache::with_hasher(hasher: S) -> Self`

Creates an unbounded map with a custom hasher.

#### `NeoCache::with_capacity_and_hasher(cache_capacity: usize, hasher: S) -> Self`

Creates a bounded map with a custom hasher and the default shard count.

#### `NeoCache::with_hasher_and_shard_amount(hasher: S, shard_amount: usize) -> Self`

Creates an unbounded map with a custom hasher and specific shard count.

#### `NeoCache::with_capacity_and_hasher_and_shard_amount(cache_capacity: usize, hasher: S, shard_amount: usize) -> Self`

The fully-specified constructor. All other constructors delegate here.

#### `NeoCache::default() -> Self`

Creates an unbounded map. Requires `S: Default`.

---

### Insertion and removal

#### `fn insert(&self, key: K, value: V) -> Option<V>`

Inserts `key → value`. Returns the previous value if the key existed.

If the shard is at capacity, S3-FIFO eviction runs before the insert. This may remove one or more other entries.

```rust
let old = map.insert("key".to_string(), 42u32);
```

#### `fn remove<Q>(&self, key: &Q) -> Option<(K, V)>`

Removes the entry for `key`. Returns `Some((key, value))` if it existed. The eviction queue entry for this key becomes stale and will be lazily skipped during the next eviction sweep.

```rust
if let Some((k, v)) = map.remove("key") {
    println!("removed {k}: {v}");
}
```

#### `fn remove_if<Q>(&self, key: &Q, f: impl FnOnce(&K, &V) -> bool) -> Option<(K, V)>`

Removes the entry only if `f(k, v)` returns `true`.

#### `fn remove_if_mut<Q>(&self, key: &Q, f: impl FnOnce(&K, &mut V) -> bool) -> Option<(K, V)>`

Like `remove_if` but gives the predicate mutable access to the value.

#### `fn retain(&self, f: impl FnMut(&K, &mut V) -> bool)`

Removes all entries for which `f` returns `false`. Correctly decrements `small_live` / `main_live` for every removed entry.

#### `fn clear(&self)`

Removes all entries and resets all S3-FIFO queues to empty. More thorough than `retain(|_,_| false)` because it also clears the small/main/ghost queues and resets live counts.

---

### Lookup

#### `fn get<Q>(&'a self, key: &Q) -> Option<Ref<'a, K, V>>`

Returns a read guard for the entry if it exists. Increments the entry's frequency counter (saturating at 3).

The returned `Ref` holds the shard's read lock until dropped. Do not hold it across await points or while calling other map methods on the same shard.

```rust
if let Some(r) = map.get("key") {
    println!("{}", *r);  // Deref gives &V
}
```

#### `fn get_mut<Q>(&'a self, key: &Q) -> Option<RefMut<'a, K, V>>`

Returns a write guard for the entry if it exists. Increments the entry's frequency counter.

```rust
if let Some(mut r) = map.get_mut("key") {
    *r += 1;
}
```

#### `fn try_get<Q>(&'a self, key: &Q) -> TryResult<Ref<'a, K, V>>`

Non-blocking variant of `get`. Returns:
- `TryResult::Present(ref)` — entry exists, lock acquired
- `TryResult::Absent` — entry not found
- `TryResult::Locked` — shard write lock is held by another thread

#### `fn try_get_mut<Q>(&'a self, key: &Q) -> TryResult<RefMut<'a, K, V>>`

Non-blocking variant of `get_mut`.

#### `fn contains_key<Q>(&self, key: &Q) -> bool`

Returns `true` if the key exists. Equivalent to `get(key).is_some()`.

#### `fn view<Q, R>(&self, key: &Q, f: impl FnOnce(&K, &V) -> R) -> Option<R>`

Calls `f` with the key and value while holding the read lock, then releases the lock and returns the result. Useful when you only need a computed value and don't want to extend the guard's lifetime.

```rust
let len = map.view("key", |_k, v| v.len());
```

---

### In-place modification

#### `fn alter<Q>(&self, key: &Q, f: impl FnOnce(&K, V) -> V)`

Replaces the value in-place by calling `f(key, old_value) -> new_value`. Does nothing if the key does not exist.

```rust
map.alter("counter", |_k, v| v + 1);
```

#### `fn alter_all(&self, f: impl FnMut(&K, V) -> V)`

Calls `f` on every entry, replacing each value with the return value. Iterates shard by shard.

---

### Entry API

The entry API allows conditional insertion and modification without double-lookup.

#### `fn entry(&'a self, key: K) -> Entry<'a, K, V>`

Returns an `Entry` that holds the shard's write lock.

```rust
map.entry("hits".to_string())
    .and_modify(|v| *v += 1)
    .or_insert(1u64);
```

#### `fn try_entry(&'a self, key: K) -> Option<Entry<'a, K, V>>`

Non-blocking variant. Returns `None` if the shard write lock is held.

#### `enum Entry<'a, K, V>`

```rust
pub enum Entry<'a, K, V> {
    Occupied(OccupiedEntry<'a, K, V>),
    Vacant(VacantEntry<'a, K, V>),
}
```

**Entry methods:**

| Method | Description |
|--------|-------------|
| `key()` | Reference to the entry's key |
| `into_key()` | Consume entry, return owned key |
| `or_insert(value)` | Insert if vacant, return `RefMut` |
| `or_insert_with(f)` | Insert with lazy value if vacant |
| `or_try_insert_with(f)` | Like above, propagates `Result` |
| `or_default()` | Insert `V::default()` if vacant |
| `and_modify(f)` | Modify value if occupied, chain |
| `insert(value)` | Insert or replace value, return `RefMut` |
| `insert_entry(value)` | Insert or replace, return `OccupiedEntry` |

**VacantEntry::insert** is where S3-FIFO eviction runs (ghost hit check → eviction loop → hashbrown insert → queue push). `K: Clone` is required.

**OccupiedEntry methods:**

| Method | Description |
|--------|-------------|
| `get()` | `&V` |
| `get_mut()` | `&mut V` |
| `insert(value)` | Replace value, return old value |
| `into_ref()` | Convert to `RefMut` |
| `key()` | `&K` |
| `into_key()` | Owned `K` |
| `remove()` | Remove, return `V` |
| `remove_entry()` | Remove, return `(K, V)` |
| `replace_entry(value)` | Replace value in-place; preserves the entry's eviction queue location (`small`/`main`) and frequency counter; returns old `(K, V)` |

---

### Iteration

#### `fn iter(&'a self) -> Iter<'a, K, V, S>`

Returns an iterator of `RefMulti<'a, K, V>` items. Each item holds a reference-counted read guard for its shard. Items from the same shard share the same guard via `Arc`.

```rust
for entry in &map {
    let (k, v) = entry.pair();
    println!("{k:?}: {v:?}");
}
```

#### `fn iter_mut(&'a self) -> IterMut<'a, K, V, S>`

Returns an iterator of `RefMutMulti<'a, K, V>` items. Each item holds an `Arc<RwLockWriteGuard>`. Items from the same shard share the write guard — mutation of one item does not release the lock between items in the same shard.

#### `IntoIterator for NeoCache<K, V, S>` → `OwningIter<K, V, S>`

Consumes the map and yields `(K, V)` pairs. Acquires the write lock for each shard, extracts the raw table via `mem::take`, releases the lock, then drains the table.

```rust
for (k, v) in map {
    // map is moved here
}
```

---

### Capacity and metadata

#### `fn len(&self) -> usize`

Total number of live entries across all shards. Acquires each shard's read lock in sequence.

#### `fn is_empty(&self) -> bool`

Returns `true` if `len() == 0`.

#### `fn capacity(&self) -> usize`

The hashbrown table's allocated capacity (not the S3-FIFO eviction capacity). This is the number of entries that can be held before hashbrown reallocates. It grows with load factor demands.

#### `fn cache_capacity(&self) -> usize`

The S3-FIFO eviction capacity specified at construction time. Returns 0 for unbounded maps.

#### `fn shrink_to_fit(&self)`

Shrinks each shard's hashbrown table to match the number of live entries.

#### `fn try_reserve(&mut self, additional: usize) -> Result<(), TryReserveError>`

Pre-allocates `additional` slots in each shard's hashbrown table. Fails if any shard's allocation fails.

#### `fn hasher(&self) -> &S`

Returns a reference to the map's hasher.

---

### Conversion

#### `fn into_read_only(self) -> ReadOnlyView<K, V, S>`

Consumes the map and returns a `ReadOnlyView` that exposes only immutable access with no lock overhead.

---

### `ReadOnlyView<K, V, S>`

A wrapper around a consumed `NeoCache` that provides lock-free read access. Use after all mutation is complete.

| Method | Description |
|--------|-------------|
| `len()` | Total entries |
| `is_empty()` | `len() == 0` |
| `capacity()` | Hashbrown allocated capacity |
| `contains_key(key)` | Membership test |
| `get(key)` | `Option<&V>` — no lock |
| `get_key_value(key)` | `Option<(&K, &V)>` |
| `iter()` | `impl Iterator<Item = (&K, &V)>` |
| `keys()` | `impl Iterator<Item = &K>` |
| `values()` | `impl Iterator<Item = &V>` |
| `into_inner()` | Recover the `NeoCache` |

---

### Reference types

#### `Ref<'a, K, V>`

Immutable guard returned by `get`. Holds the shard's read lock.

| Method | |
|--------|-|
| `key()` | `&K` |
| `value()` | `&V` |
| `pair()` | `(&K, &V)` |
| `map(f)` | Project to `MappedRef<'a, K, V, T>` |
| `try_map(f)` | Fallible projection |
| `Deref` target | `V` |

#### `RefMut<'a, K, V>`

Mutable guard returned by `get_mut`. Holds the shard's write lock.

| Method | |
|--------|-|
| `key()` | `&K` |
| `value()` | `&V` |
| `value_mut()` | `&mut V` |
| `pair()` | `(&K, &V)` |
| `pair_mut()` | `(&K, &mut V)` |
| `downgrade()` | Atomically convert to `Ref` (no lock release) |
| `map(f)` | Project to `MappedRefMut<'a, K, V, T>` |
| `try_map(f)` | Fallible projection |
| `Deref` / `DerefMut` target | `V` |

#### `MappedRef<'a, K, V, T>` / `MappedRefMut<'a, K, V, T>`

Projections of `Ref`/`RefMut` onto a sub-value of type `T`. The shard lock is still held. Can be further projected with `map` / `try_map`.

#### `RefMulti<'a, K, V>` / `RefMutMulti<'a, K, V>`

Like `Ref`/`RefMut` but for iterators. The guard is `Arc`-wrapped so multiple items from the same shard can coexist.

---

### `TryResult<R>`

Returned by `try_get` / `try_get_mut`.

```rust
pub enum TryResult<R> {
    Present(R),  // entry found, lock acquired
    Absent,      // entry not found
    Locked,      // could not acquire the lock
}
```

Methods: `is_present()`, `is_absent()`, `is_locked()`, `unwrap()`, `expect(msg)`.

---

### Trait implementations

| Trait | Bounds | Notes |
|-------|--------|-------|
| `Clone` | `K: Clone, V: Clone, S: Clone` | Deep clone including queues |
| `Default` | `K: Eq+Hash+Clone, S: Default+BuildHasher+Clone` | Unbounded map |
| `Debug` | `K: Clone+Debug, V: Debug, S: BuildHasher+Clone` | Prints as a map |
| `IntoIterator` (by value) | `K: Clone` | Yields `(K, V)` |
| `IntoIterator` (by ref) | `K: Clone` | Yields `RefMulti` |
| `FromIterator<(K, V)>` | `K: Clone, S: Default` | Creates unbounded map |
| `Extend<(K, V)>` | `K: Clone` | Calls `insert` per pair |
| `Shr` (`>>`) | `K: Clone, K: Borrow<Q>` | `get(key).unwrap()` |
| `BitOr` (`\|`) | `K: Clone, K: Borrow<Q>` | `get_mut(key).unwrap()` |
| `Sub` (`-`) | `K: Clone, K: Borrow<Q>` | `remove(key)` |
| `BitAnd` (`&`) | `K: Clone, K: Borrow<Q>` | `contains_key(key)` |
| `Send` | `K,V,S: Send` | Automatic |
| `Sync` | `K,V,S: Send+Sync` | Automatic |
