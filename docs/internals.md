# Internals

This document covers the implementation details that are not visible from the public API: the raw hashbrown usage, lock design, unsafe invariants, and the specific choices made during the DashMap fork.

## hashbrown raw API

`s3dashmap` uses hashbrown's `raw` feature, which gives direct access to `RawTable<T>`. The raw API is used in preference to the safe `HashMap<K, V>` wrapper because:

1. The value type in the table is `(K, CacheEntry<V>)`, not just `V`. The raw table allows any `T` in the bucket.
2. Eviction needs to call `remove` and `erase` on buckets found via `find`, without going through `HashMap`'s typed interface.
3. `find_or_find_insert_slot` returns an `InsertSlot` that can be used with `insert_in_slot` — this two-phase pattern avoids a second lookup in `VacantEntry::insert`.

### Operations used

| Operation | Where | Notes |
|-----------|-------|-------|
| `find(hash, eq)` | `get`, `remove`, `retain`, eviction | Returns `Option<Bucket<T>>` |
| `find_or_find_insert_slot(hash, eq, hasher)` | `_entry`, `_try_entry` | Returns `Result<Bucket<T>, InsertSlot>` |
| `insert_in_slot(hash, slot, value)` | `VacantEntry::insert` | Zero-cost insert after slot is known |
| `remove(bucket)` | `_remove`, `OccupiedEntry::remove`, eviction | Returns `(T, InsertSlot)` |
| `erase(bucket)` | `_retain` | Drop-in-place, no return |
| `iter()` | `Iter`, `IterMut`, `retain`, `ReadOnlyView::iter` | Returns `RawIter<T>` |
| `into_iter()` | `OwningIter` | Consumes table, returns `RawIntoIter<T>` |
| `try_reserve(n, hasher_fn)` | `try_reserve` | May reallocate |
| `shrink_to(n, hasher_fn)` | `shrink_to_fit` | May reallocate |
| `len()` | `_len`, `_shrink_to_fit` | O(1) |
| `capacity()` | `_capacity` | O(1) |

### InsertSlot stability during eviction

In `VacantEntry::insert`, the `InsertSlot` is computed by `find_or_find_insert_slot` before the eviction loop runs. After eviction, `insert_in_slot` is called with the same slot.

This is safe because hashbrown's `remove` and `erase` operations never trigger a resize. They only mark a slot as tombstone or empty. The table's backing allocation is unchanged, so all pre-computed slot pointers and offsets remain valid. A resize only occurs during `insert` or `try_reserve`.

### Hasher closure in raw calls

The raw API requires a `Fn(&T) -> u64` closure for rehashing during `try_reserve` and `shrink_to`. These are passed as:

```rust
self.map.try_reserve(additional, |(k, _v)| {
    let mut h = hasher.build_hasher();
    k.hash(&mut h);
    h.finish()
})
```

This closure is never called during normal lookup or eviction — only during allocation changes.

## Lock design

### RwLock internals

`lock.rs` is a direct copy of DashMap 6.1.0's custom `RwLock`, which is itself based on `parking_lot_core`. Key properties:

- **Compact state**: the entire lock state is a single `AtomicUsize`.
- **Fair**: uses two parking queues (readers and writers), preventing starvation.
- **Downgrade**: `RwLockWriteGuard::downgrade()` atomically converts to a read guard without releasing. This is implemented via a `parking_lot_core::unpark_all` call on the readers' queue, allowing waiting readers in while the (now-read) lock is still held.
- **Inline**: no heap allocation. The `RwLock` struct is `repr(C)` and fits in a single cache line slot alongside `CachePadded`.

### Why not `parking_lot::RwLock`

parking_lot's `RwLock` does not expose `downgrade()` as of writing. DashMap needed `downgrade` for `RefMut::downgrade()`, so it vendored its own implementation.

### Shard count and cache padding

```rust
Box<[CachePadded<RwLock<ShardData<K, V>>>]>
```

`CachePadded<T>` from crossbeam-utils pads `T` to the hardware cache line size (typically 64 bytes). Each shard's lock is therefore guaranteed to occupy a distinct cache line. Without this, a write to shard 0's atomic state word would invalidate the cache line containing shard 1's state on the same NUMA node, causing false sharing on every concurrent operation.

## Unsafe invariants

The codebase uses `unsafe` in several places. Each site has a specific invariant:

### `_get_read_shard`

```rust
unsafe fn _get_read_shard(&'a self, i: usize) -> &'a HashMap<K, V> {
    unsafe { &*self.shards.get_unchecked(i).data_ptr() }
}
```

**Invariant**: `i < shards.len()`. Guaranteed by `determine_shard` which computes `i` by shifting a hash value — the result is always in `[0, shard_count)` by construction. `debug_assert!` catches violations in debug builds.

`data_ptr()` returns the raw pointer to the `RwLock`'s contents (the `ShardData<K,V>` inside the lock's `UnsafeCell`). The resulting reference is valid for lifetime `'a` because `self` lives for `'a` and the contents are not moved.

**Note**: this is only sound when the caller guarantees no concurrent writer is active. `ReadOnlyView` uses it because the map is consumed (no more writers possible). The `Map` trait marks it `unsafe` to signal this.

### `Bucket::as_ref` / `as_mut`

Used extensively in `get`, `retain`, entry operations, and eviction. A `Bucket<T>` is a pointer into the hashbrown table's backing allocation.

**Invariant**: the bucket was obtained from a `find`/`iter` call on the same table, the table has not been reallocated since the bucket was obtained, and the entry has not been removed.

In `get` and `remove`, the bucket is obtained and used within a single lock scope. In `retain`, the `erase` call marks the slot as empty but does not reallocate — subsequent `iter` calls from the same `RawIter` are still valid because `RawIter` caches its own cursor.

### `VacantEntry::insert` — slot validity after eviction

```rust
// slot computed before eviction loop:
let slot = ...;
while total_live >= shard_cap { self.shard.evict_one(); }
// slot used here, after evictions:
let occupied = self.shard.map.insert_in_slot(self.hash, slot, (key, entry));
```

**Invariant**: eviction only calls `remove`/`erase`, never `try_reserve`. Therefore no reallocation occurs and the `InsertSlot` pointer remains valid.

### `RawIter` during `retain`

```rust
for bucket in shard.map.iter() {
    let (k, entry) = bucket.as_mut();
    if !f(&*k, entry.value.get_mut()) {
        shard.map.erase(bucket);
    }
}
```

**Invariant**: `erase` marks the slot as empty (tombstone) but does not move other entries or reallocate. `RawIter::next` skips empty slots. The iterator is not invalidated by erase.

### `SharedValue<T>` and `CacheEntry<V>`

`SharedValue<T>` wraps an `UnsafeCell<T>`. Shared references (`get()`) are created under a read lock; mutable references (`get_mut()`) under a write lock. The lock guarantees no aliasing between `&T` and `&mut T`.

`CacheEntry::freq` is `AtomicU8` precisely to make the read-under-read-lock bump safe without upgrading to a write lock. Multiple concurrent readers may call `fetch_update` simultaneously — `Relaxed` ordering is sufficient because the eviction decision (which reads `freq`) only happens under a write lock, establishing a happens-before with the atomic writes.

### `mem::take` in `OwningIter`

```rust
let raw_table = mem::take(&mut shard_wl.map);
drop(shard_wl);
self.current = Some(raw_table.into_iter());
```

`mem::take` replaces `shard_wl.map` with a default empty `RawTable`. The original table is moved out of the `ShardData` while the write lock is still held. The lock is then released. The extracted table is then iterated without any lock, which is sound because the `ShardData` inside the lock now has an empty table — no concurrent writer can find or access the moved-out entries.

## Frequency counter atomic ordering

`freq` uses `Ordering::Relaxed` for both load and store in `bump_freq` and `evict_from_small`/`evict_from_main`.

The reasoning:

- **On reads** (`bump_freq`): multiple threads may increment `freq` concurrently. The exact value seen by any reader is not critical — it only needs to be "approximately correct." If two readers both see 0 and both attempt to increment to 1, one will win and the other's CAS will fail. The entry keeps freq ≥ 1, which is the correct signal.
- **On eviction** (`evict_from_small`/`evict_from_main`): eviction runs under a write lock. No concurrent `bump_freq` can be in flight (the read lock is excluded). The write lock provides a sequentially consistent observation of all prior writes to `freq`. Relaxed is still sufficient because the write lock is the synchronization mechanism, not the atomic ordering.

Using `Relaxed` avoids unnecessary memory barrier instructions on x86 (where all atomic RMW operations are implicitly sequentially consistent anyway) and on ARM (where it saves a dmb instruction).

## `AbortOnPanic` in `util.rs`

```rust
struct AbortOnPanic;
impl Drop for AbortOnPanic {
    fn drop(&mut self) {
        if std::thread::panicking() { std::process::abort() }
    }
}
```

Used in `map_in_place_2`, which calls a user-provided closure to replace a value in-place. If the closure panics, the `ptr::write` that places the new value has not yet run, leaving the shard's hashbrown entry pointing to dropped memory. Rather than catching this with `catch_unwind` (which would require `V: UnwindSafe`), the guard promotes the panic to an abort. This is the same approach taken by DashMap.

## Fork delta from DashMap 6.1.0

The minimal changes from the original DashMap source:

| File | Change |
|------|--------|
| `Cargo.toml` | Removed `dashmap = "6"` dep, added vendored deps |
| `util.rs` | Added `CacheEntry<V>` struct |
| `shard.rs` | New file — `ShardData<K,V>` with all S3-FIFO logic |
| `lib.rs` | `type HashMap<K,V> = ShardData<K,V>`; all constructors take `cache_capacity`; `_get`/`_get_mut` call `bump_freq`; `_remove`/`_retain` decrement live counts; `_entry` uses `find_or_find_insert_slot`; `clear` calls `clear_all` |
| `mapref/entry.rs` | `VacantEntry::insert` runs ghost check + eviction loop; `OccupiedEntry::remove` decrements live counts; `K: Clone` added to insert methods |
| `t.rs` | `K: Clone` added to `Map` trait bounds |
| `read_only.rs` | `K: Clone` added to `Debug` impl |
| `iter.rs` | `GuardOwningIter`/`GuardIter` changed to use `CacheEntry<V>`; `mem::take(&mut shard_wl.map)` instead of whole shard |
