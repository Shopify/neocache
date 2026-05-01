# The S3-FIFO Algorithm

## Background

S3-FIFO (_Simple, Scalable FIFO queues_) was published in 2023 by Juncheng Yang et al. The key finding is that FIFO queues — not LRU lists — can achieve lower miss ratios while being far cheaper to operate in concurrent environments. The paper demonstrates that over 90% of objects in typical production caches are only accessed once, making the complexity of LRU's "move to front on every access" unnecessary.

The full paper: _"FIFO Queues are All You Need for Cache Eviction"_ (SOSP 2023).
Reference implementation: https://s3fifo.com/

## Why S3-FIFO over LRU

| Property | LRU | S3-FIFO |
|----------|-----|---------|
| Miss ratio | Good | Equal or better on most traces |
| Write on read | Yes (pointer update) | No (atomic counter only) |
| Lock contention on read | Yes (list re-link) | None beyond the existing read lock |
| Scalability | Poor (global LRU list) | Per-shard — fully independent |
| Implementation complexity | High (doubly-linked list) | Low (three FIFO queues) |

LRU requires a write operation on every cache hit to move the entry to the list head. In a concurrent setting that means either a global write lock on every read, or a complex lock-free linked list. S3-FIFO avoids this entirely: reads only bump an atomic byte counter that is already inside the shard's read lock.

## Data structures

Each shard maintains three structures:

```
Small queue  (VecDeque<u64> + VecDeque<K>)  ← ~10% of shard_cap
Main queue   (VecDeque<u64> + VecDeque<K>)  ← ~90% of shard_cap
Ghost set    (HashSet<u64, ahash>)          ← ~100% of shard_cap
```

Each FIFO queue is stored as two parallel `VecDeque`s — one of `u64` hashes and one of `K` keys — rather than a single `VecDeque<(u64, K)>`. The eviction sweep skips stale entries by calling `shard.map.find(hash, ...)`, so iterating only the contiguous hash array is cache-friendly: stale skips do not pay for `K`-sized strides. When an entry is processed, the corresponding `K` is popped from the key array in lock-step.

The ghost set stores **hashes**, not keys. This avoids cloning the key on eviction and keeps ghost membership tests allocation-free. Membership is determined by `ghost_set.contains(&hash)` on insert; a hit promotes the new entry directly to the Main queue.

## Frequency counter

Each entry in the hashbrown table carries a `CacheEntry<V>`:

```rust
pub(crate) struct CacheEntry<V> {
    pub(crate) value: SharedValue<V>,  // the user's value
    pub(crate) freq:  AtomicU8,        // access frequency, saturates at MAX_FREQ (3)
    pub(crate) loc:   u8,              // LOC_SMALL (0) or LOC_MAIN (1)
}
```

`freq` is an `AtomicU8` so it can be incremented under a read lock — no promotion to a write lock is needed on `get()`. The saturation cap of 3 prevents a pathologically popular entry from being permanently immune to eviction after the access pattern changes.

`loc` is a plain `u8` because it is only written under a write lock (during promotion from small to main in `evict_from_small`).

## Insertion path

```
insert(key, value)
  │
  ├─ hash(key) → shard index
  ├─ acquire write lock on shard
  ├─ find_or_find_insert_slot(hash, ...)
  │    ├─ key found → OccupiedEntry::insert (replace value, no eviction)
  │    └─ key absent → VacantEntry::insert
  │         ├─ ghost_set.remove(key)?  → loc = LOC_MAIN : loc = LOC_SMALL
  │         ├─ while total_live >= shard_cap { evict_one() }
  │         ├─ insert (key, CacheEntry::new(value, loc)) into hashbrown table
  │         └─ push (hash, key.clone()) to small or main queue
  └─ release write lock
```

The `find_or_find_insert_slot` call from hashbrown returns an `InsertSlot` in the absent case. This slot remains valid after the eviction loop because hashbrown's `remove`/`erase` never triggers a resize that would invalidate a pre-computed insert slot.

## Eviction: `evict_one()`

`evict_one` processes a single logical unit — it may promote without reducing `total_live`. The caller loops until a slot is free:

```rust
while self.shard.shard_cap > 0 && self.shard.total_live() >= self.shard.shard_cap {
    self.shard.evict_one();
}
```

### Decision tree

```
evict_one()
  │
  ├─ small_live >= small_cap?
  │    YES → evict_from_small()
  │    NO  → evict_from_main()

evict_from_small()
  │
  ├─ pop front of small queue
  │    STALE (map.find returns None) → skip, try again
  │    LIVE  →
  │         ├─ freq > 0 → promote to main
  │         │    small_live -= 1
  │         │    main.push_back(hash, key)
  │         │    main_live  += 1
  │         │    (total_live unchanged; main may now need eviction)
  │         └─ freq == 0 → evict
  │              small_live -= 1
  │              map.remove(bucket)
  │              add_to_ghost(key)

evict_from_main()
  │
  └─ loop:
       pop front of main queue
         STALE → skip
         LIVE  →
              ├─ freq > 0 → second chance
              │    freq.fetch_update(f → f-1)
              │    main.push_back(hash, key)   ← re-enqueue at back
              │    (continue loop to find next candidate)
              └─ freq == 0 → evict
                   main_live -= 1
                   map.remove(bucket)
                   return
```

### Ghost set management

When an entry is evicted from small (freq == 0), its hash is added to the ghost set. The ghost set is bounded to `ghost_cap` entries (equal to `shard_cap`).

When the set is at capacity, the **entire set is cleared** before the new hash is inserted (see `ShardData::add_to_ghost`). This is a deliberate simplification of the policy described in the original S3-FIFO paper, which uses a FIFO-bounded ghost queue: clearing in bulk avoids maintaining a parallel insertion-order queue and keeps the eviction path allocation-free, at the cost of a hit-rate dip immediately after each clear. In practice the ghost set is sized to `shard_cap`, so a clear happens at most once per `shard_cap` distinct evictions per shard.

On the next insert of a ghost-hit key, the entry bypasses small and goes directly to main. This models the "recently seen but evicted" promotion that gives S3-FIFO its low miss ratio on re-access patterns. After a bulk clear, recently-evicted hashes that have not yet been re-inserted lose this promotion until they are re-evicted into the new ghost set.

## Lazy removal

When `remove()` is called explicitly:

1. The entry is removed from the hashbrown table immediately.
2. The appropriate live counter (`small_live` or `main_live`) is decremented.
3. The stale `(hash, key)` entry is **not** purged from the FIFO queue.

During the next eviction sweep, `map.find(hash, ...)` returns `None` for stale queue entries and they are silently skipped. This trades a small amount of queue memory for O(1) removal without queue scanning.

## Capacity model

```
total cache capacity = cache_capacity (user-specified)
shard_cap            = ceil(cache_capacity / shard_count)
small_cap            = ceil(shard_cap / 10)   (minimum 1)
main_cap             = shard_cap - small_cap  (minimum 1)
ghost_cap            = shard_cap

Maximum live entries = shard_cap * shard_count
                     ≤ cache_capacity + shard_count - 1
```

The overshoot from ceiling division is at most `shard_count - 1`. With the default shard count of `4 * logical_cpus` (rounded to next power of two) and a 1 million entry capacity, the overshoot is at most ~63 entries — negligible in practice.

To eliminate overshoot, use `with_shard_amount(capacity, n)` where `n` divides `capacity` evenly.
