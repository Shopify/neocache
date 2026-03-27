# Ideas Backlog

## Remaining (require architectural changes)
- **Full RwLock state redesign**: Separate WRITER_BIT enables fetch_add reads (~12% faster atomics, ~5% overall). Requires rewriting all lock/unlock paths + downgrade logic. Major effort.
- **Lock-free read path**: Epoch-based reclamation or hazard pointers. Eliminates read lock entirely (~30% theoretical gain). Massive unsafe code effort.

## Exhaustively tried and failed — do NOT retry
- Fewer/more shards (32, 256): contention vs variance tradeoff, 128 is optimal
- #[inline] hints, unreachable_unchecked, repr(C) on CacheEntry
- Pre-clone key before lock, early drop of write lock guard
- Ghost cap 2×, small queue 5-7%, epoch-based freq decay
- MAX_FREQ 5/15 (7 is optimal), freq reset to 0, freq=MAX on promotion
- Shard routing low-bits mask, u32 fingerprint
- Direct insert() vs find_or_find_insert_slot
- 2× hash table pre-allocation (probe depth not bottleneck)
- Triple-try read lock (double is optimal), spin_loop between retries
- Double-try write lock (writers wait ~40ns for readers)
- erase() vs remove() in eviction (only 3% of ops)
- fetch_add read lock (blocked by ONE_WRITER bit layout)
