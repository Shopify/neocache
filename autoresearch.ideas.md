# Ideas Backlog

## Untried / promising
- **Lazy ghost trimming**: Instead of trimming ghost on every eviction, let it grow and trim in batches (e.g., trim 10% when at 120% capacity). Reduces per-eviction overhead.
- **Downgrade write→read lock after occupied insert**: OccupiedEntry::insert holds a write lock just to swap a value. If we could downgrade to read after the swap, readers on the same shard would unblock sooner.
- **Skip bump_freq for main-queue entries**: Entries already in main have proven hot; bumping freq further just delays their eventual eviction from main. Only bump for small-queue entries (new arrivals). Could reduce atomic ops on read path.
- **Reduce ShardData struct size**: Large struct = more cache lines touched per access. Could split into hot (map) and cold (queues, ghost) fields with an indirection.
- **Epoch-based frequency decay**: Periodically halve all freq counters (e.g., every N inserts). Prevents old entries from accumulating permanently high freq and blocking eviction of currently-hot entries.

## Tried and failed — do NOT retry
- Fewer shards (32): contention kills it
- More shards (256): too variable
- #[inline] hints: LTO handles it
- Pre-clone key before lock: wasted for 85% occupied writes
- Ghost cap 2×: no effect on Zipfian
- Small queue 5-7%: unstable
- MAX_FREQ 5/15: sweep complete, 7 is optimal
- Freq reset to 0: loses 0.3% hit rate
- Shard routing low-bits mask: worse distribution
- u32 fingerprint: no better than u16
- unreachable_unchecked: no gain, risky UB
- Direct insert() vs find_or_find_insert_slot: identical perf
