# Ideas Backlog

## Untried / promising
- **find() fast path for occupied inserts**: Use cheaper find() for the 85% occupied case, fall back to find_or_find_insert_slot only for vacant 15%. Avoids slot-tracking overhead on the hot path.
- **Batch ghost set trimming**: Currently trims one-at-a-time in a while loop. Batch trim could reduce overhead.
- **Pack freq (3 bits) + loc (1 bit) into a single AtomicU8**: Simplify CacheEntry, fewer field accesses. With MAX_FREQ=7, freq uses exactly 3 bits.
- **Optimize eviction for warm steady-state**: In steady state, the small queue should have very few stale entries. Could skip the stale-check loop for the common case.

## Tried and failed — do NOT retry
- Fewer shards (32): contention kills it
- More shards (256): too variable
- #[inline] hints: LTO handles it
- Hash-only eviction queues: CacheEntry size increase hurts reads
- Pre-clone key before lock: wasted for 85% occupied writes
- Ghost cap 2×: no effect on Zipfian
- Small queue 5-7%: unstable
- MAX_FREQ 5/15: sweep complete, 7 is optimal
- Freq reset to 0: loses 0.3% hit rate
- Shard routing low-bits mask: worse distribution
