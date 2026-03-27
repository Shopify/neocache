# Ideas Backlog

## Untried / promising
- **find() fast path for occupied inserts**: Use cheaper find() for the 85% occupied case, fall back to find_or_find_insert_slot only for vacant 15%. Avoids slot-tracking overhead on the hot path.
- **Batch ghost set trimming**: Currently trims one-at-a-time in a while loop. Batch trim could reduce overhead.
- **Optimize ghost set trimming**: Replace while-loop + pop_front with a batch trim (truncate + drain).
- **Reduce VecDeque overhead**: Now that queues store u64 only, could use a fixed-size ring buffer instead of VecDeque for even less overhead.
- **Try u32 hash fingerprint instead of u16**: Uses 2 more padding bytes but further reduces collision risk.

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
