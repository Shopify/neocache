# Ideas Backlog

- **Pack freq (2 bits) + loc (1 bit) into a single AtomicU8**: Reduce field count in CacheEntry, potentially better codegen. 3 bits needed total.
- **Intrusive linked list for eviction queues**: Instead of VecDeque + key clones, embed next/prev pointers in CacheEntry. Eliminates all queue-related allocations.
- **Batch eviction**: Instead of one evict_one() per insert, batch-evict N entries periodically. Reduces amortized per-insert overhead.
- **Try-lock fast path for reads**: If read lock is contended, try a different shard (secondary hash). Reduces p99 from contention.
- **Specialized get for &str keys**: Avoid Borrow<Q> indirection for the common String/&str case.
- **Reduce write lock hold time**: Pre-clone key before acquiring write lock in _insert slow path.
- **Compact eviction index**: Use u32 indices into the hashbrown table instead of storing keys in queue. Requires stable bucket positions.
