# neocache Documentation

`neocache` is a concurrent hash map with [S3-FIFO](https://s3fifo.com/) cache eviction, forked from [DashMap](https://github.com/xacrimon/dashmap) 6.1.0. It is designed as a drop-in replacement for DashMap in use cases where memory is bounded and eviction is needed.

## Documents

| Document | Description |
|----------|-------------|
| [Algorithm](algorithm.md) | How S3-FIFO works and why it was chosen |
| [Architecture](architecture.md) | Codebase layout, module responsibilities, key types |
| [API Reference](api-reference.md) | Every public type and method |
| [Internals](internals.md) | Lock design, raw hashbrown usage, unsafe invariants |
| [Usage Guide](usage-guide.md) | Patterns, recipes, and common pitfalls |

## Quick orientation

```
NeoCache<K, V, S>           ← public map type (sharded)
  └── shards: [RwLock<ShardData<K, V>>]
        └── ShardData<K, V>  ← one shard: hashbrown table + S3-FIFO queues
              ├── map:          RawTable<(K, CacheEntry<V>)>
              ├── small_hashes: VecDeque<u64>  ┐ ~10% of shard_cap
              ├── small_keys:   VecDeque<K>    ┘
              ├── main_hashes:  VecDeque<u64>  ┐ ~90% of shard_cap
              ├── main_keys:    VecDeque<K>    ┘
              └── ghost_set:    HashSet<u64>   ← bounded by shard_cap;
                                                 cleared in bulk when full
```

Every operation hashes the key once, selects a shard, and takes only that shard's lock. Eviction runs inside the same write-lock acquisition as insertion — no separate eviction thread or global lock is ever needed.
