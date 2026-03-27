# Autoresearch: s3dashmap throughput optimization

## Objective
Maximize throughput (ops/sec) of s3dashmap on a realistic concurrent cache benchmark:
- 30K cache capacity, 480K distinct keys (16× oversubscription)
- 80% reads / 20% writes, Zipfian distribution (s=1.07)
- 12 parallel OS threads, 500K ops/thread, 5KB values
- Benchmark: `rust-cache-benchmarks` at `../rust-cache-benchmarks`

The benchmark measures steady-state performance: cache is pre-populated, then
7 measurement iterations run with shuffled order, reporting median throughput.

## Metrics
- **Primary**: `eff_ops_sec` (higher is better) — effective ops/sec = throughput × hit_rate
- **Secondary**: `ops_sec`, `hit_pct` (↑), `p50_us` (↓), `p99_us` (↓), `tail_us` (↓), `cv_pct` (↓ = more robust)

## How to Run
`./autoresearch.sh` — rebuilds s3dashmap, runs benchmark, outputs METRIC lines.

## Files in Scope
- `src/shard.rs` — S3-FIFO eviction state and algorithms (evict_one, evict_from_small/main, ghost set). PRIMARY optimization target.
- `src/lib.rs` — Main S3DashMap struct, constructors, hash/shard routing, Map trait impl (get, insert, remove, entry). HOT PATHS.
- `src/mapref/entry.rs` — Entry API: VacantEntry::insert (eviction trigger + queue registration), OccupiedEntry.
- `src/util.rs` — CacheEntry struct (value + freq AtomicU8 + loc), SharedValue, bump_freq.
- `src/lock.rs` — Custom RwLock implementation (parking_lot_core based).
- `src/t.rs` — Internal Map trait.
- `src/iter.rs` — Iterator implementations.
- `src/mapref/one.rs` — Ref/RefMut guard types.
- `Cargo.toml` — Dependencies and features.

## Off Limits
- `../rust-cache-benchmarks/` — benchmark harness must not be modified
- `tests/concurrent.rs` — integration tests must pass
- Public API signatures — must remain compatible

## Constraints
- `cargo test` must pass (unit + integration tests)
- No new dependencies (optimize with what we have)
- Hit rate must not degrade significantly (>1% drop = suspect)
- API and behavior must remain compatible

## Architecture Notes

### Hot paths (80% reads, 20% writes)
1. **get()**: hash → determine_shard → read_lock → map.find() → bump_freq (AtomicU8 CAS) → return Ref
2. **insert()** via entry(): hash → determine_shard → write_lock → find_or_find_insert_slot → if vacant: check ghost_set → maybe evict_one loop → clone key → insert_in_slot → push to queue → update live count

### Key data structures per shard
- `map`: hashbrown::raw::RawTable<(K, CacheEntry<V>)>` — the hash table
- `small`: VecDeque<(u64, K)> — ~10% capacity FIFO
- `main`: VecDeque<(u64, K)> — ~90% capacity FIFO  
- `ghost`: VecDeque<K> — evicted key tracking
- `ghost_set`: HashSet<K, RandomState> — O(1) ghost membership

### Known inefficiencies to explore
- ghost_set uses std HashSet with RandomState (separate hasher from map's ahash)
- VecDeque stores (hash, Key) clones — string keys means heap allocation per queue entry
- eviction does map.find() for liveness check on each queue pop (cold memory access)
- bump_freq uses fetch_update CAS loop (could use simpler fetch_add + saturate)
- CacheEntry<V> layout may have padding (SharedValue<V> + AtomicU8 + u8)
- default_shard_amount = 4*ncpu = 48 shards on 12-core — maybe too many for 30K items (625/shard)

## What's Been Tried

### Wins (kept)
1. **bump_freq: CAS → load+store** (+7.8% throughput) — fetch_update CAS loop replaced with relaxed load+store. Safe because freq is a heuristic saturating at 3.
2. **Ghost set: store hashes, not keys** (+0.5%) — HashSet<K> → HashSet<u64>. Eliminates key cloning for ghost entries. 
3. **Pre-allocate map + queues** (p99: 1.50→1.46µs) — Pre-allocate hashbrown table to shard_cap, VecDeques to 2x capacity.
4. **Eliminate double find() in eviction** (+1.7% eff_ops) — Single find() per eviction candidate.
5. **Direct _insert bypasses Entry API** (p99: 1.46→1.38µs, tail: 1.33→1.25µs) — Occupied path: find_or_find_insert_slot + in-place swap. Avoids Entry enum dispatch.
6. **Ghost set: hashbrown+ahash instead of std+SipHash** (+1-2% eff_ops) — Faster hashing for u64 ghost keys.

### Dead ends (discarded)
- **Fewer shards (64→32)**: -10% regression. Lock contention dominates at 2×ncpu.
- **#[inline] on trait methods**: No effect — LTO already inlines monomorphized trait methods.

### Additional wins
7. **Identity hasher for ghost_set** (p99: 1.38→1.25µs) — u64 values are already well-distributed; zero-cost identity hash.
8. **128 shards (8×ncpu)** (eff_ops +3.5%, tail sub-µs) — Less contention than 64 shards.
9. **Fix flaky tests** (correctness) — Auto-reduce shard count for small caches (min 8 entries/shard).
10. **bump_freq on occupied inserts** (hit rate 84.9→85.2%) — Writes count as accesses for eviction decisions.
11. **MAX_FREQ 3→7** (eff_ops +2.4%) — More second chances in main queue.

### Additional dead ends
- **256 shards**: better latency but CV 10-12% (too variable)
- **Ghost cap 2×**: no hit rate change for this Zipfian workload
- **Small queue 5-7%**: unstable or no improvement
- **MAX_FREQ=15**: eviction too expensive (15 re-enqueues per cold entry)
- **Pre-clone key before lock**: wasted clone for 85% occupied writes
- **Freq reset to 0 (S3-FIFO paper)**: faster eviction but -0.3% hit rate

### Current state
- eff_ops_sec: ~25.2M (was 23.1M baseline → +9.0%)
- p99: ~1.21µs (was 1.50µs → -19.3%)
- tail: ~1.08µs (was 1.38µs → -21.7%)
- hit_rate: 85.2% (was 84.9% → +0.3%)
- CV: variable 2-12% (environmental, 3-run median helps)
