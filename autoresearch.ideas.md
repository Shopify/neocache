# Ideas Backlog

## Untried / promising
- **Full RwLock redesign**: Current ONE_WRITER=!(0b11) enables atomic downgrade but prevents fetch_add (overflow). A completely new state encoding with separate WRITER_BIT could enable fetch_add reads (12% faster atomic ops, ~5-7% overall) but requires rewriting downgrade, all lock/unlock paths, and parked thread handling. Major effort, high risk.
- **Lock-free read path**: Use epoch-based reclamation or hazard pointers to eliminate read lock entirely. Maximum theoretical gain (~30%) but requires fundamental architecture change and extensive unsafe code.

## Tried and failed — do NOT retry
- Fewer shards (32), more shards (256)
- #[inline] hints, unreachable_unchecked
- Pre-clone key before lock
- Ghost cap 2×, small queue 5-7%
- MAX_FREQ 5/15 (7 is optimal)
- Freq reset to 0, freq=MAX_FREQ on promotion
- Shard routing low-bits mask
- u32 fingerprint
- Direct insert() vs find_or_find_insert_slot
- Early drop of write lock guard
- 2× hash table pre-allocation
- Downgrade write→read after occupied insert (window too small)
- Epoch-based frequency decay (Zipfian is stable)
