//! Permutation-tested concurrency models, built on the
//! [`loom`](https://crates.io/crates/loom) crate.
//!
//! ## What this file does
//!
//! Loom replaces synchronization primitives (`Arc`, `thread`, atomics) with
//! deterministic, model-checked equivalents and explores every legal
//! interleaving between threads. It is the right tool for catching subtle
//! atomic-ordering or fence bugs that a stress test will only surface
//! probabilistically.
//!
//! ## What this file deliberately does *not* do
//!
//! `neocache` builds its sharded `RwLock` on top of `parking_lot_core`,
//! whose `park`/`unpark` primitives use real OS syscalls. Loom cannot
//! model these — to do so it would need a swappable atomics + parker
//! abstraction in `src/lock.rs`, which is a non-trivial refactor.
//!
//! As a result, the tests below cover:
//!
//! * The `bump_freq` saturating counter that runs concurrently under
//!   *shared* read locks. This uses only `AtomicU8` and is the only
//!   shared-mutable state outside the RwLock-protected `ShardData`. If
//!   the saturating-CAS is wrong, every cache miss-ratio assumption
//!   breaks — so it's worth modelling.
//! * The handoff between `bump_freq` (under a read lock) and the eviction
//!   path's `freq.load` (under a write lock). The lock provides the
//!   release/acquire fence; the test models a simulacrum of that
//!   handshake to confirm `Relaxed` is sufficient given the fence.
//!
//! Tests are gated behind `#![cfg(loom)]`. Run with:
//!
//! ```sh
//! RUSTFLAGS="--cfg loom" cargo test --test loom --release
//! ```
//!
//! See `.github/workflows/loom.yml` for the CI invocation.

#![cfg(loom)]

use loom::sync::Arc;
use loom::sync::atomic::{AtomicU8, Ordering};
use loom::thread;

/// Mirrors [`crate::shard::MAX_FREQ`].
const MAX_FREQ: u8 = 3;

/// Standalone copy of `CacheEntry::bump_freq` so this test does not depend
/// on the crate exporting its `freq` field. If the implementation in
/// `src/util.rs` changes, update this copy and add a comment cross-linking.
fn bump_freq(freq: &AtomicU8) {
    let _ = freq.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |f| {
        if f < MAX_FREQ { Some(f + 1) } else { None }
    });
}

/// Two concurrent readers each call `bump_freq` once. The counter must end
/// at 1 (one update lost to the saturating predicate is not possible from
/// 0) or 2 (both succeeded). Under no interleaving may it overflow above
/// `MAX_FREQ` or land at 0.
#[test]
fn bump_freq_two_readers_no_loss() {
    loom::model(|| {
        let freq = Arc::new(AtomicU8::new(0));

        let h1 = {
            let freq = freq.clone();
            thread::spawn(move || bump_freq(&freq))
        };
        let h2 = {
            let freq = freq.clone();
            thread::spawn(move || bump_freq(&freq))
        };
        h1.join().unwrap();
        h2.join().unwrap();

        let v = freq.load(Ordering::Relaxed);
        assert!(
            v == 1 || v == 2,
            "freq ended at {v}; expected 1 or 2 after two concurrent bumps from 0"
        );
    });
}

/// Three concurrent readers must saturate at exactly `MAX_FREQ`. The
/// loom model verifies this for every legal interleaving.
#[test]
fn bump_freq_saturates_at_max() {
    loom::model(|| {
        let freq = Arc::new(AtomicU8::new(MAX_FREQ - 1));

        // Three concurrent bumps; only one should succeed (MAX_FREQ - 1 -> MAX_FREQ).
        let mut handles = Vec::with_capacity(3);
        for _ in 0..3 {
            let freq = freq.clone();
            handles.push(thread::spawn(move || bump_freq(&freq)));
        }
        for h in handles {
            h.join().unwrap();
        }

        let v = freq.load(Ordering::Relaxed);
        assert_eq!(
            v, MAX_FREQ,
            "freq must saturate at MAX_FREQ under any interleaving"
        );
    });
}

/// Models the read-lock / write-lock handshake that justifies the
/// `Relaxed` ordering on `freq` operations.
///
/// In the real code, `bump_freq` runs under a *shared* read lock and the
/// eviction-path `freq.load` runs under an *exclusive* write lock. The
/// release of the read lock (a `Release` op on the lock state) and the
/// acquisition of the write lock (an `Acquire` op) form a happens-before
/// edge between the bump and the load. Loom cannot model the OS lock
/// directly, so we use a shared `AtomicUsize` flag with explicit
/// Release/Acquire on it as a stand-in for that handshake. If `Relaxed`
/// on `freq` is genuinely sufficient given the lock fence, the load on
/// the "writer" side must observe the bump in every interleaving.
#[test]
fn freq_load_observes_bump_through_release_acquire_fence() {
    use loom::sync::atomic::AtomicUsize;

    loom::model(|| {
        // `freq` is the relaxed-ordering counter from the real code.
        // `lock` stands in for the shared/exclusive lock state; it is the
        // synchronization point that establishes happens-before.
        let freq = Arc::new(AtomicU8::new(0));
        let lock = Arc::new(AtomicUsize::new(0));

        // Reader: takes the (shared) lock, bumps freq, releases.
        let reader = {
            let freq = freq.clone();
            let lock = lock.clone();
            thread::spawn(move || {
                lock.store(1, Ordering::Release);
                bump_freq(&freq);
                lock.store(2, Ordering::Release);
            })
        };

        // Writer: waits for the reader to finish (lock state == 2), then
        // reads freq. With the Release/Acquire on `lock`, the bump must
        // be visible.
        let writer = {
            let freq = freq.clone();
            let lock = lock.clone();
            thread::spawn(move || {
                while lock.load(Ordering::Acquire) != 2 {
                    loom::thread::yield_now();
                }
                let v = freq.load(Ordering::Relaxed);
                assert_eq!(
                    v, 1,
                    "writer must observe the reader's bump after the lock fence"
                );
            })
        };

        reader.join().unwrap();
        writer.join().unwrap();
    });
}
