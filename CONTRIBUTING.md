# Contributing to neocache

Thanks for your interest in contributing! `neocache` is a concurrent hash map with [S3-FIFO](https://s3fifo.com/) cache eviction, forked from [DashMap](https://github.com/xacrimon/dashmap) 6.1.0. It is small on purpose — a focused caching primitive, not a general-purpose data-structure toolkit. The guidelines below exist to keep it that way.

## How to contribute

### Things we will merge

- Bugfixes, especially for concurrency, eviction correctness, or memory safety
- Performance improvements backed by benchmark evidence (see [Benchmarks](#benchmarks))
- Tests that expand coverage of concurrent access patterns, eviction edge cases, or the DashMap-compatible API surface
- Documentation updates that are concise and improve accuracy — both the rustdoc comments and the files under `docs/`
- Minor API additions that are clearly useful to most users of a concurrent cache

### Things we won't merge

- Changes to performance-critical paths (the hot paths in `src/lib.rs`, `src/shard.rs`, `src/t.rs`) without `cargo bench` numbers showing no regression
- Code that introduces measurable throughput or latency regressions without a strong correctness justification
- Features that expand scope beyond "concurrent cache with S3-FIFO eviction" — if it can live in a wrapper crate, it should
- Features that break the DashMap 6.1.0 API surface unnecessarily — compatibility is a core goal
- Upgrades of `hashbrown` past `0.14.x`. The `raw` module became private in 0.15+, removing `RawTable`, `Bucket`, `InsertSlot`, and `RawIter` — the exact APIs this crate uses for zero-copy per-shard storage. See the note in `Cargo.toml`. A PR doing this upgrade must rewrite the storage layer around the new `HashTable` API and come with full benchmarks.
- Code without tests
- Code that breaks existing tests, `cargo clippy -- -D warnings`, or `cargo fmt --check`
- Documentation changes that are verbose, speculative, or duplicate what the code already makes obvious

## Workflow

1. Fork the repository and create a branch from `main`.
2. Make your changes. Keep commits focused; prefer small PRs.
3. Add or update tests. Concurrency changes need tests in `tests/concurrent.rs`; eviction changes need tests in `tests/coverage.rs`.
4. Run the full local check (see [Local checks](#local-checks)). CI runs the same commands and will fail on warnings.
5. For performance-sensitive changes, include before/after numbers from `cargo bench --bench throughput` in the PR description.
6. Open a pull request against `main`.

## Local checks

CI runs against Rust `1.88`. All four of these must pass:

```sh
cargo test --all-features
cargo clippy --all-features -- -D warnings
cargo fmt --check
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
```

## Benchmarks

The throughput benchmark lives at `benches/throughput.rs` and uses [criterion](https://github.com/bheisler/criterion.rs):

```sh
cargo bench --bench throughput
```

Run it on both `main` and your branch on the same machine, with no other load, and paste the summary into the PR. If your change touches a read-path, write-path, or eviction-path function, benchmarks are required.

## Unsafe code

`neocache` uses `unsafe` to access `hashbrown`'s raw table API. If you add or modify `unsafe` blocks:

- Document the invariants the caller must uphold in a comment above the block
- Make sure the invariant is actually upheld at every call site
- Read `docs/internals.md` first — it describes the lock discipline and aliasing rules that the existing unsafe code relies on

## Documentation

User-facing documentation lives in two places:

- Rustdoc comments on public items in `src/`
- Longer-form docs under `docs/` (algorithm, architecture, API reference, internals, usage guide)

If you change public behavior, update both. `docs/index.md` is the entry point.

## Releasing

Maintainers only:

1. Bump the version in `Cargo.toml`
2. Update `README.md` if the public API changed
3. Open a release PR and merge it to `main`
4. Tag the release (`git tag v0.x.y && git push --tags`)
5. Create a GitHub release pointing at the tag

## License

By contributing, you agree that your contributions will be licensed under the [MIT license](LICENSE).
